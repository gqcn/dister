// 数据同步需要注意的是：
// leader只有在通知完所有follower更新完数据之后，自身才会进行数据更新
// 因此leader
package dister

import (
    "time"
    "sync"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/gcache"
    "gitee.com/johng/gf/g/encoding/gjson"
    "gitee.com/johng/gf/g/encoding/gcompress"
)

// 日志自动保存处理
func (n *Node) autoSavingHandler() {
    // 只有server节点才进行数据物理化存储
    if n.getRole() != gROLE_SERVER {
        return
    }
    lastLogId     := n.getLastLogId()
    lastServiceId := n.getLastServiceLogId()
    for {
        if n.getLastLogId() != lastLogId {
            n.saveDataToFile()
            lastLogId = n.getLastLogId()
        }
        if n.getLastServiceLogId() != lastServiceId {
            n.saveServiceToFile()
            lastServiceId = n.getLastServiceLogId()
        }
        time.Sleep(gLOG_REPL_AUTOSAVE_INTERVAL * time.Millisecond)
    }
}

// 保存数据到磁盘
func (n *Node) saveDataToFile() {
    key := "auto_saving_data"
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct {}{}, 6000000)
    defer gcache.Remove(key)

    data := make(map[string]interface{})
    data  = map[string]interface{} {
        "LastLogId"   : n.getLastLogId(),
        "DataMap"     : *n.DataMap.Clone(),
    }
    content, err := gjson.Encode(data)
    if err != nil {
        return
    }
    if gCOMPRESS_SAVING {
        content = gcompress.Zlib(content)
    }
    if err := gfile.PutBinContents(n.getDataFilePath(), content); err != nil {
        glog.Error("saving data error:", err)
    }
}

// 保存Service到磁盘
func (n *Node) saveServiceToFile() {
    key := "auto_saving_service"
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct {}{}, 6000000)
    defer gcache.Remove(key)

    data := make(map[string]interface{})
    data  = map[string]interface{} {
        "LastServiceLogId"  : n.getLastServiceLogId(),
        "Service"           : *n.Service.Clone(),
    }
    content, err := gjson.Encode(data)
    if err != nil {
        return
    }
    if gCOMPRESS_SAVING {
        content = gcompress.Zlib(content)
    }
    if err := gfile.PutBinContents(n.getServiceFilePath(), content); err != nil {
        glog.Error("saving service error:", err)
    }
}

// 从物理化文件中恢复变量
func (n *Node) restoreFromFile() {
    // 只有server节点才进行数据物理化存储
    if n.getRole() != gROLE_SERVER {
        return
    }

    var wg sync.WaitGroup

    wg.Add(1)
    go func() {
        n.restoreDataMap()
        wg.Done()
    }()

    wg.Add(1)
    go func() {
        n.restoreService()
        wg.Done()
    }()
    wg.Wait()
}

// 恢复DataMap
func (n *Node) restoreDataMap() {
    path := n.getDataFilePath()
    if gfile.Exists(path) {
        bin := gfile.GetBinContents(path)
        if gCOMPRESS_SAVING {
            bin = gcompress.UnZlib(bin)
        }
        if bin != nil && len(bin) > 0 {
            //glog.Println("restore data from", path)
            j, err := gjson.DecodeToJson(bin)
            if err != nil {
                glog.Fatal(err)
            }
            m  := make(map[string]string)
            id := j.GetInt64("LastLogId")
            if err := j.GetToVar("DataMap", &m); err == nil {
                n.DataMap.BatchSet(m)
            } else {
                glog.Error(err)
            }
            // 判断日志与数据存储的一致性，并执行校验恢复
            list := n.getLogEntryListFromFileByLogId(id, 0, false)
            if len(list) > 0 {
                logid := id
                for _, v := range list {
                    logid = v.Id
                    n.saveLogEntryToVar(&v)
                }
                n.setLastLogId(logid)
            } else {
                n.setLastLogId(id)
            }
        }
    }
}

// 恢复Service
func (n *Node) restoreService() {
    path := n.getServiceFilePath()
    if gfile.Exists(path) {
        bin := gfile.GetBinContents(path)
        if gCOMPRESS_SAVING {
            bin = gcompress.UnZlib(bin)
        }
        if bin != nil && len(bin) > 0 {
            //glog.Println("restore service from", path)
            j, err := gjson.DecodeToJson(bin)
            if err != nil {
                glog.Fatal(err)
            }
            m := make(map[string]Service)
            n.setLastServiceLogId(j.GetInt64("LastServiceLogId"))
            if err := j.GetToVar("Service", &m); err == nil {
                for k, v := range m {
                    n.Service.Set(k, v)
                }
            } else {
                glog.Error(err)
            }
        }
    }
}


