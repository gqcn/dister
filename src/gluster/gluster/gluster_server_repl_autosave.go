// 数据同步需要注意的是：
// leader只有在通知完所有follower更新完数据之后，自身才会进行数据更新
// 因此leader
package gluster

import (
    "g/encoding/gjson"
    "time"
    "g/os/gfile"
    "g/os/glog"
    "g/encoding/gcompress"
    "sync"
    "encoding/json"
)

// 日志自动保存处理
func (n *Node) autoSavingHandler() {
    for {
        go n.saveLogList()
        go n.savePeersToFile()
        if n.getDataDirty() {
            go func() {
                n.saveDataToFile()
                n.setDataDirty(false)
            }()
        }
        if n.getServiceDirty() {
            go func() {
                n.saveServiceToFile()
                n.setServiceDirty(false)
            }()
        }
        time.Sleep(gLOG_REPL_AUTOSAVE_INTERVAL * time.Millisecond)
    }
}

// 定期物理化存储已经同步完毕的日志列表，注意：***leader和follower都需要清理***
// 获取所有已存活的节点的最小日志ID，保存本地日志列表中比该ID小的记录，如果非最小id，表明数据当前急需同步到其他节点
func (n *Node) saveLogList() {
    //glog.Println("log list size:", n.LogList.Len())
    //glog.Println("last log id:", n.getLastLogId())
    minLogId := n.getMinLogIdFromPeers()
    //glog.Println("min log id:", minLogId)
    if minLogId == 0 {
        return
    }
    //n.LogList.RLock()
    p := n.LogList.Back()
    for p != nil {
        entry := p.Value.(LogEntry)
        if entry.Id <= minLogId {
            t      := p.Prev()
            s, err := json.Marshal(entry)
            if err != nil {
                glog.Error("json marshal log entry error:", err)
                break;
            }
            s   = append(s, 10)
            err = gfile.PutBinContentsAppend(n.getLogEntryFileSavePathById(entry.Id), s)
            if err == nil {
                n.LogList.Remove(p)
            } else {
                glog.Error("save data entry error:", err)
            }
            p = t
        } else {
            break;
        }
    }
    //n.LogList.RUnlock()
}

// 保存Peers到磁盘
func (n *Node) savePeersToFile() {
    info         := n.getNodeInfo()
    data         := *n.Peers.Clone()
    data[info.Id] = info
    content := []byte(gjson.Encode(&data))
    if gCOMPRESS_SAVING {
        content = gcompress.Zlib(content)
    }
    err := gfile.PutBinContents(n.getPeersFilePath(), content)
    if err != nil {
        glog.Error("saving peers error:", err)
    }
}

// 保存数据到磁盘
func (n *Node) saveDataToFile() {
    data := make(map[string]interface{})
    data  = map[string]interface{} {
        "LastLogId" : n.getLastLogId(),
        "LogCount"  : n.getLogCount(),
        "DataMap"   : *n.DataMap.Clone(),
    }
    content := []byte(gjson.Encode(&data))
    if gCOMPRESS_SAVING {
        content = gcompress.Zlib(content)
    }
    err := gfile.PutBinContents(n.getDataFilePath(), content)
    if err != nil {
        glog.Error("saving data error:", err)
    }
}

// 保存Service到磁盘
func (n *Node) saveServiceToFile() {
    data := make(map[string]interface{})
    data  = map[string]interface{} {
        "LastServiceLogId"  : n.getLastServiceLogId(),
        "Service"           : *n.serviceMapToServiceStructMap(),
    }
    content := []byte(gjson.Encode(&data))
    if gCOMPRESS_SAVING {
        content = gcompress.Zlib(content)
    }
    err := gfile.PutBinContents(n.getServiceFilePath(), content)
    if err != nil {
        glog.Error("saving service error:", err)
    }
}

// 从物理化文件中恢复变量
func (n *Node) restoreFromFile() {
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        n.restorePeers()
        wg.Done()
    }()

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

// 恢复Peers
func (n *Node) restorePeers() {
    path := n.getPeersFilePath()
    if gfile.Exists(path) {
        bin := gfile.GetBinContents(path)
        if gCOMPRESS_SAVING {
            bin = gcompress.UnZlib(bin)
        }
        if bin != nil && len(bin) > 0 {
            glog.Println("restore peers from", path)
            m := make(map[string]NodeInfo)
            if err := gjson.DecodeTo(string(bin), &m); err == nil {
                myid := n.getId()
                for k, v := range m {
                    if k != myid && !n.Peers.Contains(k) {
                        n.Peers.Set(k, v)
                    }
                }
            } else {
                glog.Error(err)
            }
        }
    }
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
            glog.Println("restore data from", path)
            m := make(map[string]string)
            j := gjson.DecodeToJson(string(bin))
            n.setLastLogId(j.GetInt64("LastLogId"))
            n.setLogCount(j.GetInt("LogCount"))
            if err := j.GetToVar("DataMap", &m); err == nil {
                n.DataMap.BatchSet(m)
            } else {
                glog.Error(err)
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
            glog.Println("restore service from", path)
            m := make(map[string]ServiceStruct)
            j := gjson.DecodeToJson(string(bin))
            n.setLastServiceLogId(j.GetInt64("LastServiceLogId"))
            if err := j.GetToVar("Service", &m); err == nil {
                for k, v := range m {
                    n.Service.Set(k, *n.serviceSructToService(&v))
                }
            } else {
                glog.Error(err)
            }
        }
    }
}

// 使用logentry数组更新本地的日志列表
func (n *Node) updateFromLogEntriesJson(jsonContent string) error {
    array := make([]LogEntry, 0)
    err   := gjson.DecodeTo(jsonContent, &array)
    if err != nil {
        glog.Error(err)
        return err
    }
    if array != nil && len(array) > 0 {
        for _, v := range array {
            if v.Id > n.getLastLogId() {
                n.LogList.PushFront(v)
                n.saveLogEntry(v)
            }
        }
    }
    return nil
}



