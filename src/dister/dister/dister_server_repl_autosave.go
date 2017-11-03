// 数据同步需要注意的是：
// leader只有在通知完所有follower更新完数据之后，自身才会进行数据更新
// 因此leader
package dister

import (
    "time"
    "sync"
    "g/os/gcache"
    "g/encoding/gbinary"
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

    n.DataMap.Set([]byte("LastLogId"), gbinary.EncodeInt64(n.getLastLogId()))
}

// 保存Service到磁盘
func (n *Node) saveServiceToFile() {
    key := "auto_saving_service"
    if gcache.Get(key) != nil {
        return
    }
    gcache.Set(key, struct {}{}, 6000000)
    defer gcache.Remove(key)

    n.Service.Set([]byte("LastServiceLogId"), gbinary.EncodeInt64(n.getLastServiceLogId()))
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
    id := int64(0)
    if r := n.DataMap.Get([]byte("LastLogId")); r != nil {
        id = gbinary.DecodeToInt64(r)
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

// 恢复Service
func (n *Node) restoreService() {
    if r := n.DataMap.Get([]byte("LastServiceLogId")); r != nil {
        n.setLastServiceLogId(gbinary.DecodeToInt64(r))
    }

}


