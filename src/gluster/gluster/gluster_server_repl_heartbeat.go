// 数据同步需要注意的是：
// leader只有在通知完所有follower更新完数据之后，自身才会进行数据更新
// 因此leader
package gluster

import (
    "g/encoding/gjson"
    "time"
    "g/os/gfile"
    "os"
    "bufio"
    "encoding/json"
)

// leader到其他节点的数据同步监听
func (n *Node) replicationHandler() {
    // 数据同步检测
    go n.dataReplicationLoop()

    // Service同步检测
    go n.serviceReplicationLoop()

    // Peers同步检测
    go n.peersReplicationLoop()
}

// 日志自动同步检查
func (n *Node) dataReplicationLoop() {
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.Peers.Values() {
                info := v.(NodeInfo)
                if info.Status != gSTATUS_ALIVE {
                    continue
                }
                go func(ip string) {
                    conn := n.getConn(ip, gPORT_REPL)
                    if conn != nil {
                        defer conn.Close()
                        if n.sendMsg(conn, gMSG_REPL_DATA_UPDATE_CHECK, "") != nil {
                            msg := n.receiveMsg(conn)
                            if msg != nil && msg.Head == gMSG_REPL_RESPONSE {
                                n.updateDataToRemoteNode(conn, &msg.Info)
                            }
                        }
                    }
                }(info.Ip)
            }
        }
        time.Sleep(gLOG_REPL_DATA_UPDATE_INTERVAL * time.Millisecond)
    }
}

// Service自动同步检测
func (n *Node) serviceReplicationLoop() {
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.Peers.Values() {
                info := v.(NodeInfo)
                if info.Status != gSTATUS_ALIVE || n.getLastServiceLogId() <= info.LastServiceLogId {
                    continue
                }
                go func(ip string) {
                    conn := n.getConn(ip, gPORT_REPL)
                    if conn != nil {
                        defer conn.Close()
                        n.updateServiceToRemoteNode(conn)
                    }
                }(info.Ip)
            }
        }
        time.Sleep(gLOG_REPL_SERVICE_UPDATE_INTERVAL * time.Millisecond)
    }
}

// 节点Peers信息自动同步
func (n *Node) peersReplicationLoop() {
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.Peers.Values() {
                info := v.(NodeInfo)
                if info.Status != gSTATUS_ALIVE {
                    continue
                }
                go func(info *NodeInfo) {
                    conn := n.getConn(info.Ip, gPORT_REPL)
                    if conn != nil {
                        defer conn.Close()
                        n.sendMsg(conn, gMSG_REPL_PEERS_UPDATE, gjson.Encode(n.Peers.Values()))
                    }
                }(&info)
            }
        }
        time.Sleep(gLOG_REPL_PEERS_INTERVAL * time.Millisecond)
    }
}

// 获取节点中已同步的最小的log id
func (n *Node) getMinLogIdFromPeers() int64 {
    var minLogId int64 = n.getLastLogId()
    for _, v := range n.Peers.Values() {
        info := v.(NodeInfo)
        if info.Status != gSTATUS_ALIVE {
            continue
        }
        if minLogId == 0 || info.LastLogId < minLogId {
            minLogId = info.LastLogId
        }
    }
    return minLogId
}

// 根据logid获取还未更新的日志列表
// 注意：为保证日志一致性，在进行日志更新时，需要查找到目标节点logid在本地日志中存在有**完整匹配**的logid日志，并将其后的日志列表返回
// 如果出现leader的logid比follower大，并且获取不到更新的日志列表时，表示两者数据已经不一致，需要做完整的同步复制处理
// 升序查找
func (n *Node) getLogEntriesByLastLogId(id int64, max int) []LogEntry {
    if n.getLastLogId() > id {
        array := make([]LogEntry, 0)
        // 首先从内存中获取
        if n.LogList.Len() > 0 {
            //n.LogList.RLock()
            match := false
            if id == 0 {
                match = true
            }
            l := n.LogList.Back()
            for l != nil {
                // 最大获取条数控制
                if len(array) == max {
                    break;
                }
                r := l.Value.(*LogEntry)
                if r.Id > id {
                    match = true
                } else if r.Id > id {
                    if match {
                        array = append(array, *r)
                    } else {
                        break;
                    }
                }
                l = l.Prev()
            }
            //n.LogList.RUnlock()
        }
        // 如果当前内存中的数据不够，那么从文件中读取剩余数据
        length := len(array)
        if length < 1 || (length < max && array[length - 1].Id < id) {
            left   := max - length
            result := n.getLogEntryListFromFileByLogId(id, left)
            if result != nil && len(result) > 0 {
                array = append(array, result...)
            }
        }
        return array
    }
    return nil
}

// 从文件中获取指定logid之后max数量的数据
func (n *Node) getLogEntryListFromFileByLogId(logid int64, max int) []LogEntry {
    id    := logid
    match := false
    if logid == 0 {
        match = true
    }
    array := make([]LogEntry, 0)
    for {
        path      := n.getLogEntryFileSavePathById(id)
        file, err := gfile.OpenWithFlag(path, os.O_RDONLY)
        if err == nil {
            defer file.Close()
            buffer := bufio.NewReader(file)
            for {
                if len(array) == max {
                    return array
                }
                line, _, err := buffer.ReadLine()
                if err == nil {
                    var entry LogEntry
                    if err := json.Unmarshal(line, &entry); err == nil {
                        if !match && entry.Id == logid {
                            match = true
                        } else if entry.Id > logid {
                            if match {
                                array = append(array, entry)
                            } else {
                                break;
                            }
                        }
                    } else {
                        return array
                    }
                } else {
                    return array
                }
            }
        } else {
            break;
        }
        // 下一批次
        id += gLOGENTRY_FILE_SIZE
    }
    return array
}

// 由于在数据量比较大的情况下，会引起多次同步，因此必需判断给定的logid是否是一个合法的logid，以便后续进程能够保证同步是有效合理的
// 升序查找
func (n *Node) isValidLogId(id int64) bool {
    lastLogId := n.getLastLogId()
    if lastLogId >= id {
        if lastLogId == id {
            return true
        }
        //n.LogList.RLock()
        l := n.LogList.Back()
        for l != nil {
            r := l.Value.(*LogEntry)
            if r.Id == id {
                return true
            } else if r.Id < id {
                l = l.Prev()
            } else {
                return false
            }
        }
        //n.LogList.RUnlock()
    } else {
        return false
    }
    // 如果在现有的LogList查找不到，那么进入文件查找
    path      := n.getLogEntryFileSavePathById(id)
    file, err := gfile.OpenWithFlag(path, os.O_RDONLY)
    if err == nil {
        defer file.Close()
        buffer := bufio.NewReader(file)
        for {
            line, _, err := buffer.ReadLine()
            if err == nil {
                var entry LogEntry
                if json.Unmarshal(line, &entry) == nil {
                    if entry.Id == id {
                        return true
                    } else if entry.Id > id {
                        return false
                    }
                }
            } else {
                break;
            }
        }
    }
    return false
}