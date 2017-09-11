// 数据同步需要注意的是：
// leader只有在通知完所有follower更新完数据之后，自身才会进行数据更新
// 因此leader
package gluster

import (
    "g/encoding/gjson"
    "time"
    "g/core/types/gset"
    "g/os/gfile"
    "os"
    "strconv"
)

// leader到其他节点的数据同步监听
func (n *Node) replicationHandler() {
    // 初始化数据同步心跳检测
    go n.dataReplicationLoop()

    // Peers自动同步
    go n.peersReplicationLoop()
}

// 日志自动同步检查，类似心跳
func (n *Node) dataReplicationLoop() {
    conns := gset.NewStringSet()
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.Peers.Values() {
                info := v.(NodeInfo)
                if conns.Contains(info.Id) {
                    continue
                }
                go func(info *NodeInfo) {
                    conns.Add(info.Id)
                    defer conns.Remove(info.Id)
                    conn := n.getConn(info.Ip, gPORT_REPL)
                    if conn == nil {
                        return
                    }
                    defer conn.Close()
                    for {
                        if n.getRaftRole() != gROLE_RAFT_LEADER || !n.Peers.Contains(info.Id){
                            return
                        }
                        //glog.Println("sending replication heartbeat to", ip)
                        if n.sendMsg(conn, gMSG_REPL_HEARTBEAT, "") != nil {
                            return
                        }
                        msg := n.receiveMsg(conn)
                        if msg != nil {
                            // 如果当前节点正处于数据同步中，那么本次心跳不再执行任何的数据同步判断
                            if !n.getStatusInReplication() {
                                switch msg.Head {
                                    case gMSG_REPL_INCREMENTAL_UPDATE:              n.updateDataFromRemoteNode(conn, msg)
                                    case gMSG_REPL_COMPLETELY_UPDATE:               n.updateDataFromRemoteNode(conn, msg)
                                    case gMSG_REPL_NEED_UPDATE_FOLLOWER:            n.updateDataToRemoteNode(conn, msg)
                                    case gMSG_REPL_SERVICE_COMPLETELY_UPDATE:       n.updateServiceFromRemoteNode(conn, msg)
                                    case gMSG_REPL_SERVICE_NEED_UPDATE_FOLLOWER:    n.updateServiceToRemoteNode(conn, msg)
                                }
                            }
                            time.Sleep(gLOG_REPL_TIMEOUT_HEARTBEAT * time.Millisecond)
                        }
                    }
                }(&info)
            }
        }
        time.Sleep(100 * time.Millisecond)
    }
}

// 节点Peers信息自动同步
func (n *Node) peersReplicationLoop() {
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.Peers.Values() {
                info := v.(NodeInfo)
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
        //n.LogList.RLock()
        l := n.LogList.Back()
        for l != nil {
            // 最大获取条数控制
            if len(array) == max {
                break;
            }
            r := l.Value.(LogEntry)
            if r.Id > id {
                array = append(array, r)
                l = l.Prev()
            } else {
                break;
            }
        }
        //n.LogList.RUnlock()
        // 如果当前内存中的数据不够，那么从文件中读取剩余数据
        if len(array) < max && array[len(array) - 1].Id < id {
            path      := n.getLogEntryFileSavePathById(id)
            file, err := gfile.OpenWithFlag(path, os.O_RDONLY)
            if err == nil {
                defer file.Close()
                start := int64(0)
                for {
                    offset := gfile.GetNextCharOffset(file, ",", start)
                    if offset < 1 {
                        break;
                    }
                    r  := gfile.GetBinContentByTwoOffsets(file, start, offset - 1)
                    if r != nil {
                        logid, _ := strconv.ParseInt(string(r), 10, 64)
                        if logid > id {
                            offset2  := gfile.GetNextCharOffset(file, ",", offset + 1)
                            r2       := gfile.GetBinContentByTwoOffsets(file, offset + 1, offset2 - 1)
                            actid, _ := strconv.Atoi(string(r2))
                            offset3  := gfile.GetNextCharOffset(file, "\n", offset2 + 1)
                            r3       := gfile.GetBinContentByTwoOffsets(file, offset2 + 1, offset3 - 1)
                            items    := string(r3)
                            array     = append(array, LogEntry{
                                Id    : logid,
                                Act   : actid,
                                Items : gjson.Decode(items),
                            })
                            start = offset3 + 1
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        return array
    }
    return nil
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
            r := l.Value.(LogEntry)
            if r.Id == id {
                return true
            } else if r.Id > id {
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
        start := int64(0)
        for {
            offset := gfile.GetNextCharOffset(file, ",", start)
            r      := gfile.GetBinContentByTwoOffsets(file, start, offset)
            if r != nil {
                tempid, err := strconv.ParseInt(string(r), 10, 64)
                if err != nil {
                    return false
                }
                if tempid == id {
                    return true
                } else if tempid > id {
                    return false
                }
                start = gfile.GetNextCharOffset(file, "\n", offset)
                if start == 0 {
                    return false
                } else {
                    start = start + 1
                }
            }
        }
    }
    return false
}