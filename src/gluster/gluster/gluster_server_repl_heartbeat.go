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
    "g/os/glog"
    "fmt"
    "g/os/gcache"
    "io"
    "g/core/types/gset"
)

// leader到其他节点的数据同步监听
func (n *Node) replicationHandler() {
    // 数据同步检测
    go n.dataReplicationLoop()

    // Service同步检测
    go n.serviceReplicationLoop()

    // Peers同步检测
    go n.peersReplicationLoop()

    // LogList定期清理
    go n.autoCleanLogList()
}

// 日志自动同步检查，每一个节点保持一个线程检查，保证同步能够快速进行
func (n *Node) dataReplicationLoop() {
    // 存储已经保持心跳的节点
    conns := gset.NewStringSet()
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.Peers.Values() {
                info := v.(NodeInfo)
                if conns.Contains(info.Id) || info.Status != gSTATUS_ALIVE || info.Id == n.getId() {
                    continue
                }
                go func(id, ip string) {
                    conns.Add(id)
                    defer conns.Remove(id)
                    for {
                        // 如果当前节点不再是leader，或者节点表中已经删除该节点信息
                        if n.getRaftRole() != gROLE_RAFT_LEADER || !n.Peers.Contains(id) {
                            return
                        }
                        info := n.Peers.Get(id).(NodeInfo)
                        if n.getLastLogId() > info.LastLogId {
                            conn := n.getConn(ip, gPORT_REPL)
                            if conn != nil {
                                n.updateDataToRemoteNode(conn, &info)
                                conn.Close()
                            }
                        }
                        time.Sleep(gLOG_REPL_DATA_UPDATE_INTERVAL * time.Millisecond)
                    }
                }(info.Id, info.Ip)
            }
        }
        time.Sleep(1000 * time.Millisecond)
    }
}

// Service自动同步检测
func (n *Node) serviceReplicationLoop() {
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.Peers.Values() {
                info := v.(NodeInfo)
                //glog.Printf("%v: %v <= %v\n", info.Ip, n.getLastServiceLogId(), info.LastServiceLogId)
                if info.Status != gSTATUS_ALIVE || n.getLastServiceLogId() <= info.LastServiceLogId {
                    continue
                }
                go func(info *NodeInfo) {
                    key  := fmt.Sprintf("gluster_service_replication_%s", info.Id)
                    if gcache.Get(key) != nil {
                        return
                    }
                    gcache.Set(key, struct {}{}, 10000)
                    defer gcache.Remove(key)

                    conn := n.getConn(info.Ip, gPORT_REPL)
                    if conn != nil {
                        defer conn.Close()
                        glog.Println("send service replication from", n.getName(), "to", info.Name)
                        n.updateServiceToRemoteNode(conn)
                    }
                }(&info)
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

// 根据logid获取还未更新的日志列表
// 注意：为保证日志一致性，在进行日志更新时，需要查找到目标节点logid在本地日志中存在有**完整匹配**的logid日志，并将其后的日志列表返回
// 如果出现leader的logid比follower大，并且获取不到更新的日志列表时，表示两者数据已经不一致，需要做完整的同步复制处理
// 升序查找
func (n *Node) getLogEntriesByLastLogId(id int64, max int) []LogEntry {
    array := make([]LogEntry, 0)
    if n.getLastLogId() > id {
        // 首先从内存中获取，需要注意的是，
        // 如果内存列表中最小的logid比请求的大，数据会有缺失，必须从磁盘中读取（一般不会出现，因为自动清理loglist是会判断所有节点同步完成后才会执行）
        // 因此，内容列表中的logid必须包含请求的logid
        if n.LogList.Len() > 0 {
            match := false
            if id == 0 {
                match = true
            }
            l := n.LogList.Back()
            if l != nil && l.Value.(*LogEntry).Id <= id {
                for l != nil {
                    if len(array) == max {
                        break;
                    }
                    r := l.Value.(*LogEntry)
                    if r.Id <= id {
                        match = true
                    } else {
                        if match {
                            array = append(array, *r)
                        } else {
                            break;
                        }
                    }
                    l = l.Prev()
                }
            }
        }
        // 如果当前内存中的数据不够，那么从文件中读取剩余数据
        length := len(array)
        if length < 1 || (length < max && array[length - 1].Id < id) {
            left   := max - length
            leftid := id
            if length > 0 {
                leftid = array[length - 1].Id
            }
            result := n.getLogEntryListFromFileByLogId(leftid, left)
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
    // id仅用于计算文件路径
    id    := logid
    match := false
    if logid == 0 {
        match = true
    }
    array := make([]LogEntry, 0)
    for {
        // 确定数据文件
        path      := n.getLogEntryFileSavePathById(id)
        file, err := gfile.OpenWithFlag(path, os.O_RDONLY)
        if err == nil {
            defer file.Close()
            // 读取数据文件符合条件的数据
            buffer := bufio.NewReader(file)
            for {
                if len(array) == max {
                    return array
                }
                line, _, err := buffer.ReadLine()

                if err == nil {
                    // 可能是一个空换行
                    if len(line) < 10 {
                        continue
                    }
                    var entry LogEntry
                    err := json.Unmarshal(line, &entry)
                    if err == nil {
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
                        glog.Error(err)
                        return array
                    }
                } else {
                    if err == io.EOF {
                        break;
                    } else {
                        glog.Error(err)
                        return array
                    }
                }
            }
        } else {
            break;
        }
        // 如果并没有查询到指定的logid，那么就不再继续，表明给定的logid非法
        if !match {
            break
        }
        // 下一批次，注意后四位是随机数，所以这里要乘以10000
        id += gLOGENTRY_FILE_SIZE*10000
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
        l := n.LogList.Back()
        if l != nil && l.Value.(*LogEntry).Id < id {
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
        }
    } else {
        return false
    }
    return n.checkValidLogIdFromFile(id)
}

// 从物理化文件中查找logid的有效性
func (n *Node) checkValidLogIdFromFile(id int64) bool {
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

// 定期清理已经同步完毕的日志列表，注意：***leader和follower都需要清理***
// 获取所有已存活的节点的最小日志ID，清理本地日志列表中比该ID小的记录(需要在内存中保留最小记录，以便对最新数据做合法性判断)
func (n *Node) autoCleanLogList() {
    for {
        time.Sleep(gLOG_REPL_LOGCLEAN_INTERVAL * time.Millisecond)
        minLogId := n.getMinLogIdFromPeers()
        if minLogId == 0 {
            continue
        }
        // 必须保证日志先保存完毕再清理
        if minLogId > n.getLastSavedLogId() {
            minLogId = n.getLastSavedLogId()
        }
        p := n.LogList.Back()
        for p != nil {
            entry := p.Value.(*LogEntry)
            if entry.Id < minLogId {
                //glog.Printfln("auto clean log id: %d", entry.Id)
                t := p.Prev()
                n.LogList.Remove(p)
                p  = t
            } else {
                break;
            }
        }
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