// 数据同步需要注意的是，必须在节点完成election选举之后才能进行数据通信。
// 也就是说repl通信需要建立在raft成功作为前提
package dister

import (
    "net"
    "g/encoding/gjson"
    "g/util/gtime"
    "g/os/glog"
    "time"
    "fmt"
    "g/core/types/gset"
    "strconv"
    "g/os/gfile"
    "sync/atomic"
    "g/os/gcache"
)

// 集群数据同步接口回调函数
func (n *Node) replTcpHandler(conn net.Conn) {
    msg := n.receiveMsg(conn)
    // 判断集群基础信息
    if msg == nil || msg.Info.Group != n.Group  || msg.Info.Version != gVERSION {
        //glog.Debug("receive nil, auto close conn")
        conn.Close()
        return
    }
    // 判断集群权限，只有本集群的节点之间才能进行数据通信(判断id)
    leader := n.getLeader()
    if leader == nil || (leader.Id != msg.Info.Id && !n.Peers.Contains(msg.Info.Id)) {
        glog.Debugfln("invalid peer, id: %s, name: %s", msg.Info.Id, msg.Info.Name)
        conn.Close()
        return
    }
    switch msg.Head {
        case gMSG_REPL_DATA_SET:                    n.onMsgReplDataSet(conn, msg)
        case gMSG_REPL_DATA_REMOVE:                 n.onMsgReplDataRemove(conn, msg)
        case gMSG_REPL_DATA_APPENDENTRY:            n.onMsgReplDataAppendEntry(conn, msg)
        case gMSG_REPL_DATA_REPLICATION:            n.onMsgReplDataReplication(conn, msg)
        case gMSG_REPL_PEERS_UPDATE:                n.onMsgPeersUpdate(conn, msg)
        case gMSG_REPL_VALID_LOGID_CHECK:           n.onMsgReplValidLogIdCheck(conn, msg)
        case gMSG_REPL_CONFIG_FROM_FOLLOWER:        n.onMsgConfigFromFollower(conn, msg)
        case gMSG_API_DATA_GET:                     n.onMsgApiDataGet(conn, msg)
        case gMSG_API_PEERS_ADD:                    n.onMsgApiPeersAdd(conn, msg)
        case gMSG_API_PEERS_REMOVE:                 n.onMsgApiPeersRemove(conn, msg)
        case gMSG_API_SERVICE_GET:                  n.onMsgApiServiceGet(conn, msg)
        case gMSG_API_SERVICE_SET:                  n.onMsgApiServiceSet(conn, msg)
        case gMSG_API_SERVICE_REMOVE:               n.onMsgApiServiceRemove(conn, msg)
    }
    //这里不用自动关闭链接，由于链接有读取超时，当一段时间没有数据时会自动关闭
    n.replTcpHandler(conn)
}

// 用于API接口的Service查询
func (n *Node) onMsgApiServiceGet(conn net.Conn, msg *Msg) {
    n.sendMsg(conn, gMSG_REPL_RESPONSE, n.getServiceByApi(msg.Body))
}

// 用于API接口的数据查询
func (n *Node) onMsgApiDataGet(conn net.Conn, msg *Msg) {
    n.sendMsg(conn, gMSG_REPL_RESPONSE, n.getDataByApi(msg.Body))
}

// kv设置，这里增加了一把数据锁，以保证请求的先进先出队列执行，因此写效率会有所降低
func (n *Node) onMsgReplDataSet(conn net.Conn, msg *Msg) {
    result := gMSG_REPL_RESPONSE
    if n.getRaftRole() == gROLE_RAFT_LEADER {
        n.dmutex.Lock()
        items := gjson.Decode(msg.Body)
        // 由于锁机制在请求量大的情况下会造成请求排队阻塞，因此这里面还需要再判断一下当前节点角色，防止在阻塞过程中角色的转变
        if n.getRaftRole() == gROLE_RAFT_LEADER && items != nil {
            var entry = LogEntry {
                Id    : n.makeLogId(),
                Act   : msg.Head,
                Items : items,
            }
            if n.sendLogEntryToPeers(&entry) {
                n.LogList.PushFront(&entry)
                n.saveLogEntry(&entry)
            } else {
                result = gMSG_REPL_FAILED
            }
        } else {
            result = gMSG_REPL_FAILED
        }
        n.dmutex.Unlock()
    } else {
        result = gMSG_REPL_FAILED
    }
    if result == gMSG_REPL_FAILED {
        glog.Debugfln("data set failed, msg: %s", msg.Body)
    }
    n.sendMsg(conn, result, "")
}

// 当Leader获取数据写入时，直接写入数据请求
// 这里去掉了RAFT的Uncommtted LogEntry流程，算是一个优化，为提高写入性能与保证数据一致性的一个折中方案
// 因此建议客户端在请求失败时应当需要有重试机制
func (n *Node) onMsgReplDataAppendEntry(conn net.Conn, msg *Msg) {
    result := gMSG_REPL_RESPONSE
    var entry LogEntry
    // 这里必须要保证节点当前的数据是和leader同步的，才能执行新的数据写入，否则会造成数据不一致
    // 如果数据不同步的情况下，返回失败，由另外的数据同步线程来处理
    err := gjson.DecodeTo(msg.Body, &entry)
    if msg.Info.LastLogId == n.getLastLogId() && n.getRaftRole() != gROLE_RAFT_LEADER && err == nil {
        n.dmutex.Lock()
        n.LogList.PushFront(&entry)
        n.saveLogEntry(&entry)
        n.dmutex.Unlock()
    } else {
        result = gMSG_REPL_FAILED
    }
    n.sendMsg(conn, result, "")
}

// 发送数据操作到其他节点，保证有另外一个server节点成功，那么该请求便成功
// 这里只处理server节点，client节点通过另外的数据同步线程进行数据同步
func (n *Node) sendLogEntryToPeers(entry *LogEntry) bool {
    // 只有一个leader节点，并且配置是允许单节点运行
    if n.Peers.Size() < 1 && n.getMinNode() == 1 {
        if n.getMinNode() == 1 {
            return true
        } else {
            return false
        }
    }
    // 获取Server节点列表
    list  := n.getAliveServerNodes()
    total := int32(len(list))
    if total == 0 {
        return true
    }

    var doneCount int32 = 0 // 成功的请求数
    var failCount int32 = 0 // 失败的请求数
    result   := true
    entrystr := gjson.Encode(*entry)
    for _, v := range list {
        info := v
        go func(info *NodeInfo, entrystr string) {
            msg, err := n.sendAndReceiveMsgToNode(info, gPORT_REPL, gMSG_REPL_DATA_APPENDENTRY, entrystr)
            if err == nil && (msg != nil && msg.Head == gMSG_REPL_RESPONSE) {
                atomic.AddInt32(&doneCount, 1)
            } else {
                atomic.AddInt32(&failCount, 1)
            }
        }(&info, entrystr)
    }
    // 等待执行结束，超时时间60秒
    timeout := gtime.Second() + 60
    for {
        if atomic.LoadInt32(&doneCount) > 0 {
            result = true
            break;
        } else if atomic.LoadInt32(&failCount) == total {
            result = false
            break;
        } else if gtime.Second() >= timeout {
            result = false
            break;
        }
        time.Sleep(10 * time.Microsecond)
    }
    //glog.Debugfln("success count:%d, failed count:%d, total count:%d", atomic.LoadInt32(&doneCount), atomic.LoadInt32(&failCount), total)
    return result
}

// 获取存货的节点数，并做缓存处理，以提高读取效率
func (n *Node) getAliveServerNodes() []NodeInfo {
    key    := "dister_cached_server_nodes"
    result := gcache.Get(key)
    if result != nil {
        return result.([]NodeInfo)
    } else {
        list := make([]NodeInfo, 0)
        for _, v := range n.Peers.Values() {
            info := v.(NodeInfo)
            if info.Status != gSTATUS_ALIVE || info.Role != gROLE_SERVER {
                continue
            }
            list = append(list, info)
        }
        gcache.Set(key, list, 1000)
        return list
    }
}

// Follower->Leader的配置同步
func (n *Node) onMsgConfigFromFollower(conn net.Conn, msg *Msg) {
    //glog.Println("config replication from", msg.Info.Name)
    j := gjson.DecodeToJson(msg.Body)
    if j != nil {
        // 初始化节点列表，包含自定义的所需添加的服务器IP或者域名列表
        peers := j.GetArray("Peers")
        if peers != nil {
            for _, v := range peers {
                ip := v.(string)
                if ip == n.Ip || n.Peers.Contains(ip) {
                    continue
                }
                go n.sayHi(ip)
            }
        }
    }
    conn.Close()
    //glog.Println("config replication from", msg.Info.Name, "done")
}

// Leader同步Peers信息到Follower
func (n *Node) onMsgPeersUpdate(conn net.Conn, msg *Msg) {
    //glog.Println("receive peers update", msg.Body)
    m  := make([]NodeInfo, 0)
    id := n.getId()
    ip := n.getIp()
    if gjson.DecodeTo(msg.Body, &m) == nil {
        set := gset.NewStringSet()
        // 添加节点
        for _, v := range m {
            set.Add(v.Id)
            if v.Id != id {
                n.updatePeerInfo(v)
            } else if v.Ip != ip {
                n.setIp(v.Ip)
            }
        }
        // 删除leader不存在的节点
        for _, v := range n.Peers.Values() {
            info := v.(NodeInfo)
            if !set.Contains(info.Id) {
                n.Peers.Remove(info.Id)
            }
        }
    }
    conn.Close()
}

// Service删除
func (n *Node) onMsgApiServiceRemove(conn net.Conn, msg *Msg) {
    list := make([]string, 0)
    if gjson.DecodeTo(msg.Body, &list) == nil {
        if n.removeServiceByNames(list) {
            n.setLastServiceLogId(gtime.Millisecond())
        }
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

// Service设置
// 新写入的服务不做同步，等待服务健康检查后再做同步
func (n *Node) onMsgApiServiceSet(conn net.Conn, msg *Msg) {
    var sc ServiceConfig
    if gjson.DecodeTo(msg.Body, &sc) == nil {
        n.removeServiceByNames([]string{sc.Name})
        for k, v := range sc.Node {
            key := n.getServiceKeyByNameAndIndex(sc.Name, k)
            n.Service.Set(key, Service{ sc.Type, v })
            n.setLastServiceLogId(gtime.Millisecond())
        }
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

// kv删除
func (n *Node) onMsgReplDataRemove(conn net.Conn, msg *Msg) {
    n.onMsgReplDataSet(conn, msg)
}

// 保存日志数据
func (n *Node) saveLogEntry(entry *LogEntry) {
    lastLogId := n.getLastLogId()
    if entry.Id <= lastLogId {
        // 在高并发下，有可能会出现这种情况，提出警告
        glog.Errorfln("expired log entry, received:%d, current:%d", entry.Id, lastLogId)
        return
    }
    // 首先记录日志(不做缓存，直接写入，防止数据丢失)
    n.saveLogEntryToFile(entry)
    // 其次写入DataMap
    n.saveLogEntryToVar(entry)
    // 保存最新的LogId到内存
    n.setLastLogId(entry.Id)
}

// 保存LogEntry到日志文件中
func (n *Node) saveLogEntryToFile(entry *LogEntry) {
    c := fmt.Sprintf("%d,%d,%s\n", entry.Id, entry.Act, gjson.Encode(entry.Items))
    gfile.PutBinContentsAppend(n.getLogEntryFileSavePathById(entry.Id), []byte(c))
}

// 保存LogEntry到内存变量
func (n *Node) saveLogEntryToVar(entry *LogEntry) {
    switch entry.Act {
    case gMSG_REPL_DATA_SET:
        for k, v := range entry.Items.(map[string]interface{}) {
            n.DataMap.Set(k, v.(string))
        }

    case gMSG_REPL_DATA_REMOVE:
        for _, v := range entry.Items.([]interface{}) {
            n.DataMap.Remove(v.(string))
        }
    }
}

// 数据同步，更新本地数据
func (n *Node) onMsgReplDataReplication(conn net.Conn, msg *Msg) {
    n.updateDataFromRemoteNode(conn, msg)
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

// 从目标节点同步数据，采用增量模式
// follower<-leader
func (n *Node) updateDataFromRemoteNode(conn net.Conn, msg *Msg) {
    if n.getLastLogId() < msg.Info.LastLogId {
        array := make([]LogEntry, 0)
        err   := gjson.DecodeTo(msg.Body, &array)
        if err != nil {
            glog.Error(err)
            return
        }
        length := len(array)
        //glog.Debugfln("receive data replication update from: %s, start logid: %d, end logid: %d, size: %d", msg.Info.Name, array[0].Id, array[len(array)-1].Id, length)
        if array != nil && length > 0 {
            n.dmutex.Lock()
            for _, v := range array {
                if v.Id > n.getLastLogId() {
                    entry := v
                    n.LogList.PushFront(&entry)
                    n.saveLogEntry(&entry)
                }
            }
            n.dmutex.Unlock()
        }
    }
}

// 同步数据到目标节点，采用增量模式，在数据长连接中进行调用，采用单线程，不会存在并发情况，因此不用做同步判断
// leader->follower
func (n *Node) updateDataToRemoteNode(conn net.Conn, info *NodeInfo) {
    // 支持分批同步，如果数据量大，每一次增量同步大小不超过1万条
    logid := info.LastLogId
    if n.getLastLogId() > info.LastLogId {
        if !n.isValidLogId(logid) {
            glog.Errorfln("invalid log id: %d from %s, maybe it's a node from another cluster, or its data was collapsed.", logid, info.Name)
            glog.Errorfln("%s is now removed from this cluster, please manually check and repair this error, and then re-add it to cluster.", info.Name)
            n.Peers.Remove(info.Id)
            return
        }

        //glog.Println("start data incremental replication from", n.getName(), "to", info.Name)
        for {
            // 每批次的数据量要考虑到目标节点写入的时间会不会超过TCP读取等待超时时间
            list   := n.getLogEntriesByLastLogId(logid, 10000, false)
            length := len(list)
            if length > 0 {
                //glog.Debugfln("data incremental replication from %s to %s, start logid: %d, end logid: %d, size: %d", n.getName(), info.Name, list[0].Id, list[length-1].Id, length)
                if err := n.sendMsg(conn, gMSG_REPL_DATA_REPLICATION, gjson.Encode(list)); err != nil {
                    glog.Error(err)
                    time.Sleep(time.Second)
                    return
                }
                if rmsg := n.receiveMsgWithTimeout(conn, 10*time.Second); rmsg != nil {
                    logid = rmsg.Info.LastLogId
                    if n.getLastLogId() == logid {
                        return
                    }
                } else {
                    return
                }
            } else {
                glog.Debugfln("data incremental replication from %s to %s failed, logid: %d, current: %d", n.getName(), info.Name, logid, n.getLastLogId())
                time.Sleep(time.Second)
                return
            }
        }
    }
}

// 根据一个非法的logid查找日志中对应的比其下的有效logid，以便做同步
func (n *Node) getValidLogIdFromGreaterLogId(info *NodeInfo) int64 {
    var result int64 = -1
    logid := info.LastLogId
    for {
        list   := n.getLogEntriesByLastLogId(logid, 1000, false)
        length := len(list)
        if length > 0 {
            // 需要把原始Logid附带过去，以便目标节点做对比
            ids   := make([]int64, length + 1)
            ids[0] = logid
            for k, v := range list {
                ids[k + 1] = v.Id
            }
            msg, err := n.sendAndReceiveMsgToNode(info, gPORT_REPL, gMSG_REPL_VALID_LOGID_CHECK, gjson.Encode(ids))
            if err == nil && msg.Head == gMSG_REPL_RESPONSE {
                if msg.Body != "" {
                    r, err := strconv.ParseInt(msg.Body, 10, 64)
                    if err == nil {
                        return r
                    } else {
                        return result
                    }
                }
            } else {
                return result
            }
            logid = list[length - 1].Id
        } else {
            break
        }
    }
    return result
}

// 两个节点对比有效logid
func (n *Node) onMsgReplValidLogIdCheck(conn net.Conn, msg *Msg) {
    var ids []int64
    result := ""
    if gjson.DecodeTo(msg.Body, &ids) == nil {
        length := len(ids)
        if length > 0 {
            list := n.getLogEntriesByLastLogId(ids[0], length - 1, false)
            if len(list) > 0 {
                // 升序
                for i, id := range ids {
                    if i == 0 {
                        continue
                    }
                    for _, v := range list {
                        if id <= v.Id {
                            result = strconv.FormatInt(v.Id, 10)
                            break
                        }
                    }
                    if result != "" {
                        break
                    }
                }
            } else {
                result = "0"
            }
        }
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, result)
}

// 新增节点,通过IP添加
func (n *Node) onMsgApiPeersAdd(conn net.Conn, msg *Msg) {
    list := make([]string, 0)
    gjson.DecodeTo(msg.Body, &list)
    if list != nil && len(list) > 0 {
        for _, ip := range list {
            if n.Peers.Contains(ip) {
                continue
            }
            glog.Printf("adding peer: %s\n", ip)
            n.updatePeerInfo(NodeInfo{Id: ip, Ip: ip})
        }
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

// 删除节点，目前通过IP删除，效率较低
func (n *Node) onMsgApiPeersRemove(conn net.Conn, msg *Msg) {
    list := make([]string, 0)
    gjson.DecodeTo(msg.Body, &list)
    if list != nil && len(list) > 0 {
        peers := n.Peers.Values()
        for _, ip := range list {
            for _, v := range peers {
                info := v.(NodeInfo)
                if ip == info.Ip {
                    glog.Printf("removing peer: %s, ip: %s\n", info.Name, info.Ip)
                    n.Peers.Remove(info.Id)
                    break
                }
            }
        }
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

