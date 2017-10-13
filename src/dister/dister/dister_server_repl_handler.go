// 数据同步需要注意的是，必须在节点完成election选举之后才能进行数据通信。
// 也就是说repl通信需要建立在raft成功作为前提
package dister

import (
    "net"
    "g/encoding/gjson"
    "g/core/types/gmap"
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
        case gMSG_REPL_PEERS_UPDATE:                n.onMsgReplPeersUpdate(conn, msg)
        case gMSG_REPL_SERVICE_UPDATE:              n.onMsgReplServiceUpdate(conn, msg)
        case gMSG_REPL_VALID_LOGID_CHECK_FIX:       n.onMsgReplValidLogIdCheckFix(conn, msg)
        case gMSG_REPL_CONFIG_FROM_FOLLOWER:        n.onMsgReplConfigFromFollower(conn, msg)
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
            if n.sendAppendLogEntryToPeers(&entry) {
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
// 这里将RAFT的Uncommtted LogEntry和Append LogEntry请求合并为一个请求，算是一个优化，为提高写入性能与保证数据一致性的一个折中方案
// 因此建议客户端在请求失败时应当需要有重试机制
func (n *Node) onMsgReplDataAppendEntry(conn net.Conn, msg *Msg) {
    result := gMSG_REPL_RESPONSE
    var entry LogEntry
    // 这里必须要保证节点当前的数据是和leader同步的，才能执行新的数据写入，否则会造成数据不一致
    // 如果数据不同步的情况下，返回失败，由另外的数据同步线程来处理
    err := gjson.DecodeTo(msg.Body, &entry)
    if msg.Info.LastLogId == n.getLastLogId() && n.getRaftRole() != gROLE_RAFT_LEADER && err == nil {
        n.dmutex.Lock()
        n.saveLogEntry(&entry)
        n.dmutex.Unlock()
    } else {
        result = gMSG_REPL_FAILED
    }
    n.sendMsg(conn, result, "")
}

// 发送数据操作到其他节点，保证有另外一个server节点成功，那么该请求便成功
// 这里只处理server节点，client节点通过另外的数据同步线程进行数据同步
func (n *Node) sendAppendLogEntryToPeers(entry *LogEntry) bool {
    // 获取存活的Server节点列表
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

// 获取存活的Server节点数，并做缓存处理，以提高读取效率
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
func (n *Node) onMsgReplConfigFromFollower(conn net.Conn, msg *Msg) {
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
func (n *Node) onMsgReplPeersUpdate(conn net.Conn, msg *Msg) {
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

// 心跳消息提交的完整更新消息
func (n *Node) onMsgReplServiceUpdate(conn net.Conn, msg *Msg) {
    n.updateServiceFromRemoteNode(conn, msg)
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
    p := n.getLogEntryFileSavePathById(entry.Id)
    gfile.PutBinContentsAppend(p, []byte(c))
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
        glog.Debugfln("receive data replication update from: %s, start logid: %d, end logid: %d, size: %d", msg.Info.Name, array[0].Id, array[len(array)-1].Id, length)
        if array != nil && length > 0 {
            n.dmutex.Lock()
            for _, v := range array {
                if v.Id > n.getLastLogId() {
                    entry := v
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
    if n.getLastLogId() > logid {
        // 不合法的logid，有可能是数据不一致(小概率事件)，也可能是不同集群节点进行合并(人为操作问题)
        // 这个时候我们总认为Leader是正确的，对节点数据进行强制性覆盖
        if !n.isValidLogId(logid) {
            n.checkAndFixNodeData(info)
            return
        }
        //glog.Println("start data incremental replication from", n.getName(), "to", info.Name)
        for {
            // 每批次的数据量要考虑到目标节点写入的时间会不会超过TCP读取等待超时时间
            list   := n.getLogEntriesByLastLogId(logid, 10000, false)
            length := len(list)
            if length > 0 {
                glog.Debugfln("data incremental replication from %s to %s, start logid: %d, end logid: %d, size: %d", n.getName(), info.Name, list[0].Id, list[length-1].Id, length)
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

// 对非法logid的节点进行数据修复，机制如下：
// 寻找两者之间合法的一个logid，从该logid往后进行日志复制及执行
func (n *Node) checkAndFixNodeData(info *NodeInfo) {
    checkingKey := "dister_check_and_fix_data_" + info.Id
    if gcache.Get(checkingKey) != nil {
        glog.Debugfln("checking and fixing data of node: %s", info.Name)
        return
    }
    glog.Printfln("invalid logid %d from %s, start data checking and fixing...", info.LastLogId, info.Name)
    gcache.Set(checkingKey, struct {}{}, 3600000)
    defer gcache.Remove(checkingKey)

    logid := info.LastLogId
    for {
        // 每批次往前查找size条日志
        size   := 1000
        list   := n.getLogEntriesByLastLogId(logid - int64(size*gLOGENTRY_RANDOM_ID_SIZE), size, false)
        length := len(list)
        if length > 0 {
            ids := make([]int64, length + 1)
            for k, v := range list {
                if v.Id >= logid {
                    break;
                }
                ids[k] = v.Id
            }
            glog.Debugfln("sending check logids: %v", ids)
            msg, err := n.sendAndReceiveMsgToNode(info, gPORT_REPL, gMSG_REPL_VALID_LOGID_CHECK_FIX, gjson.Encode(ids))
            if err == nil && msg.Head == gMSG_REPL_RESPONSE {
                if msg.Body != "-1" {
                    return
                }
            }
            logid = list[0].Id
        } else {
            break
        }
    }
}

// 两个节点对比有效logid，返回一个有效的logid做数据同步
// 机制：保证至少连续3对logid匹配，并且匹配的时候如果遇到匹配的logid，必须保证其后的所有logid都连续匹配
func (n *Node) onMsgReplValidLogIdCheckFix(conn net.Conn, msg *Msg) {
    var ids   []int64
    var result  int64 = -1
    if gjson.DecodeTo(msg.Body, &ids) == nil {
        length := len(ids)
        if length > 0 {
            list    := n.getLogEntriesByLastLogId(ids[0] - 1, length, false)
            length2 := len(list)
            glog.Debugfln("check local log entries: %v", list)
            if length2 > 0 {
                // 升序
                for k, id := range ids {
                    // 升序
                    for k2, entry := range list {
                        if id == entry.Id {
                            // 保证至少连续3对logid匹配
                            if length - k >= 3 && length2 - k2 >= 3 {
                                // 保证其后的所有logid都连续匹配
                                math := true
                                i := k  + 1
                                j := k2 + 1
                                for ; k < length; {
                                    if ids[i] != list[j].Id {
                                        math = false
                                        break
                                    }
                                    i++
                                    j++
                                }
                                if math {
                                    result = entry.Id
                                }
                                break
                            }
                        } else if id < entry.Id {
                            break
                        }
                    }
                    if result > 0 {
                        break
                    }
                }
            }
        }
    }
    // 判断是否logid查找成功，如果成功那么执行文件脏数据清理工作
    if result > 0 {
        n.fixDataMapByLogId(result)
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, strconv.FormatInt(result, 10))
}

// (当数据不一致时)从某一个logid开始修复数据，这个logid是与leader匹配的合法logid
// 机制：将匹配的logid其后的内容去掉，通过另外数据自动同步线程进行更新
func (n *Node) fixDataMapByLogId(logid int64) {
    glog.Printfln("data checking and fixing, found valid logid: %d", logid)
    fromid := n.getLogEntryBatachNo(logid)*gLOGENTRY_FILE_SIZE*gLOGENTRY_RANDOM_ID_SIZE
    list   := n.getLogEntriesByLastLogId(fromid, int((logid - fromid)/gLOGENTRY_RANDOM_ID_SIZE), false)
    // 直接删除logid当前文件其后的存储文件
    tempid := logid
    for {
        path := n.getLogEntryFileSavePathById(tempid)
        if gfile.Exists(path) {
            gfile.Remove(path)
            tempid += gLOGENTRY_FILE_SIZE
        } else {
            break;
        }
    }
    // 将内容重新还原到logid对应的文件中
    if len(list) > 0 {
        for _, v := range list {
            if v.Id > logid {
                break;
            }
            entry := v
            n.saveLogEntryToFile(&entry)
            fromid = v.Id
        }
    } else {
        fromid = 0
    }
    n.reloadDataMap()
    n.setLastLogId(fromid)
}


// 从目标节点同步Service数据
// follower<-leader
func (n *Node) updateServiceFromRemoteNode(conn net.Conn, msg *Msg) {
    glog.Println("receive service replication from", msg.Info.Name)
    defer conn.Close()
    m   := make(map[string]Service)
    err := gjson.DecodeTo(msg.Body, &m)
    if err == nil {
        newm := gmap.NewStringInterfaceMap()
        for k, v := range m {
            newm.Set(k, v)
        }
        n.setService(newm)
        n.setLastServiceLogId(msg.Info.LastServiceLogId)
    } else {
        glog.Error(err)
    }
}

// 同步Service到目标节点
// leader->follower
func (n *Node) updateServiceToRemoteNode(conn net.Conn) {
    serviceJson := gjson.Encode(*n.Service.Clone())
    if err := n.sendMsg(conn, gMSG_REPL_SERVICE_UPDATE, serviceJson); err != nil {
        glog.Error(err)
        return
    }
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

