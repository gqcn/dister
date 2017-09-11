// 数据同步需要注意的是：
// leader只有在通知完所有follower更新完数据之后，自身才会进行数据更新
// 因此leader
package gluster

import (
    "net"
    "g/encoding/gjson"
    "g/core/types/gmap"
    "g/util/gtime"
    "g/os/glog"
    "sync/atomic"
    "time"
    "fmt"
)

// 集群数据同步接口回调函数
func (n *Node) replTcpHandler(conn net.Conn) {
    msg := n.receiveMsg(conn)
    if msg == nil || msg.Info.Group != n.Group {
        //glog.Println("receive nil")
        conn.Close()
        return
    }
    switch msg.Head {
        case gMSG_REPL_DATA_SET:                    n.onMsgReplDataSet(conn, msg)
        case gMSG_REPL_DATA_REMOVE:                 n.onMsgReplDataRemove(conn, msg)
        case gMSG_REPL_HEARTBEAT:                   n.onMsgReplHeartbeat(conn, msg)
        case gMSG_API_PEERS_ADD:                    n.onMsgApiPeersAdd(conn, msg)
        case gMSG_API_PEERS_REMOVE:                 n.onMsgApiPeersRemove(conn, msg)
        case gMSG_REPL_PEERS_UPDATE:                n.onMsgPeersUpdate(conn, msg)
        case gMSG_REPL_INCREMENTAL_UPDATE:          n.onMsgReplUpdate(conn, msg)
        case gMSG_REPL_COMPLETELY_UPDATE:           n.onMsgReplUpdate(conn, msg)
        case gMSG_REPL_CONFIG_FROM_FOLLOWER     :   n.onMsgConfigFromFollower(conn, msg)
        case gMSG_REPL_SERVICE_COMPLETELY_UPDATE:   n.onMsgServiceCompletelyUpdate(conn, msg)
        case gMSG_API_SERVICE_SET:                  n.onMsgServiceSet(conn, msg)
        case gMSG_API_SERVICE_REMOVE:               n.onMsgServiceRemove(conn, msg)
    }
    //这里不用自动关闭链接，由于链接有读取超时，当一段时间没有数据时会自动关闭
    n.replTcpHandler(conn)
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
                if ip == n.Ip || n.Peers.Contains(ip){
                    continue
                }
                go func(ip string) {
                    if !n.sayHi(ip) {
                        n.updatePeerInfo(NodeInfo{Id: ip, Ip: ip})
                    }
                }(ip)
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
        for _, v := range m {
            if v.Id != id {
                n.updatePeerInfo(v)
            } else if v.Ip != ip {
                n.setIp(v.Ip)
            }
        }
    }
    conn.Close()
}

// 心跳消息提交的完整更新消息
func (n *Node) onMsgServiceCompletelyUpdate(conn net.Conn, msg *Msg) {
    n.updateServiceFromRemoteNode(conn, msg)
}

// Service删除
func (n *Node) onMsgServiceRemove(conn net.Conn, msg *Msg) {
    list := make([]interface{}, 0)
    if gjson.DecodeTo(msg.Body, &list) == nil {
        updated := false
        for _, name := range list {
            if n.Service.Contains(name.(string)) {
                n.Service.Remove(name.(string))
                updated = true
            }
        }
        if updated {
            n.setLastServiceLogId(gtime.Microsecond())
            n.setServiceDirty(true)
        }
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

// Service设置
func (n *Node) onMsgServiceSet(conn net.Conn, msg *Msg) {
    var st ServiceStruct
    if gjson.DecodeTo(msg.Body, &st) == nil {
        n.Service.Set(st.Name, *n.serviceSructToService(&st))
        n.ServiceForApi.Set(st.Name, st)
        n.setLastServiceLogId(gtime.Microsecond())
        n.setServiceDirty(true)
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

// kv删除
func (n *Node) onMsgReplDataRemove(conn net.Conn, msg *Msg) {
    n.onMsgReplDataSet(conn, msg)
}

// kv设置，最终一致性
func (n *Node) onMsgReplDataSet(conn net.Conn, msg *Msg) {
    n.setStatusInReplication(true)
    defer n.setStatusInReplication(false)

    result := gMSG_REPL_RESPONSE
    if n.getRaftRole() == gROLE_RAFT_LEADER {
        var items interface{}
        if gjson.DecodeTo(msg.Body, &items) == nil {
            var entry = LogEntry {
                Id    : n.makeLogId(),
                Act   : msg.Head,
                Items : items,
            }
            n.saveLogEntry(entry)
            n.LogList.PushFront(entry)
            if !n.sendLogEntryToPeers(entry) {
                result = gMSG_REPL_FAILED
            }
        }
    } else {
        var entry LogEntry
        if gjson.DecodeTo(msg.Body, &entry) == nil {
            n.saveLogEntry(entry)
            n.LogList.PushFront(entry)
        } else {
            result = gMSG_REPL_FAILED
        }
    }
    n.sendMsg(conn, result, "")
}

// 发送数据操作到其他节点,为保证数据的一致性，只要有另外1个节点返回成功后，我们就认为该数据请求成功，
// 即使在处理过程中leader挂掉，只要有另外一个节点有最新的请求数据，那么就算重新进行选举，也会选举到数据最新的那个节点作为leader
// 这里的机制类似于主从备份的原理，当然由于这里采用了异步并发请求的机制，如果集群存在多个其他节点，出现仅有一个节点成功的概念很小，出现所有节点都失败的概率更小
func (n *Node) sendLogEntryToPeers(entry LogEntry) bool {
    // 只有一个leader节点，并且配置是允许单节点运行
    if n.Peers.Size() < 1 {
        if n.getRaftRole() == gROLE_RAFT_LEADER && n.getMinNode() == 1 {
            return true
        } else {
            return false
        }
    }
    n.setStatusInReplication(true)
    defer n.setStatusInReplication(false)

    var scount     int32 = 0
    var fcount     int32 = 0
    var aliveCount int32 = 0
    result := false
    for _, v := range n.Peers.Values() {
        info := v.(NodeInfo)
        if info.Status != gSTATUS_ALIVE {
            continue
        }
        aliveCount++
        go func(info *NodeInfo, entry LogEntry) {
            conn := n.getConn(info.Ip, gPORT_REPL)
            if conn == nil {
                atomic.AddInt32(&fcount, 1)
                return
            }
            defer conn.Close()
            if n.sendMsg(conn, entry.Act, gjson.Encode(entry)) == nil {
                msg := n.receiveMsg(conn)
                if msg != nil && msg.Head == gMSG_REPL_RESPONSE {
                    atomic.AddInt32(&scount, 1)
                } else {
                    atomic.AddInt32(&fcount, 1)
                }
            }
        }(&info, entry)
    }
    for aliveCount > 0 {
        if atomic.LoadInt32(&scount) > 0 {
            result = true
            break;
        }
        if atomic.LoadInt32(&fcount) == aliveCount {
            break;
        }
        time.Sleep(time.Millisecond)
    }

    return result
}

// 心跳响应，用于数据的同步，包括：Peers、DataMap、Service
func (n *Node) onMsgReplHeartbeat(conn net.Conn, msg *Msg) {
    result := gMSG_REPL_HEARTBEAT
    // 如果当前节点正处于数据同步中，那么本次心跳不再执行同步判断
    if !n.getStatusInReplication() {
        lastLogId := n.getLastLogId()
        if lastLogId < msg.Info.LastLogId {
            result = gMSG_REPL_NEED_UPDATE_FOLLOWER
        } else if lastLogId > msg.Info.LastLogId {
            result = gMSG_REPL_NEED_UPDATE_LEADER
        } else {
            // service同步检测
            lastServiceLogId := n.getLastServiceLogId()
            if lastServiceLogId < msg.Info.LastServiceLogId {
                result = gMSG_REPL_SERVICE_NEED_UPDATE_FOLLOWER
            } else if lastServiceLogId > msg.Info.LastServiceLogId {
                result = gMSG_REPL_SERVICE_NEED_UPDATE_LEADER
            }
        }
    }

    switch result {
        case gMSG_REPL_NEED_UPDATE_LEADER:          n.updateDataToRemoteNode(conn, msg)
        case gMSG_REPL_SERVICE_NEED_UPDATE_LEADER:  n.updateServiceToRemoteNode(conn, msg)
        default:
            n.sendMsg(conn, result, "")
    }
}

// 数据同步，更新本地数据
func (n *Node) onMsgReplUpdate(conn net.Conn, msg *Msg) {
    //glog.Println("receive data replication update from", msg.Info.Name)
    n.updateDataFromRemoteNode(conn, msg)
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

// 保存日志数据
func (n *Node) saveLogEntry(entry LogEntry) {
    lastLogId := n.getLastLogId()
    if entry.Id < lastLogId {
        glog.Warning(fmt.Sprintf("expired log entry, received:%v, current:%v\n", entry.Id, lastLogId))
        return
    }
    switch entry.Act {
        case gMSG_REPL_DATA_SET:
            //glog.Println("setting log entry", entry)
            for k, v := range entry.Items.(map[string]interface{}) {
                n.DataMap.Set(k, v.(string))
            }

        case gMSG_REPL_DATA_REMOVE:
            //glog.Println("removing log entry", entry)
            for _, v := range entry.Items.([]interface{}) {
                n.DataMap.Remove(v.(string))
            }

    }
    n.addLogCount()
    n.setLastLogId(entry.Id)
    n.setDataDirty(true)
}

// 从目标节点同步数据，采用增量+全量模式
func (n *Node) updateDataFromRemoteNode(conn net.Conn, msg *Msg) {
    // 如果有goroutine正在同步，那么放弃本次同步
    if n.getStatusInReplication() {
        return
    }
    if msg.Head == gMSG_REPL_INCREMENTAL_UPDATE {
        // 增量同步，LogCount和LastLogId会根据保存的LogEntry自动更新
        if n.getLastLogId() < msg.Info.LastLogId {
            n.updateFromLogEntriesJson(msg.Body)
        }
    } else {
        // 全量同步，完整的kv数据覆盖
        m   := make(map[string]string)
        err := gjson.DecodeTo(msg.Body, &m)
        if err == nil {
            newm := gmap.NewStringStringMap()
            newm.BatchSet(m)
            n.setDataMap(newm)
            n.setLogCount(msg.Info.LogCount)
            n.setLastLogId(msg.Info.LastLogId)
            n.setDataDirty(true)
        } else {
            glog.Error(err)
        }
    }
}

// 同步数据到目标节点，采用增量+全量模式
func (n *Node) updateDataToRemoteNode(conn net.Conn, msg *Msg) {
    n.setStatusInReplication(true)
    defer n.setStatusInReplication(false)

    //glog.Println("send data replication update to", msg.Info.Name)
    // 首先进行增量同步
    increUpdated := true
    // 如果增量同步的数据量大小超过DataMap的大小，那么直接采用全量模式，否则采用增量模式
    // 注意这里的logid都除以10000是因为logid的后四位是随机数，用以降低冲突的可能性，不是真实的id
    // 如果数据量大支持分批同步，每一次增量同步大小不超过10万条
    if (n.getLastLogId()/10000 - msg.Info.LastLogId/10000 < int64(n.DataMap.Size())) && n.isValidLogId(msg.Info.LastLogId) {
        for {
            list    := n.getLogEntriesByLastLogId(msg.Info.LastLogId, 100000)
            length  := len(list)
            if length > 0 {
                if err := n.sendMsg(conn, gMSG_REPL_INCREMENTAL_UPDATE, gjson.Encode(list)); err != nil {
                    glog.Error(err)
                    return
                }
                if rmsg := n.receiveMsg(conn); rmsg != nil {
                    if n.getLastLogId() == rmsg.Info.LastLogId {
                        break;
                    }
                } else {
                    increUpdated = false
                }
            } else {
                increUpdated = false
            }
            if !increUpdated {
                break
            }
        }
    } else {
        increUpdated = false
    }

    if !increUpdated {
        // 如果增量同步失败，或者判断需要完整同步，则采用全量同步
        if err := n.sendMsg(conn, gMSG_REPL_COMPLETELY_UPDATE, gjson.Encode(*n.DataMap.Clone())); err != nil {
            glog.Error(err)
            return
        }
        n.receiveMsg(conn)
    }
}

// 从目标节点同步Service数据
func (n *Node) updateServiceFromRemoteNode(conn net.Conn, msg *Msg) {
    //glog.Println("receive service replication update from", msg.Info.Name)
    m   := make(map[string]ServiceStruct)
    err := gjson.DecodeTo(msg.Body, &m)
    if err == nil {
        newmForService    := gmap.NewStringInterfaceMap()
        newmForServiceApi := gmap.NewStringInterfaceMap()
        for k, v := range m {
            newmForService.Set(k, *n.serviceSructToService(&v))
            newmForServiceApi.Set(k, v)
        }
        n.setService(newmForService)
        n.setServiceForApi(newmForServiceApi)
        n.setLastServiceLogId(msg.Info.LastServiceLogId)
        n.setServiceDirty(true)
    } else {
        glog.Error(err)
    }
}

// 同步Service到目标节点
func (n *Node) updateServiceToRemoteNode(conn net.Conn, msg *Msg) {
    //glog.Println("send service replication update to", msg.Info.Name)
    if err := n.sendMsg(conn, gMSG_REPL_SERVICE_COMPLETELY_UPDATE, gjson.Encode(*n.ServiceForApi.Clone())); err != nil {
        glog.Error(err)
        return
    }
}

