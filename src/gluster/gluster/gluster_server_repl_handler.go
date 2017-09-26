// 数据同步需要注意的是，必须在节点完成election选举之后才能进行数据通信。
// 也就是说repl通信需要建立在raft成功作为前提
package gluster

import (
    "net"
    "g/encoding/gjson"
    "g/core/types/gmap"
    "g/util/gtime"
    "g/os/glog"
    "time"
    "fmt"
    "g/core/types/gset"
)

// 集群数据同步接口回调函数
func (n *Node) replTcpHandler(conn net.Conn) {
    msg := n.receiveMsg(conn)
    // 判断集群基础信息
    if msg == nil || msg.Info.Group != n.Group  || msg.Info.Version != gVERSION {
        //glog.Println("receive nil, auto close conn")
        conn.Close()
        return
    }
    // 判断集群权限，只有本集群的节点之间才能进行数据通信(判断id)
    leader := n.getLeader()
    if leader == nil || (leader.Id != msg.Info.Id && !n.Peers.Contains(msg.Info.Id)) {
        //glog.Printf("invalid peer, id: %s, name: %s\n", msg.Info.Id, msg.Info.Name)
        conn.Close()
        return
    }
    switch msg.Head {
        case gMSG_REPL_DATA_SET:                    n.onMsgReplDataSet(conn, msg)
        case gMSG_REPL_DATA_REMOVE:                 n.onMsgReplDataRemove(conn, msg)
        case gMSG_API_PEERS_ADD:                    n.onMsgApiPeersAdd(conn, msg)
        case gMSG_API_PEERS_REMOVE:                 n.onMsgApiPeersRemove(conn, msg)
        case gMSG_REPL_PEERS_UPDATE:                n.onMsgPeersUpdate(conn, msg)
        case gMSG_REPL_DATA_REPLICATION:            n.onMsgReplDataReplication(conn, msg)
        case gMSG_REPL_CONFIG_FROM_FOLLOWER:        n.onMsgConfigFromFollower(conn, msg)
        case gMSG_REPL_SERVICE_COMPLETELY_UPDATE:   n.onMsgServiceCompletelyUpdate(conn, msg)
        case gMSG_API_SERVICE_SET:                  n.onMsgServiceSet(conn, msg)
        case gMSG_API_SERVICE_REMOVE:               n.onMsgServiceRemove(conn, msg)
    }
    //这里不用自动关闭链接，由于链接有读取超时，当一段时间没有数据时会自动关闭
    n.replTcpHandler(conn)
}

// kv设置，严格按照RAFT算法保证数据的强一致性
// 这里增加了一把数据锁，以保证请求的先进先出队列执行，因此写效率会有所降低
func (n *Node) onMsgReplDataSet(conn net.Conn, msg *Msg) {
    result := gMSG_REPL_RESPONSE
    if n.getRaftRole() == gROLE_RAFT_LEADER {
        var items interface{}
        if gjson.DecodeTo(msg.Body, &items) == nil {
            n.dmutex.Lock()
            var entry = LogEntry {
                Id    : n.makeLogId(),
                Act   : msg.Head,
                Items : items,
            }
            n.LogList.PushFront(&entry)
            n.saveLogEntry(&entry)
            n.dmutex.Unlock()
        }
    } else {
        result = gMSG_REPL_FAILED
    }
    n.sendMsg(conn, result, "")
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
func (n *Node) onMsgServiceCompletelyUpdate(conn net.Conn, msg *Msg) {
    n.updateServiceFromRemoteNode(conn, msg)
}

// Service删除
func (n *Node) onMsgServiceRemove(conn net.Conn, msg *Msg) {
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
func (n *Node) onMsgServiceSet(conn net.Conn, msg *Msg) {
    var sc ServiceConfig
    if gjson.DecodeTo(msg.Body, &sc) == nil {
        n.removeServiceByNames([]string{sc.Name})
        for k, v := range sc.Node {
            key := n.getServiceKeyByNameAndIndex(sc.Name, k)
            n.Service.Set(key, Service{ sc.Type, v })
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
    if entry.Id < lastLogId {
        // 在高并发下，有可能会出现这种情况，提出警告
        glog.Warning(fmt.Sprintf("expired log entry, received:%d, current:%d", entry.Id, lastLogId))
    }
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
    n.setLastLogId(entry.Id)
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
            for _, v := range array {
                if v.Id > n.getLastLogId() {
                    entry := v
                    n.LogList.PushFront(&entry)
                    n.saveLogEntry(&entry)
                }
            }
        }
    }
}

// 同步数据到目标节点，采用增量模式，在数据长连接中进行调用，采用单线程，不会存在并发情况，因此不用做同步判断
// leader->follower
func (n *Node) updateDataToRemoteNode(conn net.Conn, info *NodeInfo) {
    // 支持分批同步，如果数据量大，每一次增量同步大小不超过1万条
    logid := info.LastLogId
    if n.getLastLogId() > info.LastLogId {
        if logid == 0 || (logid != 0 && n.isValidLogId(logid)) {
            //glog.Println("start data incremental replication from", n.getName(), "to", info.Name)
            for {
                // 每批次的数据量要考虑到目标节点写入的时间会不会超过TCP读取等待超时时间
                list   := n.getLogEntriesByLastLogId(logid, 10000)
                length := len(list)
                if length > 0 {
                    glog.Debugfln("data incremental replication from %s to %s, start logid: %d, end logid: %d, size: %d", n.getName(), info.Name, list[0].Id, list[length-1].Id, length)
                    if err := n.sendMsg(conn, gMSG_REPL_DATA_REPLICATION, gjson.Encode(list)); err != nil {
                        glog.Error(err)
                        return
                    }
                    if rmsg := n.receiveMsgWithTimeout(conn, 10*time.Second); rmsg != nil {
                        n.updatePeerInfo(rmsg.Info)
                        logid = rmsg.Info.LastLogId
                        if n.getLastLogId() == logid {
                            break;
                        }
                    } else {
                        break
                    }
                } else {
                    glog.Debugfln("data incremental replication from %s to %s failed, logid: %d, current: %d", n.getName(), info.Name, logid, n.getLastLogId())
                    break
                }
            }
        } else {
            glog.Debugfln("failed in data replication from %s to %s, invalid log id: %d, current: %d", n.getName(), info.Name, logid, n.getLastLogId())
        }
    }
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
    if err := n.sendMsg(conn, gMSG_REPL_SERVICE_COMPLETELY_UPDATE, serviceJson); err != nil {
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
                    break;
                }
            }
        }
    }
    n.sendMsg(conn, gMSG_REPL_RESPONSE, "")
}

