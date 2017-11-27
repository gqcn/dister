package dister

import (
    "time"
    "gitee.com/johng/gf/g/os/glog"
    "gitee.com/johng/gf/g/container/gset"
)

// 通过心跳维持集群统治，如果心跳不及时，那么选民会重新进入选举流程
// 每一个节点保持一个tcp长连接
func (n *Node) heartbeatHandler() {
    // 存储已经保持心跳的节点
    conns := gset.NewStringSet()
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.Peers.Values() {
                info := v.(NodeInfo)
                if conns.Contains(info.Id) {
                    continue
                }
                // 每个节点单独一个线程进行心跳处理
                go func(id, ip string) {
                    conns.Add(id)
                    defer conns.Remove(id)

                    conn := n.getConn(ip, gPORT_RAFT)
                    if conn == nil {
                        n.updatePeerStatus(id, gSTATUS_DEAD)
                        return
                    }
                    defer conn.Close()

                    // 如果是本地同一节点通信，那么移除掉
                    if n.checkConnInLocalNode(conn) {
                        n.Peers.Remove(id)
                        return
                    }

                    for {
                        // 如果当前节点不再是leader，或者节点表中已经删除该节点信息
                        if n.getRaftRole() != gROLE_RAFT_LEADER || !n.Peers.Contains(id) {
                            return
                        }
                        // 发送心跳
                        if n.sendMsg(conn, gMSG_RAFT_HEARTBEAT, "") != nil {
                            n.updatePeerStatus(id, gSTATUS_DEAD)
                            return
                        }
                        // 接收回复
                        if msg := n.receiveMsg(conn); msg != nil {
                            //glog.Println("receive heartbeat back from:", ip)
                            switch msg.Head {
                                case gMSG_RAFT_I_AM_LEADER:
                                    glog.Printfln("brains split, set leader %s, done heartbeating", msg.Info.Name)
                                    n.setLeader(&(msg.Info))
                                    n.setRaftRole(gROLE_RAFT_FOLLOWER)

                                case gMSG_RAFT_SPLIT_BRAINS_UNSET:
                                    glog.Println("split brains occurred, remove node:", msg.Info.Name)
                                    n.Peers.Remove(msg.Info.Id)

                                default:
                                    time.Sleep(gELECTION_TIMEOUT_HEARTBEAT * time.Millisecond)
                            }
                        }
                    }
                }(info.Id, info.Ip)
            }
        }
        time.Sleep(gELECTION_TIMEOUT_HEARTBEAT * time.Millisecond)
    }
}
