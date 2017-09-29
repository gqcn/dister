package dister

import (
    "net"
    "g/os/glog"
    "g/encoding/gjson"
)

// 集群协议通信接口回调函数
func (n *Node) raftTcpHandler(conn net.Conn) {
    msg := n.receiveMsg(conn)
    if msg == nil || msg.Info.Group != n.Group || msg.Info.Version != gVERSION {
        conn.Close()
        return
    }

    // 消息处理
    switch msg.Head {
        case gMSG_RAFT_HI:                      n.onMsgRaftHi(conn, msg)
        case gMSG_RAFT_HEARTBEAT:               n.onMsgRaftHeartbeat(conn, msg)
        case gMSG_RAFT_SCORE_REQUEST:           n.onMsgRaftScoreRequest(conn, msg)
        case gMSG_RAFT_SCORE_COMPARE_REQUEST:   n.onMsgRaftScoreCompareRequest(conn, msg)
        case gMSG_RAFT_LEADER_COMPARE_REQUEST:  n.onMsgRaftLeaderCompareRequest(conn, msg)
        case gMSG_RAFT_SPLIT_BRAINS_CHECK:      n.onMsgRaftSplitBrainsCheck(conn, msg)
    }
    //这里不用自动关闭链接，由于链接有读取超时，当一段时间没有数据时会自动关闭
    n.raftTcpHandler(conn)
}

// 上线通知
func (n *Node) onMsgRaftHi(conn net.Conn, msg *Msg) {
    n.sendMsg(conn, gMSG_RAFT_HI2, "")
}

// 心跳保持
func (n *Node) onMsgRaftHeartbeat(conn net.Conn, msg *Msg) {
    if n.checkConnInLocalNode(conn) {
        n.Peers.Remove(msg.Info.Id)
        conn.Close()
        return
    }
    result := gMSG_RAFT_HEARTBEAT
    if n.getRaftRole() == gROLE_RAFT_LEADER {
        // 如果是两个leader相互心跳，表示两个leader是连通的，这时根据算法算出一个leader即可
        if n.compareLeaderWithRemoteNode(&msg.Info) {
            result = gMSG_RAFT_I_AM_LEADER
        } else {
            n.setLeader(&msg.Info)
            n.setRaftRole(gROLE_RAFT_FOLLOWER)
        }
    } else if n.getLeader() == nil {
        if n.getLastLogId() > msg.Info.LastLogId {
            // 不返回heartbeat消息，以便引起选举无法进行
            result = gMSG_RAFT_RESPONSE
        } else {
            // 如果没有leader，并且目标节点满足成为本节点leader的条件，那么设置目标节点为leader
            n.setLeader(&msg.Info)
            n.setRaftRole(gROLE_RAFT_FOLLOWER)
        }
    } else {
        // 脑裂问题处理
        if n.getLeader().Id != msg.Info.Id {
            glog.Printf("split brains occurred, heartbeat from: %s, but my leader is: %s\n", msg.Info.Name, n.getLeader().Name)
            leaderConn := n.getConn(n.getLeader().Ip, gPORT_RAFT)
            if leaderConn != nil {
                if n.sendMsg(leaderConn, gMSG_RAFT_SPLIT_BRAINS_CHECK, msg.Info.Ip) == nil {
                    rmsg := n.receiveMsg(leaderConn)
                    if rmsg != nil {
                        switch rmsg.Head {
                            case gMSG_RAFT_SPLIT_BRAINS_UNSET:
                                result = gMSG_RAFT_SPLIT_BRAINS_UNSET
                                glog.Printf("remove %s from my peers\n", msg.Info.Name)
                                n.Peers.Remove(msg.Info.Id)

                            case gMSG_RAFT_SPLIT_BRAINS_CHANGE:
                                n.setLeader(&(msg.Info))
                        }
                    }
                }
                leaderConn.Close()
            } else {
                // 如果leader连接不上，那么表示leader已经死掉，替换为新的leader
                n.setLeader(&msg.Info)
            }
        }
    }
    if result == gMSG_RAFT_HEARTBEAT {
        n.updateElectionDeadline()
    }
    n.sendMsg(conn, result, "")
}

// 检测split brains问题，检查两个leader的连通性
// 如果不连通，那么follower保持当前leader不变
// 如果能够连通，那么需要在两个leader中确定一个
func (n *Node) onMsgRaftSplitBrainsCheck(conn net.Conn, msg *Msg) {
    checkip := msg.Body
    result  := gMSG_RAFT_RESPONSE
    if n.getRaftRole() == gROLE_RAFT_LEADER {
        tconn := n.getConn(checkip, gPORT_RAFT)
        if tconn == nil {
            result = gMSG_RAFT_SPLIT_BRAINS_UNSET
        } else {
            defer tconn.Close()
            if n.sendMsg(tconn, gMSG_RAFT_HI, "") == nil {
                rmsg := n.receiveMsg(tconn)
                if rmsg != nil {
                    if !n.compareLeaderWithRemoteNode(&rmsg.Info) {
                        n.setLeader(&rmsg.Info)
                        n.setRaftRole(gROLE_RAFT_FOLLOWER)
                        result = gMSG_RAFT_SPLIT_BRAINS_CHANGE
                    }
                }
            }
        }
    } else {
        result = gMSG_RAFT_SPLIT_BRAINS_CHANGE
    }
    glog.Printf("brains check result: %d\n", result)
    n.sendMsg(conn, result, "")
}

// 选举比分获取，如果新加入的节点，也会进入到这个方法中
func (n *Node) onMsgRaftScoreRequest(conn net.Conn, msg *Msg) {
    if n.getRaftRole() == gROLE_RAFT_LEADER && n.getLastLogId() >= msg.Info.LastLogId {
        n.sendMsg(conn, gMSG_RAFT_I_AM_LEADER, "")
    } else {
        n.sendMsg(conn, gMSG_RAFT_RESPONSE, "")
    }
}

// 选举比分对比
// 注意：这里除了比分选举，还需要判断数据一致性的对比
func (n *Node) onMsgRaftScoreCompareRequest(conn net.Conn, msg *Msg) {
    j := gjson.DecodeToJson(msg.Body)
    result := gMSG_RAFT_SCORE_COMPARE_SUCCESS
    if n.getRaftRole() == gROLE_RAFT_LEADER && n.getLastLogId() >= msg.Info.LastLogId {
        result = gMSG_RAFT_I_AM_LEADER
    } else {
        if n.compareLeaderWithRemoteNodeByDetail(msg.Info.LastLogId, int32(j.GetInt("count")), j.GetInt64("score")) {
            result = gMSG_RAFT_SCORE_COMPARE_FAILURE
        } else {
            // 只是更新选举超时时间，最终leader的确定靠首次leader心跳
            n.updateElectionDeadline()
        }
    }
    n.sendMsg(conn, result, "")
}

// 两个leader进行比较
func (n *Node) onMsgRaftLeaderCompareRequest(conn net.Conn, msg *Msg) {
    j      := gjson.DecodeToJson(msg.Body)
    result := gMSG_RAFT_LEADER_COMPARE_SUCCESS
    if n.compareLeaderWithRemoteNodeByDetail(msg.Info.LastLogId, int32(j.GetInt("count")), j.GetInt64("score")) {
        result = gMSG_RAFT_LEADER_COMPARE_FAILURE
    }
    n.sendMsg(conn, result, "")
}


