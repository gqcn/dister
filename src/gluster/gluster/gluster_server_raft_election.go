package gluster

import (
    "sync"
    "time"
    "g/util/gtime"
    "g/os/glog"
)

// 服务器节点选举
// 改进：
// 3个节点以内的集群也可以完成leader选举
func (n *Node) electionHandler() {
    for {
        if n.Role == gROLE_SERVER && n.getRaftRole() != gROLE_RAFT_LEADER && gtime.Millisecond() >= n.getElectionDeadline() {
            // 使用MinNode变量控制最小节点数(这里判断的时候要去除自身的数量)
            if n.Peers.Size() >= int(n.MinNode - 1) {
                if n.Peers.Size() > 0 {
                    // 集群是2个节点及以上
                    n.resetAsCandidate()
                    n.beginScore()
                } else {
                    // 集群目前仅有1个节点
                    //glog.Println("only one node in this cluster, i'll be the leader")
                    n.setLeader(n.getNodeInfo())
                    n.setRaftRole(gROLE_RAFT_LEADER)
                }

            } else {
                //glog.Println("no meet the least nodes count:", n.MinNode, ", current:", n.Peers.Size() + 1)
            }
            n.updateElectionDeadline()
        }
        time.Sleep(100 * time.Millisecond)
    }
}

// 改进的RAFT选举
func (n *Node) beginScore() {
    var wg sync.WaitGroup
    //glog.Println("new election...")
    // 请求比分，获取比分数据
    for _, v := range n.Peers.Values() {
        info := v.(NodeInfo)
        if info.Status != gSTATUS_ALIVE || n.getId() == info.Id {
            continue
        }
        wg.Add(1)
        go func(info *NodeInfo) {
            defer wg.Done()
            if n.checkFailedTheElection() {
                return
            }
            stime := time.Now().UnixNano()
            conn  := n.getConn(info.Ip, gPORT_RAFT)
            if conn == nil {
                n.updatePeerStatus(info.Id, gSTATUS_DEAD)
                return
            }
            defer conn.Close()
            // 如果是本地同一节点通信，那么移除掉
            if n.checkConnInLocalNode(conn) {
                n.Peers.Remove(info.Id)
                return
            }
            if err := n.sendMsg(conn, gMSG_RAFT_SCORE_REQUEST, ""); err != nil {
                glog.Error(err)
                return
            }
            msg := n.receiveMsg(conn)
            if msg != nil {
                if n.checkFailedTheElection() {
                    return
                }
                switch msg.Head {
                    case gMSG_RAFT_I_AM_LEADER:
                        n.setLeader(&msg.Info)
                        n.setRaftRole(gROLE_RAFT_FOLLOWER)

                    case gMSG_RAFT_RESPONSE:
                        etime := time.Now().UnixNano()
                        score := etime - stime
                        n.addScore(score)
                        n.addScoreCount()
                }
            } else {
                n.updatePeerStatus(info.Id, gSTATUS_DEAD)
            }
        }(&info)
    }
    wg.Wait()

    // 必需要获得多数派(n/2+1)比分（以保证能够连通绝大部分的节点）才能满足leader的基础条件
    // 注意这里的ScoreCount和n.Peers.Size都不包含自身
    scoreCount := n.getScoreCount() + 1
    leastCount := int((n.Peers.Size() + 1)/2) + 1
    if scoreCount < int32(leastCount) {
        n.updateElectionDeadline()
        //glog.Printf("election failed: could not reach major of the nodes, score count:%d, group size:%d\n", scoreCount, n.Peers.Size() + 1)
        return
    }

    if n.checkFailedTheElection() {
        return
    }

    // 执行比分，对比比分数据，选举出leader
    for _, v := range n.Peers.Values() {
        info := v.(NodeInfo)
        if info.Status != gSTATUS_ALIVE {
            continue
        }
        wg.Add(1)
        go func(info *NodeInfo) {
            defer wg.Done()
            if n.checkFailedTheElection() {
                return
            }
            conn := n.getConn(info.Ip, gPORT_RAFT)
            if conn == nil {
                n.updatePeerStatus(info.Id, gSTATUS_DEAD)
                return
            }
            defer conn.Close()
            if err := n.sendMsg(conn, gMSG_RAFT_SCORE_COMPARE_REQUEST, ""); err != nil {
                glog.Error(err)
                return
            }
            msg := n.receiveMsg(conn)
            if msg != nil {
                if n.checkFailedTheElection() {
                    return
                }
                switch msg.Head {
                    // 对比过程中发现leader，那么设置leader
                    case gMSG_RAFT_I_AM_LEADER:
                        glog.Println("score comparison: get leader from", msg.Info.Name)
                        n.setLeader(&msg.Info)
                        n.setRaftRole(gROLE_RAFT_FOLLOWER)
                        n.updateElectionDeadline()

                    // 我比你更有资格当leader，别烦大人干正事，你自己先一边玩去
                    case gMSG_RAFT_SCORE_COMPARE_FAILURE:
                        glog.Println("score comparison: get failure from", msg.Info.Name)
                        n.setRaftRole(gROLE_RAFT_FOLLOWER) // 别烦大人干正事
                        n.updateElectionDeadline()         // 自己先一边玩去

                    // 对比成功，向leader的路又更迈进一步
                    case gMSG_RAFT_SCORE_COMPARE_SUCCESS:
                        glog.Println("score comparison: get success from", msg.Info.Name)
                }
            }
        }(&info)
    }
    wg.Wait()

    // 判断是否选举失败
    if !n.checkFailedTheElection() {
        //glog.Println("won the score comparison, become the leader")
        n.setLeader(n.getNodeInfo())
        n.setRaftRole(gROLE_RAFT_LEADER)
    }
}

// 在选举流程中时刻调用该方法来检查是否选举失败，以便进一步做退出选举处理
func (n *Node) checkFailedTheElection() bool {
    // 如果在计算比分的过程中发现了leader，那么不再继续比分，退出选举
    if n.getLeader() != nil {
        //glog.Println("failed the election, set", n.getLeader().Name, "as leader")
        return true
    }
    // 如果选举过程中状态变化，那么自身选举失败
    if n.getRaftRole() != gROLE_RAFT_CANDIDATE {
        return true
    }
    return false
}

