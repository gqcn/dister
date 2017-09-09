package gluster

import (
    "time"
    "g/net/gip"
    "g/net/gtcp"
    "net"
    "fmt"
    "encoding/json"
    "g/util/gtime"
    "g/core/types/gmap"
    "g/os/gfile"
    "g/net/ghttp"
    "g/os/gconsole"
    "g/encoding/gjson"
    "g/os/glog"
    "strings"
    "os"
    "errors"
    "g/util/grand"
)

// 获取数据
func (n *Node) receive(conn net.Conn) []byte {
    return Receive(conn)
}

// 获取Msg
func (n *Node) receiveMsg(conn net.Conn) *Msg {
    return RecieveMsg(conn)
}

// 发送数据
func (n *Node) send(conn net.Conn, data []byte) error {
    return Send(conn, data)
}

// 发送Msg
func (n *Node) sendMsg(conn net.Conn, head int, body string) error {
    ip, _  := gip.ParseAddress(conn.LocalAddr().String())
    info   := n.getNodeInfo()
    info.Ip = ip
    s, err := json.Marshal(Msg { head, body, *info })
    if err != nil {
        glog.Error("send msg parse err:", err)
        return err
    }
    return n.send(conn, s)
}

// 获得TCP链接
func (n *Node) getConn(ip string, port int) net.Conn {
    conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), 3 * time.Second)
    if err == nil {
        return conn
    }
    return nil
}

// 运行节点
func (n *Node) Run() {
    // 命令行操作
    if gconsole.Value.Get(1) != "" {
        gconsole.AutoRun()
        os.Exit(0)
        return
    }

    // 读取配置文件
    n.initFromCfg()

    // 读取命令行参数
    n.initFromCommand()

    // 初始化节点数据
    n.restoreDataFromFile()

    // 显示当前节点信息
    fmt.Printf( "gluster version %s, start running...\n", gVERSION)
    fmt.Println("===============================================================================")
    fmt.Println("Host Id       :", n.Id)
    fmt.Println("Host Role     :", roleName(n.Role))
    fmt.Println("Host Name     :", n.Name)
    fmt.Println("Host Group    :", n.Group)
    fmt.Println("Host LogPath  :", glog.GetLogPath())
    fmt.Println("Host SavePath :", n.SavePath)
    fmt.Println("Group MinNode :", n.MinNode)
    fmt.Println("===============================================================================")

    // 创建接口监听
    go gtcp.NewServer(fmt.Sprintf(":%d", gPORT_RAFT),  n.raftTcpHandler).Run()
    go gtcp.NewServer(fmt.Sprintf(":%d", gPORT_REPL),  n.replTcpHandler).Run()
    go func() {
        api := ghttp.NewServerByAddr(fmt.Sprintf(":%d", gPORT_API))
        api.BindController("/kv",      &NodeApiKv{node: n})
        api.BindController("/node",    &NodeApiNode{node: n})
        api.BindController("/service", &NodeApiService{node: n})
        api.BindController("/balance", &NodeApiBalance{node: n})
        api.Run()
    }()

    // 通知上线（这里采用局域网扫描的方式进行广播通知）
    //go n.sayHiToAll()
    //time.Sleep(2 * time.Second)
    // 配置同步
    go n.replicateConfigToLeader()
    // 选举超时检查
    go n.electionHandler()
    // 心跳保持及存活性检查
    go n.heartbeatHandler()
    // 日志同步处理
    go n.replicationHandler()
    // 本地节点数据自动存储处理
    go n.autoSavingHandler()
    // 服务健康检查
    go n.serviceHealthCheckHandler()
}

// 从命令行读取配置文件内容
func (n *Node) initFromCommand() {
    // 节点名称
    name := gconsole.Option.Get("Name")
    if name != "" {
        n.setName(name)
    }
    // 集群名称
    group := gconsole.Option.Get("Group")
    if group != "" {
        n.setGroup(group)
    }
    // 集群角色
    role := gconsole.Option.GetInt("Role")
    if role < 0 || role > 2 {
        glog.Fatalln("invalid role setting, exit")
    } else {
        n.setRole(role)
    }
    // 数据保存路径(请保证运行gcluster的用户有权限写入)
    savepath := gconsole.Option.Get("SavePath")
    if savepath != "" {
        n.setSavePathFromConfig(savepath)
    }
    // 日志保存路径
    logpath := gconsole.Option.Get("LogPath")
    if logpath != "" {
        n.setLogPathFromConfig(logpath)
    }
    // (可选)节点地址IP或者域名
    ip := gconsole.Option.Get("Ip")
    if ip != "" {
        n.setIp(ip)
    }
    // (可选)节点地址IP或者域名
    scan := gconsole.Option.GetBool("Scan")
    if scan {
        n.sayHiToLocalLan()
    }
    // (可选)节点地址IP或者域名
    minNode := gconsole.Option.GetInt("MinNode")
    if minNode != 0 {
        n.setMinNode(minNode)
    }
    // (可选)初始化节点列表，包含自定义的所需添加的服务器IP或者域名列表
    peerstr := gconsole.Option.Get("Peers")
    if peerstr != "" {
        peers := strings.Split(strings.TrimSpace(peerstr), ",")
        n.setPeersFromConfig(peers)
    }
}

// 从配置文件读取配置文件内容
func (n *Node) initFromCfg() {
    // 获取命令行指定的配置文件路径，如果不存在，那么使用默认路径的配置文件
    // 默认路径为gcluster执行文件的同一目录下的gluster.json文件
    cfgpath := gconsole.Option.Get("cfg")
    if cfgpath == "" {
        cfgpath = gfile.SelfDir() + gfile.Separator + "gluster.json"
    } else {
        if !gfile.Exists(cfgpath) {
            glog.Error(cfgpath, "does not exist")
            return
        }
    }
    if !gfile.Exists(cfgpath) {
        return
    }
    n.CfgFilePath = cfgpath

    j := gjson.DecodeToJson(string(gfile.GetContents(cfgpath)))
    if j == nil {
        glog.Fatalln("config file decoding failed(surely a json format?), exit")
    }
    glog.Println("initializing from", cfgpath)
    // 节点名称
    name := j.GetString("Name")
    if name != "" {
        n.setName(name)
    }
    // 集群名称
    group := j.GetString("Group")
    if group != "" {
        n.setGroup(group)
    }
    // 集群角色
    role := j.GetInt("Role")
    if role < 0 || role > 2 {
        glog.Fatalln("invalid role setting, exit")
    } else {
        n.setRole(role)
    }
    // 数据保存路径(请保证运行gcluster的用户有权限写入)
    savepath := j.GetString("SavePath")
    if savepath != "" {
        n.setSavePathFromConfig(savepath)
    }
    // 日志保存路径
    logpath := j.GetString("LogPath")
    if logpath != "" {
        n.setLogPathFromConfig(logpath)
    }
    // (可选)节点地址IP或者域名
    ip := j.GetString("Ip")
    if ip != "" {
        n.setIp(ip)
    }
    // (可选)本地局域网搜索
    scan := j.GetBool("Scan")
    if scan {
        n.sayHiToLocalLan()
    }
    // (可选)节点地址IP或者域名
    minNode := j.GetInt("MinNode")
    if minNode != 0 {
        n.setMinNode(minNode)
    }
    // (可选)初始化节点列表，包含自定义的所需添加的服务器IP或者域名列表
    params := j.GetArray("Peers")
    if params != nil {
        peers := make([]string, 0)
        for _, v := range params {
            peers = append(peers, v.(string))
        }
        n.setPeersFromConfig(peers)
    }
}

// 从配置中设置SavePath
func (n *Node) setSavePathFromConfig(savepath string) {
    if !gfile.Exists(savepath) {
        gfile.Mkdir(savepath)
    }
    if !gfile.IsWritable(savepath) {
        glog.Fatalln(savepath, "is not writable for saving data")
    }
    n.SetSavePath(strings.TrimRight(savepath, gfile.Separator))
}

// 从配置中设置LogPath
func (n *Node) setLogPathFromConfig(logpath string) {
    if !gfile.Exists(logpath) {
        gfile.Mkdir(logpath)
    }
    if !gfile.IsWritable(logpath) {
        glog.Fatalln(logpath, "is not writable for saving log")
    }
    glog.SetLogPath(logpath)
}

// 从配置中设置Peers
func (n *Node) setPeersFromConfig(peers []string) {
    ip := n.getIp()
    for _, v := range peers {
        if v == ip {
            continue
        }
        go func(ip string) {
            if !n.sayHi(ip) {
                n.updatePeerInfo(NodeInfo{Id: ip, Ip: ip})
            }
        }(v)
    }
}

// 将本地配置信息同步到leader
func (n *Node) replicateConfigToLeader() {
    for !n.CfgReplicated {
        if n.getRaftRole() != gROLE_RAFT_LEADER {
            if n.getLeader() != nil {
                if gfile.Exists(n.CfgFilePath) {
                    //glog.Println("replicate config to leader")
                    err := n.SendToLeader(gMSG_REPL_CONFIG_FROM_FOLLOWER, gPORT_REPL, gfile.GetContents(n.CfgFilePath))
                    if err == nil {
                        n.CfgReplicated = true
                        //glog.Println("replicate config to leader, done")
                    } else {
                        glog.Error(err)
                    }
                } else {
                    n.CfgReplicated = true
                }
            }
        } else {
            n.CfgReplicated = true
        }
        time.Sleep(100 * time.Millisecond)
    }
}

// 获取当前节点的信息
func (n *Node) getNodeInfo() *NodeInfo {
    return &NodeInfo {
        Group            : n.Group,
        Id               : n.Id,
        Ip               : n.Ip,
        Name             : n.Name,
        Status           : gSTATUS_ALIVE,
        Role             : n.Role,
        RaftRole         : n.getRaftRole(),
        Score            : n.getScore(),
        ScoreCount       : n.getScoreCount(),
        LastLogId        : n.getLastLogId(),
        LogCount         : n.getLogCount(),
        LastActiveTime   : gtime.Millisecond(),
        LastServiceLogId : n.getLastServiceLogId(),
        Version          : gVERSION,
    }
}

// 向leader发送操作请求
func (n *Node) SendToLeader(head int, port int, body string) error {
    leader := n.getLeader()
    if leader == nil {
        return errors.New(fmt.Sprintf("leader not found, please try again after leader election done, request head: %d", head))
    }
    conn := n.getConn(leader.Ip, port)
    if conn == nil {
        return errors.New("could not connect to leader: " + leader.Ip)
    }
    defer conn.Close()
    err := n.sendMsg(conn, head, body)
    if err != nil {
        return errors.New("sending request error: " + err.Error())
    } else {
        msg := n.receiveMsg(conn)
        if msg != nil && ((port == gPORT_RAFT && msg.Head != gMSG_RAFT_RESPONSE) || (port == gPORT_REPL && msg.Head != gMSG_REPL_RESPONSE)) {
            return errors.New("handling request error")
        }
    }
    return nil
}

// 通过IP向一个节点发送消息并建立双方联系
// HI只是建立联系，并不做其他处理，例如谁更适合作为leader判断是通过心跳流程来处理的
func (n *Node) sayHi(ip string) bool {
    if ip == n.Ip {
        return false
    }
    conn := n.getConn(ip, gPORT_RAFT)
    if conn == nil {
        return false
    }
    defer conn.Close()
    // 如果是本地同一节点通信，那么移除掉
    if n.checkConnInLocalNode(conn) {
        n.Peers.Remove(ip)
        return false
    }
    err := n.sendMsg(conn, gMSG_RAFT_HI, "")
    if err != nil {
        return false
    }
    msg := n.receiveMsg(conn)
    if msg != nil && msg.Head == gMSG_RAFT_HI2 {
        n.updatePeerInfo(msg.Info)
    }
    return true
}

// 向局域网内其他主机通知上线
func (n *Node) sayHiToLocalLan() {
    ip      := n.getIp()
    segment := gip.GetSegment(ip)
    if segment == "" || !gip.IsIntranet(ip){
        glog.Fatalln("invalid listening ip given")
        return
    }
    if segment == "127.0.0" {
        glog.Error("there're multiple ips in this host, bind one to make scan work, exit scanning")
        return
    }
    for i := 1; i < 256; i++ {
        go func(ip string) {
            if n.sayHi(ip) {
                glog.Println("successfully scan and add local ip:", ip)
            }
        }(fmt.Sprintf("%s.%d", segment, i))
    }
}

// 与远程节点对比谁可以成为leader，返回true表示自己，false表示对方节点
// 需要同时对比日志信息及选举比分
func (n *Node) compareLeaderWithRemoteNode(info *NodeInfo) bool {
    result := false
    if n.getLogCount() > info.LogCount && n.getLastLogId() > info.LastLogId {
        result = true
    } else if n.getLogCount() == info.LogCount && n.getLastLogId() == info.LastLogId {
        if n.getScoreCount() > info.ScoreCount {
            result = true
        } else if n.getScoreCount() == info.ScoreCount {
            if n.getScore() > info.Score {
                result = true
            } else if n.getScore() == info.Score {
                // 极少数情况, 这时采用随机策略
                if grand.Rand(0, 1) == 0 {
                    result = true
                }
            }
        }
    }
    return result
}

// 检查链接是否属于本地的一个链接(即：自己链接自己)
func (n *Node) checkConnInLocalNode(conn net.Conn) bool {
    localip,  _ := gip.ParseAddress(conn.LocalAddr().String())
    remoteip, _ := gip.ParseAddress(conn.RemoteAddr().String())
    return localip == remoteip
}

// 获得Peers节点信息(包含自身)
func (n *Node) getAllPeers() *[]NodeInfo{
    list := make([]NodeInfo, 0)
    list  = append(list, *n.getNodeInfo())
    for _, v := range n.Peers.Values() {
        list = append(list, v.(NodeInfo))
    }
    return &list
}

// 生成一个唯一的LogId，相对于leader节点来说，并且只有leader节点才能生成LogId
// 由于一个集群中只会存在一个leader，因此该LogId可以看做唯一性
func (n *Node) makeLogId() int64 {
    n.mutex.Lock()
    index := int64(n.LastLogId/10000)
    if n.LogIdIndex < index {
        n.LogIdIndex = index
    }
    n.LogIdIndex++
    // 后四位是随机数，保证出现冲突的概率很小
    r := n.LogIdIndex*10000 + int64(grand.Rand(0, 9999))
    n.mutex.Unlock()
    return r
}

func (n *Node) getId() string {
    n.mutex.RLock()
    r := n.Id
    n.mutex.RUnlock()
    return r
}

func (n *Node) getIp() string {
    n.mutex.RLock()
    r := n.Ip
    n.mutex.RUnlock()
    return r
}

func (n *Node) getName() string {
    n.mutex.RLock()
    r := n.Name
    n.mutex.RUnlock()
    return r
}

func (n *Node) getLeader() *NodeInfo {
    n.mutex.RLock()
    r := n.Leader
    n.mutex.RUnlock()
    return r
}

func (n *Node) getRaftRole() int {
    n.mutex.RLock()
    r := n.RaftRole
    n.mutex.RUnlock()
    return r
}

func (n *Node) getScore() int64 {
    n.mutex.RLock()
    r := n.Score
    n.mutex.RUnlock()
    return r
}

func (n *Node) getScoreCount() int {
    n.mutex.RLock()
    r := n.ScoreCount
    n.mutex.RUnlock()
    return r
}

func (n *Node) getLastLogId() int64 {
    n.mutex.RLock()
    r := n.LastLogId
    n.mutex.RUnlock()
    return r
}

func (n *Node) getMinNode() int {
    n.mutex.RLock()
    r := n.MinNode
    n.mutex.RUnlock()
    return r
}

func (n *Node) getLogCount() int {
    n.mutex.RLock()
    r := n.LogCount
    n.mutex.RUnlock()
    return r
}

func (n *Node) getLastServiceLogId() int64 {
    n.mutex.Lock()
    r := n.LastServiceLogId
    n.mutex.Unlock()
    return r
}

func (n *Node) getStatusInReplication() bool {
    n.mutex.RLock()
    r := n.isInDataReplication
    n.mutex.RUnlock()
    return r
}

func (n *Node) getElectionDeadline() int64 {
    n.mutex.RLock()
    r := n.ElectionDeadline
    n.mutex.RUnlock()
    return r
}

func (n *Node) getPeersFilePath() string {
    n.mutex.RLock()
    path := n.SavePath + gfile.Separator + "gluster.peers.db"
    n.mutex.RUnlock()
    return path
}

func (n *Node) getDataFilePath() string {
    n.mutex.RLock()
    path := n.SavePath + gfile.Separator + "gluster.data.db"
    n.mutex.RUnlock()
    return path
}

func (n *Node) getServiceFilePath() string {
    n.mutex.RLock()
    path := n.SavePath + gfile.Separator + "gluster.service.db"
    n.mutex.RUnlock()
    return path
}

func (n *Node) getDataDirty() bool {
    n.mutex.RLock()
    r := n.IsDataDirty
    n.mutex.RUnlock()
    return r
}

func (n *Node) getServiceDirty() bool {
    n.mutex.RLock()
    r := n.IsServiceDirty
    n.mutex.RUnlock()
    return r
}

// 添加比分节
func (n *Node) addScore(s int64) {
    n.mutex.Lock()
    n.Score += s
    n.mutex.Unlock()
}

// 添加比分节点数
func (n *Node) addScoreCount() {
    n.mutex.Lock()
    n.ScoreCount++
    n.mutex.Unlock()
}

// 添加日志总数
func (n *Node) addLogCount() {
    n.mutex.Lock()
    n.ScoreCount++
    n.mutex.Unlock()
}

// 重置为候选者，并初始化投票给自己
func (n *Node) resetAsCandidate() {
    n.mutex.Lock()
    n.RaftRole   = gROLE_RAFT_CANDIDATE
    n.Leader     = nil
    n.Score      = 0
    n.ScoreCount = 0
    n.mutex.Unlock()
}

func (n *Node) setName(name string) {
    n.mutex.Lock()
    n.Name = name
    n.mutex.Unlock()
}

func (n *Node) setGroup(group string) {
    n.mutex.Lock()
    n.Group = group
    n.mutex.Unlock()
}

func (n *Node) setIp(ip string) {
    n.mutex.Lock()
    n.Ip = ip
    n.mutex.Unlock()
}

func (n *Node) setMinNode(count int) {
    n.mutex.Lock()
    n.MinNode = count
    n.mutex.Unlock()
}

func (n *Node) setRole(role int) {
    n.mutex.Lock()
    n.Role = role
    n.mutex.Unlock()

}

func (n *Node) setDataDirty(b bool) {
    n.mutex.Lock()
    n.IsDataDirty = b
    n.mutex.Unlock()
}

func (n *Node) setServiceDirty(b bool) {
    n.mutex.Lock()
    n.IsServiceDirty = b
    n.mutex.Unlock()
}

func (n *Node) setRaftRole(role int) {
    n.mutex.Lock()
    if n.RaftRole != role {
        glog.Printf("role changed from %s to %s\n", raftRoleName(n.RaftRole), raftRoleName(role))
    }
    n.RaftRole = role
    n.mutex.Unlock()

}

func (n *Node) setLeader(info *NodeInfo) {
    n.mutex.Lock()
    if n.Leader != nil {
        glog.Printf("leader changed from %s to %s\n", n.Leader.Name, info.Name)
    } else {
        glog.Println("set leader:", info.Name)
    }
    n.Leader = info
    n.mutex.Unlock()

}

// 设置数据保存目录路径
func (n *Node) SetSavePath(path string) {
    n.mutex.Lock()
    n.SavePath = path
    n.mutex.Unlock()
}

func (n *Node) setLastLogId(id int64) {
    n.mutex.Lock()
    n.LastLogId = id
    n.mutex.Unlock()
}

func (n *Node) setLogCount(count int) {
    n.mutex.Lock()
    n.LogCount = count
    n.mutex.Unlock()
}

func (n *Node) setLastServiceLogId(id int64) {
    n.mutex.Lock()
    n.LastServiceLogId = id
    n.mutex.Unlock()
}

func (n *Node) setStatusInReplication(status bool ) {
    n.mutex.Lock()
    n.isInDataReplication = status
    n.mutex.Unlock()
}

func (n *Node) setService(m *gmap.StringInterfaceMap) {
    if m == nil {
        return
    }
    n.mutex.Lock()
    n.Service = m
    n.mutex.Unlock()
}

func (n *Node) setServiceForApi(m *gmap.StringInterfaceMap) {
    if m == nil {
        return
    }
    n.mutex.Lock()
    n.ServiceForApi = m
    n.mutex.Unlock()
}

func (n *Node) setPeers(m *gmap.StringInterfaceMap) {
    if m == nil {
        return
    }
    n.mutex.Lock()
    n.Peers = m
    n.mutex.Unlock()
}

func (n *Node) setDataMap(m *gmap.StringStringMap) {
    if m == nil {
        return
    }
    n.mutex.Lock()
    n.DataMap = m
    n.mutex.Unlock()
}

// 更新节点信息
func (n *Node) updatePeerInfo(info NodeInfo) {
    n.Peers.Set(info.Id, info)
    // 去掉初始化时写入的IP键名记录
    if n.Peers.Contains(info.Ip) {
        if info.Id != info.Ip {
            n.Peers.Remove(info.Ip)
        }
    }
}

func (n *Node) updatePeerStatus(Id string, status int) {
    r := n.Peers.Get(Id)
    if r != nil {
        info       := r.(NodeInfo)
        info.Status = status
        if info.LastActiveTime == 0 || status == gSTATUS_ALIVE {
            info.LastActiveTime = gtime.Millisecond()
        }
        n.updatePeerInfo(info)
    }
}

// 更新选举截止时间
// 改进：固定时间进行比分，看谁的比分更多
func (n *Node) updateElectionDeadline() {
    n.mutex.Lock()
    n.ElectionDeadline = gtime.Millisecond() + gELECTION_TIMEOUT
    n.mutex.Unlock()
}



