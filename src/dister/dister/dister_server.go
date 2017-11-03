package dister

import (
    "time"
    "g/net/gip"
    "g/net/gtcp"
    "net"
    "fmt"
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
    "sync/atomic"
    "g/encoding/gbinary"
    "strconv"
    "g/database/gkvdb"
)

// 获取Msg，使用默认的超时时间
func (n *Node) receiveMsg(conn net.Conn) *Msg {
    return n.receiveMsgWithTimeout(conn, gTCP_READ_TIMEOUT * time.Millisecond)
}

// 获取Msg，自定义超时时间
func (n *Node) receiveMsgWithTimeout(conn net.Conn, timeout time.Duration) *Msg {
    conn.SetReadDeadline(time.Now().Add(timeout))
    return n.doRecieveMsg(conn)
}

// 获取Msg，不超时
func (n *Node) receiveMsgWithoutTimeout(conn net.Conn, timeout time.Duration) *Msg {
    return n.doRecieveMsg(conn)
}

// 获取Msg
func (n *Node) doRecieveMsg(conn net.Conn) *Msg {
    data := Receive(conn)
    if data != nil && len(data) > 0 {
        msg := n.decodeMsg(data)
        if msg.Info.Ip == "127.0.0.1" || msg.Info.Ip == "" {
            ip, _      := gip.ParseAddress(conn.RemoteAddr().String())
            msg.Info.Ip = ip
        }
        // 保存节点信息
        if msg.Info.Id != n.Id {
            n.updatePeerInfo(msg.Info)
        }
        return msg
    }
    return nil
}

// 发送Msg
func (n *Node) sendMsg(conn net.Conn, head int, body string) error {
    ip, _  := gip.ParseAddress(conn.LocalAddr().String())
    info   := n.getNodeInfo()
    info.Ip = ip
    s, _ := n.encodeMsg(head, body, info)
    return Send(conn, s)
}

// 对Msg进行二进制打包
func (n *Node) encodeMsg(head int, body string, info *NodeInfo) ([]byte, error) {
    c       := []byte(body)
    b1, err := gbinary.Encode(int32(head), int32(len(c)), c)
    if err != nil {
        glog.Error(err)
        return nil, err
    }
    nameBytes  := []byte(info.Name)
    groupBytes := []byte(info.Group)
    id, _      := strconv.ParseUint(info.Id, 16, 32)
    iplong     := gip.Ip2long(info.Ip)
    b2, err    := gbinary.Encode(
        int32(len(nameBytes)),  nameBytes,
        int32(len(groupBytes)), groupBytes,
        uint32(id), iplong, info.Role, info.RaftRole,
        info.LastLogId, info.LastServiceLogId,
        []byte(info.Version),
    )
    if err != nil {
        glog.Error(err)
        return nil, err
    }
    return append(b1, b2...), nil
}

// 对Msg进行二进制解包
func (n *Node) decodeMsg(b []byte) *Msg {
    head          := gbinary.DecodeToInt32 (b)
    bodySize      := gbinary.DecodeToInt32 (b[4:])
    bodyBytes     := b[8 : bodySize]
    nameSize      := gbinary.DecodeToInt32 (b[8 + bodySize:])
    nameBytes     := b[8 + bodySize + 4 : nameSize]
    groupSize     := gbinary.DecodeToInt32 (b[8 + bodySize + 4 + nameSize:])
    groupBytes    := b[8 + bodySize + 4 + nameSize + 4 : groupSize]
    id            := gbinary.DecodeToUint32(b[8 + bodySize + 4 + nameSize + 4 + groupSize:])
    iplong        := gbinary.DecodeToUint32(b[8 + bodySize + 4 + nameSize + 4 + groupSize + 4:])
    role          := gbinary.DecodeToInt32 (b[8 + bodySize + 4 + nameSize + 4 + groupSize + 8:])
    raft          := gbinary.DecodeToInt32 (b[8 + bodySize + 4 + nameSize + 4 + groupSize + 12:])
    logid         := gbinary.DecodeToInt64 (b[8 + bodySize + 4 + nameSize + 4 + groupSize + 16:])
    sid           := gbinary.DecodeToInt64 (b[8 + bodySize + 4 + nameSize + 4 + groupSize + 24:])
    version       := b[8 + bodySize + 4 + nameSize + 4 + groupSize + 32:]
    return &Msg {
        Head: int(head),
        Body: string(bodyBytes),
        Info: NodeInfo{
            Name             : string(nameBytes),
            Group            : string(groupBytes),
            Id               : strings.ToUpper(fmt.Sprintf("%x", id)),
            Ip               : gip.Long2ip(iplong),
            Status           : gSTATUS_ALIVE,
            Role             : role,
            RaftRole         : raft,
            LastLogId        : logid,
            LastServiceLogId : sid,
            Version          : string(version),
        },
    }
}

// 向指定节点发送并接收消息
func (n *Node) sendAndReceiveMsgToNode(info *NodeInfo, port int, head int, body string) (*Msg, error) {
    conn := n.getConn(info.Ip, port)
    if conn != nil {
        defer conn.Close()
        if n.checkConnInLocalNode(conn) {
            n.Peers.Remove(info.Id)
            return nil, errors.New("cannot make local connection")
        }
        err := n.sendMsg(conn, head, body)
        if err == nil {
            msg := n.receiveMsg(conn)
            if msg != nil {
                return msg, nil
            } else {
                return nil, errors.New(fmt.Sprintf("receive msg error from node: %s, sent msg head: %d", info.Ip, head))
            }
        } else {
            return nil, err
        }
    } else {
        return nil, errors.New(fmt.Sprintf("cannot connect to node: %s", info.Ip))
    }
    return nil, errors.New("error")
}

// 获得TCP链接
func (n *Node) getConn(ip string, port int) net.Conn {
    conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), 3 * time.Second)
    if err == nil {
        return conn
    }
    return nil
}

// 初始化本地KV数据库
func (n *Node) initLocalDb() {
    // 初始化DataMap数据库
    db, err := gkvdb.New(n.getDataFilePath())
    if err != nil {
        glog.Fatalln(err)
    } else {
        n.DataMap = db
    }
    // 初始化Service数据库
    db, err  = gkvdb.New(n.getServiceFilePath())
    if err != nil {
        glog.Fatalln(err)
    } else {
        n.Service = db
    }
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

    // 初始化本地数据库
    n.initLocalDb()

    // 初始化节点数据
    n.restoreFromFile()

    // 显示当前节点信息
    logpathstr := glog.GetLogPath()
    if logpathstr == "" {
        logpathstr = "STDOUT"
    }
    fmt.Printf( "dister version %s, start running...\n", gVERSION)
    fmt.Println("==================================================================================")
    fmt.Println("Host Id         :", n.Id)
    fmt.Println("Host Role       :", roleName(n.Role))
    fmt.Println("Host Name       :", n.Name)
    fmt.Println("Host Group      :", n.Group)
    fmt.Println("Host LogPath    :", logpathstr)
    fmt.Println("Host SavePath   :", n.getSavePath())
    fmt.Println("Host MinNode    :", n.MinNode)
    fmt.Println("Last Log Id     :", n.getLastLogId())
    fmt.Println("Last Service Id :", n.getLastServiceLogId())
    fmt.Println("==================================================================================")

    // 创建接口监听
    go gtcp.NewServer(fmt.Sprintf(":%d", gPORT_RAFT),  n.raftTcpHandler).Run()
    go gtcp.NewServer(fmt.Sprintf(":%d", gPORT_REPL),  n.replTcpHandler).Run()
    go func() {
        // API只能本地访问
        api := ghttp.NewServerByAddr(fmt.Sprintf("127.0.0.1:%d", gPORT_API))
        api.BindController("/kv",      &NodeApiKv{node: n})
        api.BindController("/node",    &NodeApiNode{node: n})
        api.BindController("/service", &NodeApiService{node: n})
        api.BindController("/balance", &NodeApiBalance{node: n})
        api.Run()
    }()

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

    // 所有线程启动完成后，局域网自动扫描
    if n.AutoScan {
        n.sayHiToLocalLan()
    }

    // 其他一些处理
    glog.SetDebug(gDEBUG)
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
        n.setRole(int32(role))
    }
    // 数据保存路径(请保证运行gcluster的用户有权限写入)
    savepath := gconsole.Option.Get("SavePath")
    if savepath == "" {
        savepath = n.getSavePath()
    }
    n.setSavePathFromConfig(savepath)
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
    if gconsole.Option.Get("Scan") != "" {
        n.AutoScan = gconsole.Option.GetBool("Scan")
    }

    // (可选)节点地址IP或者域名
    minNode := gconsole.Option.GetInt("MinNode")
    if minNode != 0 {
        n.setMinNode(int32(minNode))
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
    // 默认路径为gcluster执行文件的同一目录下的dister.json文件
    cfgpath := gconsole.Option.Get("cfg")
    if cfgpath == "" {
        cfgpath = gfile.SelfDir() + gfile.Separator + "dister.json"
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
    //glog.Println("initializing from", cfgpath)
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
        n.setRole(int32(role))
    }
    // 数据保存路径(请保证运行gcluster的用户有权限写入)
    savepath := j.GetString("SavePath")
    if savepath != "" {
        n.SetSavePath(savepath)
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
    // (可选)本地局域网搜索，默认情况下是开启，当有设置的时候使用设置值
    if j.Get("Scan") != nil {
        n.AutoScan = j.GetBool("Scan")
    }
    // (可选)节点地址IP或者域名
    minNode := j.GetInt("MinNode")
    if minNode != 0 {
        n.setMinNode(int32(minNode))
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
    dbpath := strings.TrimRight(savepath, gfile.Separator) + gfile.Separator + "dister.db"
    if !gfile.Exists(dbpath) {
        gfile.Mkdir(dbpath)
    }
    n.SetSavePath(dbpath)
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
        go n.sayHi(v)
    }
}

// 将本地配置信息同步到leader
func (n *Node) replicateConfigToLeader() {
    for !n.CfgReplicated {
        if n.getRaftRole() != gROLE_RAFT_LEADER {
            if n.getLeader() != nil {
                if gfile.Exists(n.CfgFilePath) {
                    //glog.Println("replicate config to leader")
                    _, err := n.SendToLeader(gMSG_REPL_CONFIG_FROM_FOLLOWER, gPORT_REPL, gfile.GetContents(n.CfgFilePath))
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
        LastLogId        : n.getLastLogId(),
        LastServiceLogId : n.getLastServiceLogId(),
        Version          : gVERSION,
    }
}

// 向leader发送操作请求，并返回执行结果
func (n *Node) SendToLeader(head int, port int, body string) (string, error) {
    leader := n.getLeader()
    if leader == nil {
        return "", errors.New(fmt.Sprintf("leader not found, please try again after leader election done, request head: %d", head))
    }
    conn := n.getConn(leader.Ip, port)
    if conn == nil {
        return "", errors.New("could not connect to leader: " + leader.Ip)
    }
    defer conn.Close()
    err := n.sendMsg(conn, head, body)
    if err != nil {
        return "", errors.New("sending request error: " + err.Error())
    } else {
        msg := n.receiveMsg(conn)
        if msg != nil && ((port == gPORT_RAFT && msg.Head != gMSG_RAFT_RESPONSE) || (port == gPORT_REPL && msg.Head != gMSG_REPL_RESPONSE)) {
            return "", errors.New(fmt.Sprintf("handling request error, response code: %d", msg.Head))
        } else {
            return msg.Body, nil
        }
    }
    return "", errors.New("unknown error")
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
    } else {
        n.receiveMsg(conn)
    }
    return true
}

// 向局域网内其他主机通知上线
func (n *Node) sayHiToLocalLan() {
    ips, err := gip.IntranetIP()
    if err != nil {
        glog.Error(err)
        return
    }
    for _, v := range ips {
        segment := gip.GetSegment(v)
        for i := 1; i < 256; i++ {
            go func(ip string) {
                if n.sayHi(ip) {
                    glog.Println("successfully scan and add local ip:", ip)
                }
            }(fmt.Sprintf("%s.%d", segment, i))
        }
    }
}

// 与远程节点对比谁可以成为leader，返回true表示自己，false表示对方节点
// 需要同时对比日志信息及选举比分
func (n *Node) compareLeaderWithRemoteNode(info *NodeInfo) bool {
    // 优先比较数据新旧程度
    if n.getLastLogId() > info.LastLogId {
        return true
    } else if n.getLastLogId() < info.LastLogId {
        return false
    }
    // 如果数据一致，那么比较选举比分
    result      := true
    compareData := gjson.Encode(map[string]interface{}{
        "score": n.getScore(),
        "count": n.getScoreCount(),
    })
    msg, err := n.sendAndReceiveMsgToNode(info, gPORT_RAFT, gMSG_RAFT_LEADER_COMPARE_REQUEST, compareData)
    if err != nil || (msg != nil && msg.Head == gMSG_RAFT_LEADER_COMPARE_FAILURE) {
        result = false
    }
    return result
}

// 使用具体对比信息进行对比
func (n *Node) compareLeaderWithRemoteNodeByDetail(logid int64, count int32, score int64) bool {
    result := false
    if n.getLastLogId() > logid {
        result = true
    } else if n.getLastLogId() == logid {
        if n.getScoreCount() > count {
            result = true
        } else if n.getScoreCount() == count {
            if n.getScore() > score {
                result = true
            } else if n.getScore() == score {
                // 极少数情况, 这时避让策略
                // 同样条件，最后请求的被选举为leader
                result = true
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
    index := int64(n.LastLogId/gLOGENTRY_RANDOM_ID_SIZE)
    if n.LogIdIndex < index {
        n.LogIdIndex = index
    }
    n.LogIdIndex++
    // 后四位是随机数，保证出现冲突的概率很小
    r := n.LogIdIndex*gLOGENTRY_RANDOM_ID_SIZE + int64(grand.Rand(0, gLOGENTRY_RANDOM_ID_SIZE - 1))
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

func (n *Node) getSavePath() string {
    n.mutex.RLock()
    r := n.SavePath
    n.mutex.RUnlock()
    return r
}

func (n *Node) getLeader() *NodeInfo {
    n.mutex.RLock()
    r := n.Leader
    n.mutex.RUnlock()
    return r
}

func (n *Node) getRole() int32 {
    return atomic.LoadInt32(&n.Role)
}

func (n *Node) getRaftRole() int32 {
    return atomic.LoadInt32(&n.RaftRole)
}

func (n *Node) getScore() int64 {
    return atomic.LoadInt64(&n.Score)
}

func (n *Node) getScoreCount() int32 {
    return atomic.LoadInt32(&n.ScoreCount)
}

func (n *Node) getLastLogId() int64 {
    return atomic.LoadInt64(&n.LastLogId)
}

func (n *Node) getMinNode() int32 {
    return atomic.LoadInt32(&n.MinNode)
}

func (n *Node) getLastServiceLogId() int64 {
    return atomic.LoadInt64(&n.LastServiceLogId)
}

func (n *Node) getElectionDeadline() int64 {
    return atomic.LoadInt64(&n.ElectionDeadline)
}

func (n *Node) getPeersFilePath() string {
    n.mutex.RLock()
    path := n.SavePath + gfile.Separator + "dister.peers.db"
    n.mutex.RUnlock()
    return path
}

func (n *Node) getDataFilePath() string {
    n.mutex.RLock()
    path := n.SavePath + gfile.Separator + "dister.data.db"
    n.mutex.RUnlock()
    return path
}

func (n *Node) getServiceFilePath() string {
    n.mutex.RLock()
    path := n.SavePath + gfile.Separator + "dister.service.db"
    n.mutex.RUnlock()
    return path
}

// 根据logid计算数据存储的文件绝对路径，每个数据日志文件最大存储 gLOGENTRY_FILE_SIZE 条记录
func (n *Node) getLogEntryFileSavePathById(id int64) string {
    return n.getLogEntryFileSavePathByBatchNo(n.getLogEntryBatachNo(id))
}

// 根据批次号获取日志文件存储绝对路径
func (n *Node) getLogEntryFileSavePathByBatchNo(no int64) string {
    n.mutex.RLock()
    path := n.SavePath + gfile.Separator + fmt.Sprintf("dister.entry.log/%d/%d", int(no/100), no)
    n.mutex.RUnlock()
    return path
}

// 获得logid存储的批次编号，用于文件存储的分组
func (n *Node) getLogEntryBatachNo(id int64) int64 {
    return int64(id/gLOGENTRY_RANDOM_ID_SIZE/gLOGENTRY_FILE_SIZE)
}

// 添加比分节
func (n *Node) addScore(s int64) {
    atomic.AddInt64(&n.Score, s)
}

// 添加比分节点数
func (n *Node) addScoreCount() {
    atomic.AddInt32(&n.ScoreCount, 1)
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

func (n *Node) setMinNode(count int32) {
    atomic.StoreInt32(&n.MinNode, count)
}

func (n *Node) setRole(role int32) {
    atomic.StoreInt32(&n.Role, role)
}

func (n *Node) setRaftRole(role int32) {
    r := n.getRaftRole()
    if r != role {
        glog.Printfln("role changed from %s to %s", raftRoleName(r), raftRoleName(role))
    }
    atomic.StoreInt32(&n.RaftRole, role)
}

func (n *Node) setLeader(info *NodeInfo) {
    n.mutex.Lock()
    defer n.mutex.Unlock()
    if n.Leader != nil {
        if n.Leader.Id == info.Id {
            return
        } else {
            glog.Printf("leader changed from %s to %s\n", n.Leader.Name, info.Name)
        }
    } else {
        glog.Println("set leader:", info.Name)
    }
    n.Leader = info
}

// 设置数据保存目录路径
func (n *Node) SetSavePath(path string) {
    n.mutex.Lock()
    n.SavePath = path
    n.mutex.Unlock()
}

func (n *Node) setLastLogId(id int64) {
    atomic.StoreInt64(&n.LastLogId, id)
}

func (n *Node) setLastServiceLogId(id int64) {
    atomic.StoreInt64(&n.LastServiceLogId, id)
}

func (n *Node) setPeers(m *gmap.StringInterfaceMap) {
    if m == nil {
        return
    }
    n.mutex.Lock()
    n.Peers = m
    n.mutex.Unlock()
}

// 更新节点信息
func (n *Node) updatePeerInfo(info NodeInfo) {
    n.Peers.Set(info.Id, info)
    // leader更新判断
    leader := n.getLeader()
    if leader != nil && leader.Id == info.Id {
        n.setLeader(&info)
    }
    // 去掉初始化时写入的IP键名记录
    if n.Peers.Contains(info.Ip) {
        if info.Id != info.Ip {
            n.Peers.Remove(info.Ip)
        }
    }
}

func (n *Node) updatePeerStatus(Id string, status int32) {
    r := n.Peers.Get(Id)
    if r != nil {
        info       := r.(NodeInfo)
        info.Status = status
        n.updatePeerInfo(info)
    }
}

// 更新选举截止时间
// 改进：固定时间进行比分，看谁的比分更多
func (n *Node) updateElectionDeadline() {
    atomic.StoreInt64(&n.ElectionDeadline, gtime.Millisecond() + gELECTION_TIMEOUT)
}

// 丢弃现有的DataMap数据，重新从数据文件中读取数据（**使用请慎重**）
func (n *Node) reloadDataMap() {
    var logid int64
    n.DataMap.Clear()
    for {
        list := n.getLogEntriesByLastLogId(logid, 10000, false)
        if len(list) > 0 {
            for _, v := range list {
                n.saveLogEntryToVar(&v)
                logid = v.Id
            }
        } else {
            break;
        }
    }
}