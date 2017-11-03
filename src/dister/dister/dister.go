/*
    使用raft算法处理集群的一致性
    已解决split brains造成的数据一致性问题：
        经典分区多节点脑裂问题：出现两个及以上的分区网络，网络之间无法相互连接
        复杂非分区三点脑裂问题：A-B-C，AC之间无法相互连接（B是双网卡），这样会造成A、C为leader，B为follower
        以上需要解决的是数据一致性问题，解决方案：检测集群各节点需要相互能够连通，剔除不连通的节点（例如星型拓扑结构的节点）
 */

package dister

import (
    "os"
    "g/core/types/gmap"
    "sync"
    "g/core/types/glist"
    "g/net/ghttp"
    "g/os/glog"
    "g/os/gfile"
    "net"
    "time"
    "io"
    "g/net/gip"
    "strings"
    "g/encoding/gcompress"
    "g/os/gconsole"
    "fmt"
    "g/encoding/ghash"
    "g/database/gkvdb"
)

const (
    gVERSION                                = "1.6"   // 当前版本
    gDEBUG                                  = true    // 用于控制调试信息(开发阶段使用)
    gCOMPRESS_COMMUNICATION                 = true    // 是否在通信时进行内容压缩(开发阶段使用)
    gCOMPRESS_SAVING                        = false   // 是否在存储时压缩内容(开发阶段使用)
    gLOGENTRY_FILE_SIZE                     = 100000  // 每个LogEntry存储文件的最大存储数量，不能随意改动
    gLOGENTRY_RANDOM_ID_SIZE                = 10000   // 每个LogEntry的ID生成随机数的长度：10000表示4个随机数，1000表示3个，以此类推

    // 集群端口定义
    gPORT_RAFT                              = 4166    // 集群协议通信接口
    gPORT_REPL                              = 4167    // 集群数据同步接口
    gPORT_API                               = 4168    // 服务器对外API接口
    //gPORT_MONITOR                           = 4169    // 监控服务接口
    //gPORT_WEBUI                             = 4170    // WEB管理界面

    // 节点状态
    gSTATUS_DEAD                            = 0
    gSTATUS_ALIVE                           = 1

    // 集群角色
    gROLE_SERVER                            = 0
    gROLE_CLIENT                            = 1
    gROLE_MONITOR                           = 2

    // RAFT角色
    gROLE_RAFT_FOLLOWER                     = 0
    gROLE_RAFT_CANDIDATE                    = 1
    gROLE_RAFT_LEADER                       = 2

    // 超时时间设置
    gTCP_RETRY_COUNT                        = 0       // TCP请求失败时的重试次数
    gTCP_READ_TIMEOUT                       = 6000    // (毫秒)TCP链接读取超时
    gELECTION_TIMEOUT                       = 2000    // (毫秒)RAFT选举超时时间(如果Leader挂掉之后到重新选举的时间间隔)
    gELECTION_TIMEOUT_HEARTBEAT             = 500     // (毫秒)RAFT Leader统治维持心跳间隔
    gLOG_REPL_DATA_UPDATE_INTERVAL          = 1000    // (毫秒)数据同步间隔
    gLOG_REPL_SERVICE_UPDATE_INTERVAL       = 2000    // (毫秒)Service同步检测心跳间隔
    gLOG_REPL_AUTOSAVE_INTERVAL             = 1000    // (毫秒)数据自动物理化保存的间隔(更新时会做更新判断)
    gLOG_REPL_LOGCLEAN_INTERVAL             = 5000    // (毫秒)LogList定期清理过期(已同步)的日志列表
    gLOG_REPL_PEERS_INTERVAL                = 5000    // (毫秒)Peers节点信息同步(非完整同步)
    gSERVICE_HEALTH_CHECK_INTERVAL          = 2000    // (毫秒)健康检查默认间隔

    // RAFT操作
    gMSG_RAFT_HI                            = 110
    gMSG_RAFT_HI2                           = 120
    gMSG_RAFT_RESPONSE                      = 130
    gMSG_RAFT_HEARTBEAT                     = 140
    gMSG_RAFT_I_AM_LEADER                   = 150
    gMSG_RAFT_SPLIT_BRAINS_CHECK            = 160
    gMSG_RAFT_SPLIT_BRAINS_UNSET            = 170
    gMSG_RAFT_SPLIT_BRAINS_CHANGE           = 180
    gMSG_RAFT_SCORE_REQUEST                 = 190
    gMSG_RAFT_SCORE_COMPARE_REQUEST         = 200
    gMSG_RAFT_SCORE_COMPARE_FAILURE         = 210
    gMSG_RAFT_SCORE_COMPARE_SUCCESS         = 220
    gMSG_RAFT_LEADER_COMPARE_REQUEST        = 230
    gMSG_RAFT_LEADER_COMPARE_FAILURE        = 240
    gMSG_RAFT_LEADER_COMPARE_SUCCESS        = 250

    // 数据同步操作
    gMSG_REPL_DATA_SET                      = 300
    gMSG_REPL_DATA_REMOVE                   = 310
    gMSG_REPL_DATA_APPENDENTRY              = 320
    gMSG_REPL_DATA_REPLICATION              = 330
    gMSG_REPL_VALID_LOGID_CHECK_FIX         = 340
    gMSG_REPL_FAILED                        = 350
    gMSG_REPL_RESPONSE                      = 360
    gMSG_REPL_PEERS_UPDATE                  = 370
    gMSG_REPL_CONFIG_FROM_FOLLOWER          = 380
    gMSG_REPL_SERVICE_UPDATE                = 390

    // API相关
    gMSG_API_DATA_GET                       = 500
    gMSG_API_PEERS_ADD                      = 510
    gMSG_API_PEERS_REMOVE                   = 520
    gMSG_API_SERVICE_GET                    = 530
    gMSG_API_SERVICE_SET                    = 540
    gMSG_API_SERVICE_REMOVE                 = 550
)

// 消息
type Msg struct {
    Head int
    Body string
    Info NodeInfo
}

// 服务器节点信息
type Node struct {
    mutex                sync.RWMutex             // 通用锁，可以使用不同的锁来控制对应变量以提高读写效率
    dmutex               sync.RWMutex             // DataMap锁，用以保证KV请求的先进先出队列执行

    Group                string                   // 集群名称
    Id                   string                   // 节点ID(根据算法自动生成的集群唯一名称)
    Name                 string                   // 节点主机名称
    Ip                   string                   // 主机节点的ip，由通信的时候进行填充，
                                                  // 一个节点可能会有多个IP，这里保存最近通信的那个，节点唯一性识别使用的是Name字段
    CfgFilePath          string                   // 配置文件绝对路径
    CfgReplicated        bool                     // 本地配置对象是否已同步到leader(配置同步需要注意覆盖问题)
    Peers                *gmap.StringInterfaceMap // 集群所有的节点信息(ip->节点信息)，不包含自身
    Role                 int32                    // 集群角色
    RaftRole             int32                    // RAFT角色
    Leader               *NodeInfo                // Leader节点信息
    MinNode              int32                    // 最小节点数
    Score                int64                    // 选举比分
    ScoreCount           int32                    // 选举比分的节点数
    ElectionDeadline     int64                    // 选举超时时间点
    AutoScan             bool                     // 启动时自动扫描局域网，添加dister节点

    LogIdIndex           int64                    // 用于生成LogId的参考字段
    LastLogId            int64                    // 最后一次保存log的id，用以数据一致性判断
    LastServiceLogId     int64                    // 最后一次保存的service id号，用以识别本地Service数据是否已更新，不做Leader与Follower的同步数据
    LogList              *glist.SafeList          // 日志列表，用以存储临时的消息日志，以便快速进行数据同步到其他节点，仅在Leader节点存储
    ServiceList          *glist.SafeList          // Service同步事件列表，用以Service同步
    SavePath             string                   // 物理存储的本地数据*目录*绝对路径
    Service              *gkvdb.DB                // 存储的服务配置表
    DataMap              *gkvdb.DB                // 存储的K-V哈希表
}

// 服务节点对象(用于程序更新及检索结构)
type Service struct {
    Type  string                   `json:"type"`
    Node  map[string]interface{}   `json:"node"`
}

// 服务信息配置对象(用于配置结构)
type ServiceConfig struct {
    Name  string                   `json:"name"`
    Type  string                   `json:"type"`
    Node  []map[string]interface{} `json:"node"`
}

// 用于KV API接口的对象
type NodeApiKv struct {
    ghttp.Controller
    node *Node
}

// 用于Node API接口的对象
type NodeApiNode struct {
    ghttp.Controller
    node *Node
}

// 用于Service API接口的对象
type NodeApiService struct {
    ghttp.Controller
    node *Node
}

// 用于Service 负载均衡API接口的对象
type NodeApiBalance struct {
    ghttp.Controller
    node *Node
}

// 用于Monitor WebUI对象
type MonitorWebUI struct {
    ghttp.Controller
    node *Node
}

// 节点信息
type NodeInfo struct {
    Name             string `json:"name"`
    Group            string `json:"group"`
    Id               string `json:"id"`
    Ip               string `json:"ip"`
    Status           int32  `json:"status"`
    Role             int32  `json:"role"`
    RaftRole         int32  `json:"raft"`
    LastLogId        int64  `json:"logid"`
    LastServiceLogId int64  `json:"serviceid"`
    Version          string `json:"version"`
}

// 日志记录项
type LogEntry struct {
    Id               int64                  // 唯一ID
    Act              int
    Items            interface{}            // map[string]string或[]string
}

// 绑定本地IP并创建一个服务节点
func NewServer() *Node {
    // 主机名称
    hostname, err := os.Hostname()
    if err != nil {
        glog.Fatalln("getting local hostname failed:", err)
        return nil
    }
    node := Node {
        Id                  : nodeId(),
        Ip                  : "127.0.0.1",
        Name                : hostname,
        Group               : "default.group.dister",
        Role                : gROLE_SERVER,
        RaftRole            : gROLE_RAFT_FOLLOWER,
        Leader              : nil,
        MinNode             : 2,
        AutoScan            : true,
        Peers               : gmap.NewStringInterfaceMap(),
        SavePath            : gfile.SelfDir(),
        LogList             : glist.NewSafeList(),
        ServiceList         : glist.NewSafeList(),
    }
    ips, err := gip.IntranetIP()
    if err == nil && len(ips) == 1 {
        node.Ip = ips[0]
    }
    // 命令行操作绑定
    gconsole.BindHandle("nodes",      cmd_nodes)
    gconsole.BindHandle("addnode",    cmd_addnode)
    gconsole.BindHandle("delnode",    cmd_delnode)
    gconsole.BindHandle("kvs",        cmd_kvs)
    gconsole.BindHandle("getkv",      cmd_getkv)
    gconsole.BindHandle("addkv",      cmd_addkv)
    gconsole.BindHandle("delkv",      cmd_delkv)
    gconsole.BindHandle("services",   cmd_services)
    gconsole.BindHandle("getservice", cmd_getservice)
    gconsole.BindHandle("addservice", cmd_addservice)
    gconsole.BindHandle("delservice", cmd_delservice)
    gconsole.BindHandle("balance",    cmd_balance)
    gconsole.BindHandle("help",       cmd_help)
    gconsole.BindHandle("?",          cmd_help)

    return &node
}

// 生成节点的唯一ID(md5(第一张网卡的mac地址))
// @todo 可以考虑有无更好的方式标识一个节点的唯一性
func nodeId() string {
    interfaces, err := net.Interfaces()
    if err != nil {
        glog.Fatalln("getting local MAC address failed:", err)
    }
    mac := ""
    for _, inter := range interfaces {
        address := inter.HardwareAddr.String()
        if address != "" {
            mac = address
            break;
        }
    }
    if mac == "" {
        glog.Fatalln("getting local MAC address failed")
    }
    return strings.ToUpper(fmt.Sprintf("%x", ghash.BKDRHash([]byte(mac))))
}

// 获取数据
func Receive(conn net.Conn) []byte {
    retry      := 0
    buffersize := 1024
    data       := make([]byte, 0)
    for {
        buffer      := make([]byte, buffersize)
        length, err := conn.Read(buffer)
        if length < 1 && err != nil {
            if err == io.EOF || retry > gTCP_RETRY_COUNT - 1 {
                break;
            }
            retry ++
            time.Sleep(100 * time.Millisecond)
        } else {
            if length == buffersize {
                data = append(data, buffer...)
                // 如果读取的数据太大，需要延迟超时时间
                conn.SetReadDeadline(time.Now().Add(gTCP_READ_TIMEOUT * time.Millisecond))
                // 这句很重要，用于等待缓冲区数据，以便下一次读取，如果马上读取在大数据请求的情况下会引起数据被截断
                time.Sleep(time.Millisecond)
            } else {
                data = append(data, buffer[0:length]...)
                break;
            }
            if err == io.EOF {
                break;
            }
        }
    }
    if gCOMPRESS_COMMUNICATION {
        return gcompress.UnZlib(data)
    }

    return data
}

// 发送数据
func Send(conn net.Conn, data []byte) error {
    retry := 0
    for {
        if gCOMPRESS_COMMUNICATION {
            //size1 := len(data)
            data = gcompress.Zlib(data)
            //size2 := len(data)
            //glog.Debugfln("send size compressed from %d to %d", size1, size2)
        }
        _, err := conn.Write(data)
        if err != nil {
            if retry > gTCP_RETRY_COUNT - 1 {
                return err
            }
            retry ++
            time.Sleep(100 * time.Millisecond)
        } else {
            return nil
        }
    }
}

// 将集群角色字段转换为可读的字符串
func roleName(role int32) string {
    switch role {
        case gROLE_CLIENT:  return "client"
        case gROLE_SERVER:  return "server"
        case gROLE_MONITOR: return "monitor"
    }
    return "unknown"
}

// 将RAFT角色字段转换为可读的字符串
func raftRoleName(role int32) string {
    switch role {
        case gROLE_RAFT_FOLLOWER:  return "follower"
        case gROLE_RAFT_CANDIDATE: return "candidate"
        case gROLE_RAFT_LEADER:    return "leader"
    }
    return "unknown"
}
