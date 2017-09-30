package dister

import (
    "time"
    "strings"
    "g/database/gdb"
    "g/net/ghttp"
    "strconv"
    "g/os/gcache"
    "os/exec"
    "fmt"
    "g/util/gtime"
    "g/os/glog"
    "errors"
    "regexp"
    "sync"
    "g/encoding/gjson"
)

// 用于Service API操作的读写锁
var sApiMutex sync.RWMutex

// 用于API访问，将Service转换为API方便查询的结构，并缓存
func (n *Node) getServiceMap() *map[string]ServiceConfig {
    key := fmt.Sprintf("dister_service_map_for_api_%v", n.getLastServiceLogId())
    r   := gcache.Get(key)
    if r == nil {
        sApiMutex.Lock()
        defer sApiMutex.Unlock()
        m      := make(map[string]ServiceConfig)
        reg, _ := regexp.Compile(`^(\d+)\.(\w+)\.service\.dister$`)
        for k, v := range *n.Service.Clone() {
            match := reg.FindStringSubmatch(k)
            if match != nil {
                s        := v.(Service)
                scptr    := new(ServiceConfig)
                name     := match[2]
                rsc, ok  := m[name]
                if !ok {
                    scptr = &ServiceConfig {
                        Name: name,
                        Type: s.Type,
                        Node: make([]map[string]interface{}, 0),
                    }
                } else {
                    scptr = &rsc
                }
                // 注意：这里需要对Node的map数据进行深度拷贝(使用实现，缓存的时候可以不考虑效率)
                scptr.Node = append(scptr.Node, gjson.Decode(gjson.Encode(s.Node)).(map[string]interface{}))
                m[name]    = *scptr
            }
        }
        gcache.Set(key, &m, 60000)
        return &m
    } else {
        return r.(*map[string]ServiceConfig)
    }
}

// 用于API访问，查询所有Service配置
func (n *Node) getServiceMapForApi() interface{} {
    key    := fmt.Sprintf("dister_service_map_value_for_api_%v", n.getLastServiceLogId())
    result := gcache.Get(key)
    if result == nil {
        m := n.getServiceMap()
        sApiMutex.RLock()
        defer sApiMutex.RUnlock()
        result = gjson.Decode(gjson.Encode(*m))
        gcache.Set(key, result, 60000)
        return result
    }
    return result
}

// 用于API访问，查询单条Service配置，返回值为interface{}是为了API端处理方便
func (n *Node) getServiceForApiByName(name string) interface{} {
    key    := fmt.Sprintf("dister_service_for_api_by_name_%s_%v", name, n.getLastServiceLogId())
    result := gcache.Get(key)
    if result == nil {
        m := n.getServiceMap()
        sApiMutex.RLock()
        defer sApiMutex.RUnlock()
        if r, ok := (*m)[name]; ok {
            var sc ServiceConfig
            if gjson.DecodeTo(gjson.Encode(r), &sc) == nil {
                gcache.Set(key, sc, 60000)
                return sc
            }
        }
    }
    return result
}

// 获取用于健康检查的所有Service
// 这里使用缓存降低Service.Clone压力，提高执行效率
func (n *Node) getServiceListForCheck() map[string]Service {
    key := fmt.Sprintf("dister_service_list_for_check_%v", n.getLastServiceLogId())
    m   := make(map[string]Service)
    r   := gcache.Get(key)
    if r == nil {
        for k,v := range *n.Service.Clone() {
            m[k] = v.(Service)
        }
        gcache.Set(key, m, 60000)
    } else {
        m = r.(map[string]Service)
    }
    return m
}

// 根据名称列表删除Service
// 返回值，true 表示有更新，false 表示没有更新(没有找到删除项)
func (n *Node) removeServiceByNames(names []string) bool {
    updated := false
    for _, name := range names {
        for i := 0;; i++ {
            key := n.getServiceKeyByNameAndIndex(name, i)
            if n.Service.Contains(key) {
                n.Service.Remove(key)
                updated = true
            } else {
                break;
            }
        }
    }
    return updated
}

// 服务健康检查回调函数
func (n *Node) serviceHealthCheckHandler() {
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for k, v := range n.getServiceListForCheck() {
                go n.checkServiceHealth(k, v)
            }
        }
        time.Sleep(1000 * time.Millisecond)
    }
}

// 服务健康检测
// 如果新增检测类型，需要更新该方法
func (n *Node) checkServiceHealth(skey string, node Service) {
    checkingKey := "dister_service_node_checking_" + skey
    if gcache.Get(checkingKey) != nil {
        //glog.Debugfln("node:%s is being checking health", skey)
        return
    }
    gcache.Set(checkingKey, struct {}{}, 60000)
    //glog.Debugfln("checking health of node:%s", skey)
    ostatus, _ := node.Node["status"]
    n.doCheckService(skey, &node)
    nstatus, _ := node.Node["status"]
    // 无论状态是int还是float64，这里统一转换为字符串进行比较
    if fmt.Sprintf("%v", ostatus) != fmt.Sprintf("%v", nstatus) {
        n.Service.Set(skey, node)
        n.setLastServiceLogId(gtime.Millisecond())
        glog.Printf("service updated, node: %s, from %v to %v\n", skey, ostatus, nstatus)
    }

    timeout     := int64(gSERVICE_HEALTH_CHECK_INTERVAL)
    interval, _ := node.Node["interval"]
    if interval != nil {
        if r, err := strconv.ParseInt(interval.(string), 10, 64); err == nil {
            timeout = r
        }
    }
    //glog.Debugfln("checking health of node:%s, done", skey)
    gcache.Set(checkingKey, struct {}{}, timeout)
}

// 健康检测服务项
func (n *Node) doCheckService(skey string, node *Service) {
    var err error
    //glog.Println("start checking", skey, node.Type, node.Node)
    switch strings.ToLower(node.Type) {
        case "mysql":  fallthrough
        case "pgsql":  err = n.dbHealthCheck(node)
        case "web":    err = n.webHealthCheck(node)
        case "tcp":    err = n.tcpHealthCheck(node)
        case "custom": err = n.customHealthCheck(node)
    }
    if err != nil {
        glog.Errorf("%s: %s\n", skey, err.Error())
    }
}

// MySQL/PostgreSQL数据库健康检查
// 使用并发方式并行测试同一个配置中的数据库链接
func (n *Node) dbHealthCheck(node *Service) error {
    host, _ := node.Node["host"]
    port, _ := node.Node["port"]
    user, _ := node.Node["user"]
    pass, _ := node.Node["pass"]
    name, _ := node.Node["database"]
    if host == nil || port == nil || user == nil || pass == nil || name == nil {
        return errors.New(fmt.Sprintf("invalid config of service item: %v", node.Node))
    }
    dbcfg   := gdb.ConfigNode{
        Host    : host.(string),
        Port    : port.(string),
        User    : user.(string),
        Pass    : pass.(string),
        Name    : name.(string),
        Type    : node.Type,
    }
    db, err := gdb.NewByConfigNode(dbcfg)
    if err != nil || db == nil {
        node.Node["status"] = 0
    } else {
        if db.PingMaster() != nil {
            node.Node["status"] = 0
        } else {
            node.Node["status"] = 1
        }
        db.Close()
    }
    return nil
}

// WEB健康检测
func (n *Node) webHealthCheck(node *Service) error {
    url, _ := node.Node["url"]
    if url == nil {
        return errors.New(fmt.Sprintf("invalid config of service item: %v", node.Node))
    }
    r := ghttp.Get(url.(string))
    if r == nil || r.StatusCode != 200 {
        node.Node["status"] = 0
    } else {
        node.Node["status"] = 1
    }
    if r != nil {
        r.Close()
    }
    return nil
}

// TCP链接健康检查
func (n *Node) tcpHealthCheck(node *Service) error {
    host, _ := node.Node["host"]
    port, _ := node.Node["port"]
    if host == nil || port == nil {
        return errors.New(fmt.Sprintf("invalid config of service item: %v", node.Node))
    }
    p, err := strconv.Atoi(port.(string))
    if err != nil {
        node.Node["status"] = 0
    } else {
        conn := n.getConn(host.(string), p)
        if conn != nil {
            defer conn.Close()
            node.Node["status"] = 1
        } else {
            node.Node["status"] = 0
        }
    }
    return nil
}

// 自定义程序健康检测
func (n *Node) customHealthCheck(node *Service) error {
    script, _ := node.Node["script"]
    if script == nil {
        return errors.New(fmt.Sprintf("invalid config of service item: %v", node.Node))
    }
    parts  := strings.Fields(script.(string))
    result := 0
    if len(parts) > 1 {
        r, err := exec.Command(parts[0], parts[1:]...).Output()
        if err != nil {
            //glog.Error(err)
        } else if strings.TrimSpace(string(r)) == "1" {
            result = 1
        }
    } else {
        r, err := exec.Command(parts[0]).Output()
        if err != nil {
            //glog.Error(err)
        } else if strings.TrimSpace(string(r)) == "1" {
            result = 1
        }

    }
    node.Node["status"] = result
    return nil
}

// 根据服务的分组名称和节点索引生成对应的服务唯一键名
func (n *Node) getServiceKeyByNameAndIndex(name string, index int) string {
    return fmt.Sprintf("%d.%s.service.dister", index, name)
}