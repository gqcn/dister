package gluster

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
)

// 获取用于健康检查的所有Service
func (n *Node) getServiceListForCheck() map[string]Service {
    key := fmt.Sprintf("gluster_service_list_for_check_%v", n.getLastServiceLogId())
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

// 服务健康检查回调函数
func (n *Node) serviceHealthCheckHandler() {
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for k, v := range n.getServiceListForCheck() {
                go n.checkServiceHealth(k, v)
            }
        }
        time.Sleep(2000 * time.Millisecond)
    }
}

// 服务健康检测
// 如果新增检测类型，需要更新该方法
func (n *Node) checkServiceHealth(skey string, node Service) {
    checkingKey := "gluster_service_node_checking_" + skey
    if gcache.Get(checkingKey) != nil {
        return
    }
    gcache.Set(checkingKey, 1, 60000)
    ostatus, _ := node.Node["status"]
    n.doCheckService(skey, &node)
    nstatus, _ := node.Node["status"]
    if ostatus != nstatus {
        n.smutex.Lock()
        n.Service.Set(skey, node)
        n.setLastServiceLogId(gtime.Millisecond())
        n.smutex.Unlock()
        glog.Printf("service updated, node: %s, from %v to %v\n", skey, ostatus, nstatus)
    }
    interval, _ := node.Node["interval"]
    timeout     := int64(gSERVICE_HEALTH_CHECK_INTERVAL)
    if interval != nil {
        timeout, _ = strconv.ParseInt(interval.(string), 10, 64)
    }
    // 缓存时间是按照秒进行缓存，因此需要将毫秒转换为秒
    gcache.Set(checkingKey, 1, timeout)
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
    url := node.Node["url"]
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
    return fmt.Sprintf("%d.%s.node.service.gluster", index, name)
}