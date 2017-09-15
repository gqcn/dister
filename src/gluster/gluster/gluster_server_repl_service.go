package gluster

import (
    "time"
    "strings"
    "g/database/gdb"
    "g/net/ghttp"
    "g/core/types/gmap"
    "strconv"
    "g/os/gcache"
    "os/exec"
    "fmt"
    "g/util/gtime"
    "reflect"
    "g/os/glog"
    "errors"
)

// 获取用于健康检查的Service列表
func (n *Node) getServiceListForCheck() []Service {
    key := fmt.Sprintf("gluster_service_list_for_check_%v", n.getLastServiceLogId())
    l   := make([]Service, 0)
    r   := gcache.Get(key)
    if r == nil {
        for _, v := range n.Service.Values() {
            l = append(l, v.(Service))
        }
        gcache.Set(key, l, 60000)
    } else {
        l = r.([]Service)
    }
    return l
}

// 服务健康检查回调函数
func (n *Node) serviceHealthCheckHandler() {
    for {
        if n.getRaftRole() == gROLE_RAFT_LEADER {
            for _, v := range n.getServiceListForCheck() {
                service := v
                go func(service *Service) {
                    n.checkServiceHealth(n.Service, service)
                }(&service)
            }
        }
        time.Sleep(2000 * time.Millisecond)
    }
}

// 服务健康检测
// 如果新增检测类型，需要更新该方法
func (n *Node) checkServiceHealth(group *gmap.StringInterfaceMap, service *Service) {
    //glog.Println(service.Name)
    for k, v := range service.Node {
        if reflect.TypeOf(v).String() != "map[string]interface {}" {
            continue
        }
        name := k
        node := gmap.NewStringInterfaceMap()
        node.BatchSet(v.(map[string]interface {}))
        go func(name string, m *gmap.StringInterfaceMap) {
            cachekey  := fmt.Sprintf("gluster_service_%s_%s_check", service.Name, name)
            needcheck := gcache.Get(cachekey)
            if needcheck != nil {
                return
            }
            gcache.Set(cachekey, 1, 5000)
            ostatus := m.Get("status")
            n.checkServiceNodeItem(service, name, m)
            nstatus := m.Get("status")
            if ostatus != nstatus {
                n.updateServiceNode(group, service, name, *m.Clone())
                glog.Printf("service updated, node: %s, from %v to %v, name: %s, \n", name, ostatus, nstatus, service.Name)
            }
            interval := m.Get("interval")
            timeout  := int64(gSERVICE_HEALTH_CHECK_INTERVAL)
            if interval != nil {
                timeout, _ = strconv.ParseInt(interval.(string), 10, 64)
            }
            // 缓存时间是按照秒进行缓存，因此需要将毫秒转换为秒
            gcache.Set(cachekey, 1, timeout)
        }(name, node)
    }
}

// 健康检测服务项
func (n *Node) checkServiceNodeItem(service *Service, name string, m *gmap.StringInterfaceMap) {
    var err error
    glog.Println("start checking", service.Name, service.Type, name, m.Clone())
    switch strings.ToLower(service.Type) {
        case "mysql":  fallthrough
        case "pgsql":  err = n.dbHealthCheck(service.Type, m)
        case "web":    err = n.webHealthCheck(m)
        case "tcp":    err = n.tcpHealthCheck(m)
        case "custom": err = n.customHealthCheck(m)
    }
    if err != nil {
        glog.Errorf("%s - %s: %s\n", service.Name, name, err.Error())
    }
}

// 更新Service服务项的指定节点
func (n *Node) updateServiceNode(group *gmap.StringInterfaceMap, service *Service, name string, m map[string]interface{}) {
    n.smutex.Lock()
    defer n.smutex.Unlock()

    service.Node[name] = m
    group.Set(service.Name, *service)
    fmt.Printf("%p\n", group)
    fmt.Printf("%p\n", n.Service)
    // 判断地址是否相等，如果不相等，表示服务有更新，当前健康检测的结果将被废弃掉
    if group == n.Service {
        n.setLastServiceLogId(gtime.Microsecond())
    } else {
        glog.Println("service changed, ignore this updates")
    }
}

// MySQL/PostgreSQL数据库健康检查
// 使用并发方式并行测试同一个配置中的数据库链接
func (n *Node) dbHealthCheck(stype string, item *gmap.StringInterfaceMap) error {
    host := item.Get("host")
    port := item.Get("port")
    user := item.Get("user")
    pass := item.Get("pass")
    name := item.Get("database")
    if host == nil || port == nil || user == nil || pass == nil || name == nil {
        return errors.New(fmt.Sprintf("invalid config of service item: %v", item.Clone()))
    }
    dbcfg   := gdb.ConfigNode{
        Host    : host.(string),
        Port    : port.(string),
        User    : user.(string),
        Pass    : pass.(string),
        Name    : name.(string),
        Type    : stype,
    }
    db, err := gdb.NewByConfigNode(dbcfg)
    if err != nil || db == nil {
        item.Set("status", 0)
    } else {
        if db.PingMaster() != nil {
            item.Set("status", 0)
        } else {
            item.Set("status", 1)
        }
        db.Close()
    }
    return nil
}

// WEB健康检测
func (n *Node) webHealthCheck(item *gmap.StringInterfaceMap) error {
    url := item.Get("url")
    if url == nil {
        return errors.New(fmt.Sprintf("invalid config of service item: %v", item.Clone()))
    }
    r := ghttp.Get(url.(string))
    if r == nil || r.StatusCode != 200 {
        item.Set("status", 0)
    } else {
        item.Set("status", 1)
    }
    if r != nil {
        r.Close()
    }
    return nil
}

// TCP链接健康检查
func (n *Node) tcpHealthCheck(item *gmap.StringInterfaceMap) error {
    host := item.Get("host")
    port := item.Get("port")
    if host == nil || port == nil {
        return errors.New(fmt.Sprintf("invalid config of service item: %v", item.Clone()))
    }
    p, err := strconv.Atoi(port.(string))
    if err != nil {
        item.Set("status", 0)
    } else {
        conn := n.getConn(host.(string), p)
        if conn != nil {
            item.Set("status", 1)
            conn.Close()
        } else {
            item.Set("status", 0)
        }
    }
    return nil
}

// 自定义程序健康检测
func (n *Node) customHealthCheck(item *gmap.StringInterfaceMap) error {
    script := item.Get("script")
    if script == nil {
        return errors.New(fmt.Sprintf("invalid config of service item: %v", item.Clone()))
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
    item.Set("status", result)
    return nil
}