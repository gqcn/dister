package gluster

import (
    "g/net/ghttp"
    "fmt"
    "errors"
    "g/util/grand"
    "strconv"
    "g/os/gcache"
)

// 用于负载均衡计算的结构体
type PriorityNode struct {
    index    int
    priority int
}

// 负载均衡查询
func (this *NodeApiBalance) Get(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    name := r.GetRequestString("name")
    if name == "" {
        w.ResponseJson(0, "incomplete input: name is required", nil)
    } else {
        key    := fmt.Sprintf("gluster_service_balance_name_%s_%v", name, this.node.getLastServiceLogId())
        result := gcache.Get(key)
        if result == nil {
            r, err := this.getAliveServiceByPriority(name)
            if err != nil {
                w.ResponseJson(0, err.Error(), nil)
                return
            } else {
                result = r
                gcache.Set(key, result, 0)
            }
        }
        w.ResponseJson(1, "ok", result)
    }
}

// 查询存货的service, 并根据priority计算负载均衡，取出一条返回
func (this *NodeApiBalance) getAliveServiceByPriority(name string ) (interface{}, error) {
    r := this.node.getServiceForApiByName(name)
    if r == nil {
        return nil, errors.New(fmt.Sprintf("no service named '%s'", name))
    }
    s    := r.(ServiceConfig)
    list := make([]PriorityNode, 0)
    for k, m := range s.Node {
        status, ok := m["status"]
        if !ok || fmt.Sprintf("%v", status) == "0" {
            continue
        }
        priority, ok := m["priority"]
        if !ok {
            continue
        }
        r, err := strconv.Atoi(priority.(string))
        if err == nil {
            list = append(list, PriorityNode{k, r})
        }
    }
    if len(list) < 1 {
        return nil, errors.New("service does not support balance, or no nodes of this service are alive")
    }
    index := this.getServiceByPriority(list)
    if index < 0 {
        return nil, errors.New("get node by balance failed, please check the data structure of the service")
    }
    return s.Node[index], nil
}

// 根据priority计算负载均衡
func (this *NodeApiBalance) getServiceByPriority (list []PriorityNode) int {
    if len(list) < 2 {
        return list[0].index
    }
    var total int
    for i := 0; i < len(list); i++ {
        total += list[i].priority * 100
    }
    r   := grand.Rand(0, total)
    min := 0
    max := 0
    for i := 0; i < len(list); i++ {
        max = min + list[i].priority * 100
        //fmt.Printf("r: %d, min: %d, max: %d\n", r, min, max)
        if r >= min && r < max {
            return list[i].index
        } else {
            min = max
        }
    }
    return -1
}
