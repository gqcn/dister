package dister

import (
    "fmt"
    "errors"
    "strconv"
    "gitee.com/johng/gf/g/net/ghttp"
    "gitee.com/johng/gf/g/os/gcache"
    "gitee.com/johng/gf/g/util/grand"
    "gitee.com/johng/gf/g/encoding/gjson"
)

// 用于负载均衡计算的结构体
type PriorityNode struct {
    index    int
    priority int
}

// 负载均衡查询
func (this *NodeApiBalance) Get(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    name := r.GetRequestString("name")
    if name == "" {
        w.WriteJson(0, "incomplete input: name is required", nil)
    } else {
        key    := fmt.Sprintf("dister_service_balance_name_%s_%v", name, this.node.getLastServiceLogId())
        result := gcache.Get(key)
        if result == nil {
            r, err := this.getAliveServiceByPriority(name)
            if err != nil {
                w.WriteJson(0, err.Error(), nil)
                return
            } else {
                result = r
                gcache.Set(key, result, 1000)
            }
        }
        w.WriteJson(1, "ok", result.([]byte))
    }
}

// 从Leader获取/查询Service
func (this *NodeApiBalance) getServiceFromLeaderByName(name string) (interface{}, error) {
    leader := this.node.getLeader()
    if leader == nil {
        return nil, errors.New(fmt.Sprintf("leader not found, please try again after leader election done"))
    }
    key    := fmt.Sprintf("dister_balance_service_for_api_%d_%s", leader.LastServiceLogId, name)
    result := gcache.Get(key)
    if result != nil {
        return result, nil
    } else {
        if r, err := this.node.SendToLeader(gMSG_API_SERVICE_GET, gPORT_REPL, []byte(name)); err != nil {
            return nil, err
        } else {
            var res ghttp.ResponseJson
            err = gjson.DecodeTo(r, &res)
            if err != nil {
                return nil, err
            } else {
                // 转换数据结构，这里即使res.Data是空，也必须存一个空的对象进去，以便缓存
                sc := ServiceConfig{}
                if err := gjson.DecodeTo(res.Data, &sc); err != nil {
                    return nil, err
                }
                gcache.Set(key, sc, 3600000)
                return sc, nil
            }
        }
    }
}

// 查询存活的service, 并根据priority计算负载均衡，取出一条返回
func (this *NodeApiBalance) getAliveServiceByPriority(name string) (interface{}, error) {
    var s ServiceConfig
    if this.node.getRole() != gROLE_SERVER {
        r, err := this.getServiceFromLeaderByName(name)
        if err != nil {
            return nil, err
        } else {
            s = r.(ServiceConfig)
        }
    } else {
        r := this.node.getServiceForApiByName(name)
        if r == nil {
            return nil, errors.New(fmt.Sprintf("no service named '%s'", name))
        } else {
            s = r.(ServiceConfig)
        }
    }
    if s.Name == "" {
        return nil, errors.New(fmt.Sprintf("no service named '%s'", name))
    }
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
        return nil, errors.New("no nodes of this service are alive")
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
