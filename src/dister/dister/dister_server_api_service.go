package dister

import (
    "g/net/ghttp"
    "g/encoding/gjson"
    "errors"
    "fmt"
    "reflect"
    "g/os/gcache"
)

// Service 查询
func (this *NodeApiService) Get(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    name := r.GetRequestString("name")
    if this.node.getRole() != gROLE_SERVER {
        r, err := this.getServiceFromLeader(name)
        if err != nil {
            w.ResponseJson(0, err.Error(), nil)
        } else {
            w.Write([]byte(r))
        }
    } else {
        w.Write([]byte(this.node.getServiceByApi(name)))
    }
}

// Service 新增
func (this *NodeApiService) Put(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    this.Post(r, w)
}

// Service 修改
func (this *NodeApiService) Post(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    list := make([]ServiceConfig, 0)
    err  := gjson.DecodeTo(r.GetRaw(), &list)
    if err != nil {
        w.ResponseJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    // 数据验证
    for _, v := range list {
        err  = validateServiceConfig(&v)
        if err != nil {
            w.ResponseJson(0, err.Error(), nil)
            return
        }
    }
    // 提交数据到leader
    for _, v := range list {
        _, err  = this.node.SendToLeader(gMSG_API_SERVICE_SET, gPORT_REPL, gjson.Encode(v))
        if err != nil {
            w.ResponseJson(0, err.Error(), nil)
            return
        }
    }
    w.ResponseJson(1, "ok", nil)
}

// 验证Service提交参数
func validateServiceConfig(sc *ServiceConfig) error {
    for k, m := range sc.Node {
        commonError := errors.New(fmt.Sprintf("invalid config of service: %s, type: %s, node index: %d", sc.Name, sc.Type, k))
        switch sc.Type {
            case "pgsql": fallthrough
            case "mysql":
                host, _ := m["host"]
                port, _ := m["port"]
                user, _ := m["user"]
                pass, _ := m["pass"]
                name, _ := m["database"]
                if host == nil || port == nil || user == nil || pass == nil || name == nil ||
                    reflect.TypeOf(host).String() != "string" ||
                    reflect.TypeOf(port).String() != "string" ||
                    reflect.TypeOf(user).String() != "string" ||
                    reflect.TypeOf(pass).String() != "string" ||
                    reflect.TypeOf(name).String() != "string" {
                    return commonError
                }

            case "tcp":
                host, _ := m["host"]
                port, _ := m["port"]
                if host == nil || port == nil ||
                    reflect.TypeOf(host).String() != "string" ||
                    reflect.TypeOf(port).String() != "string" {
                    return commonError
                }

            case "web":
                url, _ := m["url"]
                if url == nil || reflect.TypeOf(url).String() != "string" {
                    return commonError
                }

            case "custom":
                script, _ := m["script"]
                if script == nil || reflect.TypeOf(script).String() != "string" {
                    return commonError
                }
        }
    }
    return nil
}

// Service 删除
func (this *NodeApiService) Delete(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    list := make([]string, 0)
    err  := gjson.DecodeTo(r.GetRaw(), &list)
    if err != nil {
        w.ResponseJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    _, err  = this.node.SendToLeader(gMSG_API_SERVICE_REMOVE, gPORT_REPL, gjson.Encode(list))
    if err != nil {
        w.ResponseJson(0, err.Error(), nil)
    } else {
        w.ResponseJson(1, "ok", nil)
    }
}

// 从Leader获取/查询Service
func (this *NodeApiService) getServiceFromLeader(name string) (string, error) {
    leader := this.node.getLeader()
    if leader == nil {
        return "", errors.New(fmt.Sprintf("leader not found, please try again after leader election done"))
    }
    key    := fmt.Sprintf("dister_service_get_for_api_%d_%s", leader.LastServiceLogId, name)
    result := gcache.Get(key)
    if result != nil {
        return result.(string), nil
    } else {
        r, err := this.node.SendToLeader(gMSG_API_SERVICE_GET, gPORT_REPL, name)
        if err != nil {
            return "", err
        } else {
            gcache.Set(key, r, 60000)
            return r, nil
        }
    }
}


