package dister

import (
    "errors"
    "fmt"
    "reflect"
    "gitee.com/johng/gf/g/net/ghttp"
    "gitee.com/johng/gf/g/encoding/gjson"
)

// Service 查询
func (this *NodeApiService) Get(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    name := r.GetRequestString("name")
    if this.node.getRole() != gROLE_SERVER {
        r, err := this.getServiceFromLeader(name)
        if err != nil {
            w.WriteJson(0, err.Error(), nil)
        } else {
            w.Write([]byte(r))
        }
    } else {
        if b, err := this.node.getServiceByApi(name); err != nil {
            w.WriteJson(0, err.Error(), nil)
        } else {
            w.WriteJson(1, "ok", b)
        }
    }
}

// Service 新增/修改
func (this *NodeApiService) Post(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    list := make([]ServiceConfig, 0)
    err  := gjson.DecodeTo(r.GetRaw(), &list)
    if err != nil {
        w.WriteJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    // 数据验证
    for _, v := range list {
        err  = this.validateServiceConfig(&v)
        if err != nil {
            w.WriteJson(0, err.Error(), nil)
            return
        }
    }
    // 提交数据到leader
    for _, v := range list {
        b, err := gjson.Encode(v)
        if err != nil {
            w.WriteJson(0, err.Error(), nil)
            return
        }
        _, err  = this.node.SendToLeader(gMSG_API_SERVICE_SET, gPORT_REPL, b)
        if err != nil {
            w.WriteJson(0, err.Error(), nil)
            return
        }
    }
    w.WriteJson(1, "ok", nil)
}

// 数据校验的通用错误
func (this *NodeApiService) validateCommonError(n, t string, k int) error {
    return errors.New(fmt.Sprintf("invalid config of service: %s, type: %s, node index: %d", n, t, k))
}

// 验证Service提交参数
// @todo 这里进行了简单的验证，需要封装表单验证包来实现丰富的验证功能
func (this *NodeApiService) validateServiceConfig(sc *ServiceConfig) error {
    for k, m := range sc.Node {
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
                    return this.validateCommonError(sc.Name, sc.Type, k)
                }

            case "tcp":
                host, _ := m["host"]
                port, _ := m["port"]
                if host == nil || port == nil ||
                    reflect.TypeOf(host).String() != "string" ||
                    reflect.TypeOf(port).String() != "string" {
                    return this.validateCommonError(sc.Name, sc.Type, k)
                }

            case "web":
                url, _ := m["url"]
                if url == nil || reflect.TypeOf(url).String() != "string" {
                    return this.validateCommonError(sc.Name, sc.Type, k)
                }

            case "custom":
                script, _ := m["script"]
                if script == nil || reflect.TypeOf(script).String() != "string" {
                    return this.validateCommonError(sc.Name, sc.Type, k)
                }
        }
    }
    return nil
}

// Service 删除
func (this *NodeApiService) Delete(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    list := make([]string, 0)
    err  := gjson.DecodeTo(r.GetRaw(), &list)
    if err != nil {
        w.WriteJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    b, err := gjson.Encode(list)
    if err != nil {
        w.WriteJson(0, err.Error(), nil)
        return
    }
    _, err  = this.node.SendToLeader(gMSG_API_SERVICE_REMOVE, gPORT_REPL, b)
    if err != nil {
        w.WriteJson(0, err.Error(), nil)
    } else {
        w.WriteJson(1, "ok", nil)
    }
}

// 从Leader获取/查询Service
func (this *NodeApiService) getServiceFromLeader(name string) ([]byte, error) {
    leader := this.node.getLeader()
    if leader == nil {
        return nil, errors.New(fmt.Sprintf("leader not found, please try again after leader election done"))
    }
    return this.node.SendToLeader(gMSG_API_SERVICE_GET, gPORT_REPL, []byte(name))
}


