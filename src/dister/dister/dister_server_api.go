// 封装API常用方法
package dister

import (
    "g/net/ghttp"
    "g/encoding/gjson"
)

// Api数据查询
func (n *Node) getDataByApi(k string) string {
    var r ghttp.ResponseJson
    if k == "" {
        r = ghttp.ResponseJson{0, "need a key to search", nil}
    } else {
        if v := n.DataMap.Get([]byte(k)); v != nil {
            r = ghttp.ResponseJson{1, "ok", string(v)}
        } else {
            r = ghttp.ResponseJson{0, "data not found", nil}
        }
    }
    return gjson.Encode(r)
}

// Api Service查询
func (n *Node) getServiceByApi(name string) string {
    var r ghttp.ResponseJson
    if name == "" {
        if n.Service.Size() > 1000 {
            r = ghttp.ResponseJson{0, "too large service size, need a service name to search", nil}
        } else {
            r = ghttp.ResponseJson{1, "ok", n.getServiceMapForApi()}
        }
    } else {
        sc := n.getServiceForApiByName(name)
        if sc != nil {
            r = ghttp.ResponseJson{1, "ok", sc}
        } else {
            r = ghttp.ResponseJson{0, "service not found", nil}
        }
    }
    return gjson.Encode(r)
}

