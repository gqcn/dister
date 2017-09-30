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
        if n.DataMap.Size() > 1000 {
            r = ghttp.ResponseJson{0, "too large data size, need a key to search", nil}
        } else {
            r = ghttp.ResponseJson{1, "ok", *n.DataMap.Clone()}
        }
    } else {
        if n.DataMap.Contains(k) {
            r = ghttp.ResponseJson{1, "ok", n.DataMap.Get(k)}
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

