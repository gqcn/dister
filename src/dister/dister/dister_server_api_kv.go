// 返回格式统一：
// {result:1, message:"", data:""}

package dister

import (
    "fmt"
    "errors"
    "gitee.com/johng/gf/g/net/ghttp"
    "gitee.com/johng/gf/g/encoding/gjson"
)


// K-V 查询
func (this *NodeApiKv) Get(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    k := r.GetRequestString("k")
    if this.node.getRole() != gROLE_SERVER {
        b, err := this.getDataFromLeader(k)
        if err != nil {
            w.WriteJson(0, err.Error(), nil)
        } else {
            w.WriteJson(1, "ok", b)
        }
    } else {
        if b, err := this.node.getDataByApi(k); err != nil {
            w.WriteJson(0, err.Error(), nil)
        } else {
            w.WriteJson(1, "ok", b)
        }
    }
}

// K-V 新增/修改
func (this *NodeApiKv) Post(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    items := make(map[string]string)
    err   := gjson.DecodeTo(r.GetRaw(), &items)
    if err != nil {
        w.WriteJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    data, err := gjson.Encode(items)
    if err != nil {
        w.WriteJson(0, err.Error(), nil)
    } else {
        if _, err  = this.node.SendToLeader(gMSG_REPL_DATA_SET, gPORT_REPL, data); err != nil {
            w.WriteJson(0, err.Error(), nil)
        } else {
            w.WriteJson(1, "ok", nil)
        }
    }
}

// K-V 删除
func (this *NodeApiKv) Delete(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    list := make([]string, 0)
    if err  := gjson.DecodeTo(r.GetRaw(), &list); err != nil {
        w.WriteJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    b, err := gjson.Encode(list)
    if err != nil {
        w.WriteJson(0, err.Error(), nil)
    } else {
        if _, err  = this.node.SendToLeader(gMSG_REPL_DATA_REMOVE, gPORT_REPL, b); err != nil {
            w.WriteJson(0, err.Error(), nil)
        } else {
            w.WriteJson(1, "ok", nil)
        }
    }
}

// 从Leader获取/查询数据
func (this *NodeApiKv) getDataFromLeader(k string) ([]byte, error) {
    leader := this.node.getLeader()
    if leader == nil {
        return nil, errors.New(fmt.Sprintf("leader not found, please try again after leader election done"))
    }
    return this.node.SendToLeader(gMSG_API_DATA_GET, gPORT_REPL, []byte(k))
}
