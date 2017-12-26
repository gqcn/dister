// 返回格式统一：
// {result:1, message:"", data:""}

package dister

import (
    "gitee.com/johng/gf/g/net/ghttp"
    "gitee.com/johng/gf/g/encoding/gjson"
)

// 查询Peers
func (this *NodeApiNode) Get(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    if b, err := gjson.Encode(*this.node.getAllPeers()); err != nil {
        w.WriteJson(0, err.Error(), nil)
    } else {
        w.WriteJson(1, "ok", b)
    }
}

// 新增/修改Peer
func (this *NodeApiNode) Post(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
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
    _, err  = this.node.SendToLeader(gMSG_API_PEERS_ADD, gPORT_REPL, b)
    if err != nil {
        w.WriteJson(0, err.Error(), nil)
    } else {
        w.WriteJson(1, "ok", nil)
    }
}

// 删除Peer
func (this *NodeApiNode) Delete(s *ghttp.Server, r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
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
    _, err  = this.node.SendToLeader(gMSG_API_PEERS_REMOVE, gPORT_REPL, b)
    if err != nil {
        w.WriteJson(0, err.Error(), nil)
    } else {
        w.WriteJson(1, "ok", nil)
    }
}
