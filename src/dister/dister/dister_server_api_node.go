// 返回格式统一：
// {result:1, message:"", data:""}

package dister

import (
    "g/net/ghttp"
    "g/encoding/gjson"
)

// 查询Peers
func (this *NodeApiNode) Get(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    w.ResponseJson(1, "ok", *this.node.getAllPeers())
}

// 新增Peer
func (this *NodeApiNode) Put(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    this.Post(r, w)
}

// 修改Peer
func (this *NodeApiNode) Post(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    list := make([]string, 0)
    err  := gjson.DecodeTo(r.GetRaw(), &list)
    if err != nil {
        w.ResponseJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    _, err  = this.node.SendToLeader(gMSG_API_PEERS_ADD, gPORT_REPL, gjson.Encode(list))
    if err != nil {
        w.ResponseJson(0, err.Error(), nil)
    } else {
        w.ResponseJson(1, "ok", nil)
    }
}

// 删除Peer
func (this *NodeApiNode) Delete(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    list := make([]string, 0)
    err  := gjson.DecodeTo(r.GetRaw(), &list)
    if err != nil {
        w.ResponseJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    _, err  = this.node.SendToLeader(gMSG_API_PEERS_REMOVE, gPORT_REPL, gjson.Encode(list))
    if err != nil {
        w.ResponseJson(0, err.Error(), nil)
    } else {
        w.ResponseJson(1, "ok", nil)
    }
}
