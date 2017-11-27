// 返回格式统一：
// {result:1, message:"", data:""}

package dister

import (
    "fmt"
    "errors"
    "gitee.com/johng/gf/g/os/gcache"
    "gitee.com/johng/gf/g/net/ghttp"
    "gitee.com/johng/gf/g/encoding/gjson"
)


// K-V 查询
func (this *NodeApiKv) Get(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    items := make(map[string]string)
    err   := gjson.DecodeTo("{\"key_99999\":\"value_99999\"}", &items)
    if err != nil {
        w.ResponseJson(0, "invalid data type: " + err.Error(), nil)
        return
    }

    _, err  = this.node.SendToLeader(gMSG_REPL_DATA_SET, gPORT_REPL, gjson.Encode(items))
    if err != nil {
        w.WriteHeader(500)
        return
    } else {
        // 为保证客户端能够及时响应（例如在写入请求的下一次获取请求将一定能够获取到最新的数据），
        // 因此，请求端应当在leader返回成功后，同时将该数据写入到本地，这是保证整体集群效率的一个做法
        leader := this.node.getLeader()
        if leader != nil && this.node.getId() != leader.Id {
            this.node.DataMap.BatchSet(items)
        }
        w.ResponseJson(1, "ok", nil)
    }
    return

    k := r.GetRequestString("k")
    if this.node.getRole() != gROLE_SERVER {
        r, err := this.getDataFromLeader(k)
        if err != nil {
            w.ResponseJson(0, err.Error(), nil)
        } else {
            w.Write([]byte(r))
        }
    } else {
        w.Write([]byte(this.node.getDataByApi(k)))
    }
}

// K-V 新增
func (this *NodeApiKv) Put(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    this.Post(r, w)
}

// K-V 修改
func (this *NodeApiKv) Post(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    items := make(map[string]string)
    err   := gjson.DecodeTo(r.GetRaw(), &items)
    if err != nil {
        w.ResponseJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    _, err  = this.node.SendToLeader(gMSG_REPL_DATA_SET, gPORT_REPL, gjson.Encode(items))
    if err != nil {
        w.ResponseJson(0, err.Error(), nil)
    } else {
        w.ResponseJson(1, "ok", nil)
    }
}

// K-V 删除
func (this *NodeApiKv) Delete(r *ghttp.ClientRequest, w *ghttp.ServerResponse) {
    list := make([]string, 0)
    err  := gjson.DecodeTo(r.GetRaw(), &list)
    if err != nil {
        w.ResponseJson(0, "invalid data type: " + err.Error(), nil)
        return
    }
    _, err  = this.node.SendToLeader(gMSG_REPL_DATA_REMOVE, gPORT_REPL, gjson.Encode(list))
    if err != nil {
        w.ResponseJson(0, err.Error(), nil)
    } else {
        w.ResponseJson(1, "ok", nil)
    }
}

// 从Leader获取/查询数据
func (this *NodeApiKv) getDataFromLeader(k string) (string, error) {
    leader := this.node.getLeader()
    if leader == nil {
        return "", errors.New(fmt.Sprintf("leader not found, please try again after leader election done"))
    }
    key    := fmt.Sprintf("dister_data_get_for_api_%d_%s", leader.LastLogId, k)
    result := gcache.Get(key)
    if result != nil {
        return result.(string), nil
    } else {
        r, err := this.node.SendToLeader(gMSG_API_DATA_GET, gPORT_REPL, k)
        if err != nil {
            return "", err
        } else {
            gcache.Set(key, r, 60000)
            return r, nil
        }
    }
}
