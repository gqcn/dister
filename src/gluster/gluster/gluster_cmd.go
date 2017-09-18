package gluster

import (
    "g/os/gconsole"
    "strings"
    "fmt"
    "g/net/ghttp"
    "g/encoding/gjson"
    "g/os/gfile"
    "encoding/json"
    "strconv"
)

// 查看集群节点
// 使用方式：gluster nodes
func cmd_nodes () {
    r := ghttp.Get(fmt.Sprintf("http://127.0.0.1:%d/node", gPORT_API))
    if r == nil {
        fmt.Println("ERROR: connect to local gluster api failed")
        return
    }
    defer r.Close()
    content := r.ReadAll()
    peers   := make([]NodeInfo, 0)
    jsonvar := gjson.DecodeToJson(content)
    err := jsonvar.GetToVar("data", &peers)
    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Printf("%32s %30s %15s %12s %10s\n", "Name", "Group", "Ip", "Role", "Status")
        for _,v := range peers {
            status := "alive"
            if v.Status == 0 {
                status = "dead"
            }
            fmt.Printf("%32s %30s %15s %12s %10s\n", v.Name, v.Group, v.Ip, raftRoleName(v.RaftRole), status)
        }
    }
}

// 添加集群节点
// 使用方式：gluster addnode IP1,IP2,IP3,...
func cmd_addnode () {
    nodes := gconsole.Value.Get(2)
    if nodes != "" {
        params := make([]string, 0)
        list   := strings.Split(strings.TrimSpace(nodes), ",")
        for _, v := range list {
            if v != "" {
                params = append(params, v)
            }
        }
        if len(params) > 0 {
            r := ghttp.Post(fmt.Sprintf("http://127.0.0.1:%d/node", gPORT_API), gjson.Encode(params))
            if r == nil {
                fmt.Println("ERROR: connect to local gluster api failed")
                return
            }
            defer r.Close()
            content := r.ReadAll()
            data    := gjson.DecodeToJson(content)
            if data.GetInt("result") != 1 {
                fmt.Println(data.GetString("message"))
                return
            }
        }
    }
    fmt.Println("ok")
}

// 删除集群节点
// 使用方式：gluster delnode IP1,IP2,IP3,...
func cmd_delnode () {
    nodes := gconsole.Value.Get(2)
    if nodes != "" {
        params := make([]string, 0)
        list   := strings.Split(strings.TrimSpace(nodes), ",")
        for _, v := range list {
            if v != "" {
                params = append(params, v)
            }
        }
        if len(params) > 0 {
            r := ghttp.Delete(fmt.Sprintf("http://127.0.0.1:%d/node", gPORT_API), gjson.Encode(params))
            if r == nil {
                fmt.Println("ERROR: connect to local gluster api failed")
                return
            }
            defer r.Close()
            content := r.ReadAll()
            data    := gjson.DecodeToJson(content)
            if data.GetInt("result") != 1 {
                fmt.Println(data.GetString("message"))
                return
            }
        }
    }
    fmt.Println("ok")
}

// 查看所有kv
// 使用方式：gluster kvs
func cmd_kvs () {
    r := ghttp.Get(fmt.Sprintf("http://127.0.0.1:%d/kv", gPORT_API))
    if r == nil {
        fmt.Println("ERROR: connect to local gluster api failed")
        return
    }
    defer r.Close()
    content := r.ReadAll()
    data    := gjson.DecodeToJson(content)
    if data.GetInt("result") != 1 {
        fmt.Println("ERROR: " + data.GetString("message"))
        return
    }
    m := data.GetMap("data")
    if len(m) > 0 {
        // 自动计算key的宽度
        length := 0
        for k, _ := range m {
            if len(k) > length {
                length = len(k)
            }
        }
        lenstr := strconv.Itoa(length)
        format1 := "%-" + lenstr + "s : %s\n"
        format2 := "%-" + lenstr + "s : %.100s\n"
        fmt.Printf(format1, "KEY", "VALUE")
        for k, v := range m {
            fmt.Printf(format2, k, v)
        }
    } else {
        fmt.Println("it's empty")
    }
}


// 查询kv
// 使用方式：gluster getkv 键名
func cmd_getkv () {
    k := gconsole.Value.Get(2)
    r := ghttp.Get(fmt.Sprintf("http://127.0.0.1:%d/kv?k=%s", gPORT_API, k))
    if r == nil {
        fmt.Println("ERROR: connect to local gluster api failed")
        return
    }
    defer r.Close()
    content := r.ReadAll()
    data    := gjson.DecodeToJson(content)
    if data.GetInt("result") != 1 {
        fmt.Println("ERROR: " + data.GetString("message"))
        return
    }
    fmt.Println(data.GetString("data"))
}

// 设置kv
// 使用方式：gluster addkv 键名 键值
func cmd_addkv () {
    k := gconsole.Value.Get(2)
    v := gconsole.Value.Get(3)
    if k != "" && v != ""{
        m := map[string]string{k: v}
        r := ghttp.Post(fmt.Sprintf("http://127.0.0.1:%d/kv", gPORT_API), gjson.Encode(m))
        if r == nil {
            fmt.Println("ERROR: connect to local gluster api failed")
            return
        }
        defer r.Close()
        content := r.ReadAll()
        data    := gjson.DecodeToJson(content)
        if data.GetInt("result") != 1 {
            fmt.Println("ERROR: " + data.GetString("message"))
            return
        }
    }
    fmt.Println("ok")
}

// 删除
// 使用方式：gluster delkv 键名1,键名2,键名3,...
func cmd_delkv () {
    keys := gconsole.Value.Get(2)
    if keys != "" {
        params := make([]string, 0)
        list   := strings.Split(strings.TrimSpace(keys), ",")
        for _, v := range list {
            if v != "" {
                params = append(params, v)
            }
        }
        if len(params) > 0 {
            r := ghttp.Delete(fmt.Sprintf("http://127.0.0.1:%d/kv", gPORT_API), gjson.Encode(params))
            if r == nil {
                fmt.Println("ERROR: connect to local gluster api failed")
                return
            }
            defer r.Close()
            content := r.ReadAll()
            data    := gjson.DecodeToJson(content)
            if data.GetInt("result") != 1 {
                fmt.Println("ERROR: " + data.GetString("message"))
                return
            }
        }
    }
    fmt.Println("ok")
}

// 查看所有Service
// 使用方式：gluster services
func cmd_services () {
    r := ghttp.Get(fmt.Sprintf("http://127.0.0.1:%d/service", gPORT_API))
    if r == nil {
        fmt.Println("ERROR: connect to local gluster api failed")
        return
    }
    defer r.Close()
    content := r.ReadAll()
    data    := gjson.DecodeToJson(content)
    if data.GetInt("result") != 1 {
        fmt.Println("ERROR: " + data.GetString("message"))
        return
    }
    services := data.GetMap("data")
    if services != nil {
        s, _ := json.MarshalIndent(services, "", "    ")
        fmt.Println(string(s))
    }
}

// 查看Service
// 使用方式：gluster getservice Service名称
func cmd_getservice () {
    name := gconsole.Value.Get(2)
    r    := ghttp.Get(fmt.Sprintf("http://127.0.0.1:%d/service?name=%s", gPORT_API, name))
    if r == nil {
        fmt.Println("ERROR: connect to local gluster api failed")
        return
    }
    defer r.Close()
    content := r.ReadAll()
    data    := gjson.DecodeToJson(content)
    if data.GetInt("result") != 1 {
        fmt.Println("ERROR: " + data.GetString("message"))
        return
    }
    service := data.GetMap("data")
    if service != nil {
        s, _ := json.MarshalIndent(service, "", "    ")
        fmt.Println(string(s))
    }
}

// 添加Service
// 使用方式：gluster addservice Service文件路径
func cmd_addservice () {
    path := gconsole.Value.Get(2)
    if path == "" {
        fmt.Println("please sepecify the service config file path")
        return
    }
    if !gfile.Exists(path) {
        fmt.Println("service config file does not exist")
        return
    }
    r := ghttp.Post(fmt.Sprintf("http://127.0.0.1:%d/service", gPORT_API), gfile.GetContents(path))
    if r == nil {
        fmt.Println("ERROR: connect to local gluster api failed")
        return
    }
    defer r.Close()
    content := r.ReadAll()
    data    := gjson.DecodeToJson(content)
    if data.GetInt("result") != 1 {
        fmt.Println("ERROR: " + data.GetString("message"))
        return
    }
    fmt.Println("ok")
}

// 删除Service
// 使用方式：gluster delservice Service名称1,Service名称2,Service名称3,...
func cmd_delservice () {
    s := gconsole.Value.Get(2)
    if s != "" {
        params := make([]string, 0)
        list  := strings.Split(strings.TrimSpace(s), ",")
        for _, v := range list {
            if v != "" {
                params = append(params, v)
            }
        }
        if len(params) > 0 {
            r := ghttp.Delete(fmt.Sprintf("http://127.0.0.1:%d/service", gPORT_API), gjson.Encode(params))
            if r == nil {
                fmt.Println("ERROR: connect to local gluster api failed")
                return
            }
            defer r.Close()
            content := r.ReadAll()
            data    := gjson.DecodeToJson(content)
            if data.GetInt("result") != 1 {
                fmt.Println("ERROR: " + data.GetString("message"))
                return
            }
        }
    }
    fmt.Println("ok")
}


// 负载均衡查询
// 使用方式：gluster balance Service名称
func cmd_balance () {
    name := gconsole.Value.Get(2)
    r := ghttp.Get(fmt.Sprintf("http://127.0.0.1:%d/balance?name=%s", gPORT_API, name))
    if r == nil {
        fmt.Println("ERROR: connect to local gluster api failed")
        return
    }
    defer r.Close()
    content := r.ReadAll()
    data    := gjson.DecodeToJson(content)
    if data.GetInt("result") != 1 {
        fmt.Println("ERROR: " + data.GetString("message"))
        return
    }
    service := data.GetMap("data")
    if service != nil {
        s, _ := json.MarshalIndent(service, "", "    ")
        fmt.Println(string(s))
    }
}
