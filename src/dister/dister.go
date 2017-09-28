// 高性能的分布式集群管理工具
// 1、分布式KV数据管理
// 2、服务注册与发现
// 3、服务健康检查
// 4、服务负载均衡
package main

import (
    "dister/dister"
)

func main() {
    server := dister.NewServer()
    server.Run()

    select { }
}