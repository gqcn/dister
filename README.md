
## 介绍
dister(Distribution Cluster)是一款轻量级高性能的分布式集群管理软件，实现了分布式软件架构中的常用核心组件，包括：
1. 服务配置管理中心;
2. 服务注册与发现;
3. 服务健康检查;
4. 服务负载均衡;

dister的灵感来源于ZooKeeper、Consul、Etcd，它们都实现了类似的分布式组件，但是dister更加的轻量级、低成本、易维护、架构清晰、简单实用、性能高效，这也是dister设计的初衷。

dister是开源的，免费的，基于MIT协议进行分发，开源项目地址(gitee与github仓库保持实时同步)： Gitee( https://gitee.com/johng/dister )，Github( https://github.com/johng-cn/dister )

## 特点

1. 开源、免费、跨平台；
1. 使用RAFT算法实现分布式一致性；
1. 使用通用的REST协议提供API操作；
1. 详尽的设计及使用说明文档，易于使用维护；
1. 超高读写性能，适合各种高并发的应用场景；
1. 使用分布式KV键值存储实现服务的配置管理；
1. 支持集群分组，不同的集群之间数据相互隔离；
1. 配置管理简单，简化的API接口以及终端管理命令；


## 安装
1. 建议下载预编译好的各平台版本使用，下载地址：[https://gitee.com/johng/dists](https://gitee.com/johng/dists)
2. (第三方依赖变化较大，目前暂无法编译，新版本开发完成后将会恢复)源码编译安装，需要gf框架的支持，框架地址：[https://gitee.com/johng/gf](https://gitee.com/johng/gf)




## 文档

官方网站：http://johng.cn/dister

相关文档：
1. [dister的介绍及设计](http://johng.cn/dister-brief/)
1. [dister的安装及使用](http://johng.cn/dister-installation-and-usage/)
1. [dister的使用示例](http://johng.cn/dister-example/)
1. [dister的性能测试](http://johng.cn/dister-performance-test/)

## 计划

**v2.00**

    1. 重新梳理RAFT实现，查看有无进一步的改进空间；
    2. 改进binlog设计，全新高可用的binlog文件结构及实现；
    3. 使用KV数据存储设计，使用独立的嵌入式KV数据库进行存储；
    4. 改进数据同步逻辑，保证节点在数据同步的高可用；
    5. 重新梳理、简化、改进服务健康检查业务逻辑；
    6. 严格、仔细的功能及性能测试；

**v2.10**

    1. 增加服务的API控制功能(服务注册与发现、服务健康检查)；

**v2.50**

    1. 增加Socket接口支持；

## 贡献

dister是开源的、免费的软件，这意味着任何人都可以为其开发和进步贡献力量。
dister的项目源代码目前同时托管在 Gitee 和 Github 平台上，您可以选择您喜欢的平台来 fork 项目和合并你的贡献，两个平台的仓库将会保持即时的同步。
我们非常欢迎有更多的朋友加入到dister的开发中来，您为dister所做出的任何贡献都将会被记录到dister的史册中。
