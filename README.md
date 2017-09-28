# dister

### dister的介绍
dister是一款轻量级高性能的分布式集群管理软件，实现了分布式软件架构中的常用核心组件，包括：
1. 服务配置管理中心;
2. 服务注册与发现;
3. 服务健康检查;
4. 服务负载均衡;

dister的灵感来源于ZooKeeper及Consul，它们都实现了类似的分布式组件，但是dister更加的轻量级、低成本、易维护、架构清晰、简单实用、性能高效，这也是dister设计的初衷。

### dister的特点

1. 开源、免费、跨平台；
1. 使用RAFT算法实现分布式一致性；
1. 使用通用的REST协议提供API操作；
1. 使用分布式KV键值存储实现服务的配置管理；
1. 超高读写性能，适合各种高并发的应用场景；
1. 支持集群分组，不同的集群之间数据相互隔离；
1. 配置管理简单，且仅提供实用的API接口以及终端管理命令，轻量级、低成本、易维护；


官方网站：http://johng.cn/dister

相关文档：
1. [dister的介绍及设计](http://johng.cn/dister-brief/)
2. [dister的安装及使用](http://johng.cn/dister-installation-and-usage/)
3. [dister的使用示例](http://johng.cn/dister-example/)




