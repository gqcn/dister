# 安装
1. 建议下载预编译好的各平台版本使用，地址：[https://gitee.com/johng/dists](https://gitee.com/johng/dists)
2. 源码编译安装，需要gf框架的支持，地址：[https://gitee.com/johng/gf](https://gitee.com/johng/gf)


# 介绍
dister(Distribution Cluster)是一款轻量级高性能的分布式集群管理软件，实现了分布式软件架构中的常用核心组件，包括：
1. 服务配置管理中心;
2. 服务注册与发现;
3. 服务健康检查;
4. 服务负载均衡;

dister的灵感来源于ZooKeeper及Consul，它们都实现了类似的分布式组件，但是dister更加的轻量级、低成本、易维护、架构清晰、简单实用、性能高效，这也是dister设计的初衷。


# 特点

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
1. [dister的安装及使用](http://johng.cn/dister-installation-and-usage/)
1. [dister的使用示例](http://johng.cn/dister-example/)
1. [dister的性能测试](http://johng.cn/dister-performance-test/)


# 历史
* 2017-06-29 go开发框架gf项目立项，其中一个子项目为研究RAFT分布式算法而成立，名称为gluster
* 2017-09-06 gluster作为一个子项目从gf项目中拆分出来
* 2017-09-20 gluster v1.0开源版本发布，托管于https://gitee.com/johng/gluster
* 2017-09-28 为避贤于glusterfs项目，gluster更名为dister，全称为Distribution Cluster，取两个单词的前三个字符与后三个字符构成
* 2017-09-30 完成dister的功能测试、性能测试及数据一致性测试，发布v1.5_stable稳定版本以及[性能测试报告](http://johng.cn/dister-performance-test/)




