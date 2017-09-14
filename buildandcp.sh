#!/bin/bash
######################################
# 用于将gluster同步到其他节点进行测试的脚本 #
######################################

# 编译
go build src/gluster/gluster.go
## 同步执行文件到测试节点
#sshpass -p 123456 scp -P 22 ./gluster john@192.168.2.147:/home/john/gluster
sshpass -p 123456 scp -P 22 ./gluster john@192.168.2.10:/home/john/gluster
#sshpass -p 123456 scp -P 22 ./gluster john@192.168.2.196:/home/john/gluster
# 同步配置文件到测试节点
#sshpass -p 123456 scp -P 22 ./src/gluster/gluster_server.json john@192.168.2.147:/home/john/gluster.json
sshpass -p 123456 scp -P 22 ./src/gluster/gluster_server.json john@192.168.2.10:/home/john/gluster.json
#sshpass -p 123456 scp -P 22 ./src/gluster/gluster_server.json john@192.168.2.196:/home/john/gluster.json
##删除本地执行文件
rm gluster