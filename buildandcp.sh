#!/bin/bash
######################################
# 用于将dister同步到其他节点进行测试的脚本 #
######################################

# 编译
go build src/dister/dister.go
## 同步执行文件到测试节点
sshpass -p 123456 scp -P 22 ./dister john@192.168.2.62:/home/john/dister
#sshpass -p 123456 scp -P 22 ./dister john@192.168.2.121:/home/john/dister
#sshpass -p 123456 scp -P 22 ./dister john@192.168.2.114:/home/john/dister
# 同步配置文件到测试节点
#sshpass -p 123456 scp -P 22 ./src/dister/dister_server.json john@192.168.2.62:/home/john/dister.json
#sshpass -p 123456 scp -P 22 ./src/dister/dister_server.json john@192.168.2.121:/home/john/dister.json
#sshpass -p 123456 scp -P 22 ./src/dister/dister_server.json john@192.168.2.114:/home/john/dister.json
##删除本地执行文件
rm dister