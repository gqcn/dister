#!/bin/bash
######################################
#      用于跨平台交叉编译gluster         #
######################################

#./bin/cbuild.linux_amd64 src/gluster/gluster.go --name=gluster --version=lastest
./bin/cbuild.linux_amd64 src/gluster/gluster.go --name=gluster --version=lastest --arch=amd64 --os=linux
