#!/bin/bash
######################################
#      用于跨平台交叉编译gluster         #
######################################

#./bin/cbuild.linux_amd64src/gapp/gluster/gluster.go --name=gluster --version=lastest
./bin/cbuild.linux_amd64 src/gluster/gluster.go --name=gluster --version=0.6_beta --arch=386,amd64 --os=linux,windows
