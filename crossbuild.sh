#!/bin/bash
######################################
#      用于跨平台交叉编译dister         #
######################################

./bin/cbuild.linux_amd64 src/dister/dister.go --name=dister --version=lastest
#./bin/cbuild.linux_amd64 src/dister/dister.go --name=dister --version=lastest --arch=amd64 --os=linux
