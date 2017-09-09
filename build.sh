#!/bin/sh

#SYSTEM=`uname -s`
#if [ $SYSTEM != "Darwin" ];then
    # 如果需要使用go1.7 ，请打开下面两行注释：
#    export GOROOT=/usr/local/go1.7.5
#    export PATH=$GOROOT/bin:$PATH
#fi

workspace=$(cd $(dirname $0) && pwd -P)
#godepsrc=$workspace/Godeps/_workspace
export GOPATH=$workspace

make 
ret=$?
if [ $ret -ne 0 ];then
    echo "===== build failure ====="
    exit $ret
else
    echo "===== build successfully! ====="
fi

exit



