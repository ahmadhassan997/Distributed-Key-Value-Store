#!/bin/bash

if [ -z $GOPATH ]; then
    echo "FAIL: GOPATH environment variable is not set"
    exit 1
fi

if [ -n "$(go version | grep 'darwin/amd64')" ]; then    
    GOOS="darwin_amd64"
elif [ -n "$(go version | grep 'linux/amd64')" ]; then
    GOOS="linux_amd64"
else
    echo "FAIL: only 64-bit Mac OS X and Linux operating systems are supported"
    exit 1
fi

# Build the student's paxos node implementation.
# Exit immediately if there was a compile-time error.
go install paxosapp/runners/prunner
if [ $? -ne 0 ]; then
  echo "FAIL: code does not compile"
  exit $?
fi

# Pick random ports between [10000, 20000).
NODE_PORT0=$(((RANDOM % 10000) + 10000))
NODE_PORT1=$(((RANDOM % 10000) + 10000))
NODE_PORT2=$(((RANDOM % 10000) + 10000))
NODE_PORT3=$(((RANDOM % 10000) + 10000))
NODE_PORT4=$(((RANDOM % 10000) + 10000))
TESTER_PORT=$(((RANDOM % 10000) + 10000))
PROXY_PORT=$(((RANDOM & 10000) + 10000))
PAXOS_TEST=$GOPATH/bin/paxostest_pack3
PAXOS_NODE=$GOPATH/bin/prunner
ALL_PORTS="${NODE_PORT0},${NODE_PORT1},${NODE_PORT2},${NODE_PORT3},${NODE_PORT4}"
echo All ports: ${ALL_PORTS}
echo Proxy port: ${PROXY_PORT}
##################################################

# Start paxos node.
${PAXOS_NODE} -myport=${NODE_PORT0} -ports=${ALL_PORTS} -N=5 -id=0 -pxport=${PROXY_PORT} -proxy=0 -retries=10 2> /dev/null &
PAXOS_NODE_PID0=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT1} -ports=${ALL_PORTS} -N=5 -id=1 -pxport=${PROXY_PORT} -proxy=0 -retries=10 2> /dev/null &
PAXOS_NODE_PID1=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT2} -ports=${ALL_PORTS} -N=5 -id=2 -pxport=${PROXY_PORT} -proxy=0 -retries=10 2> /dev/null &
PAXOS_NODE_PID2=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT3} -ports=${ALL_PORTS} -N=5 -id=3 -pxport=${PROXY_PORT} -proxy=0 -retries=10 2> /dev/null &
PAXOS_NODE_PID3=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT4} -ports=${ALL_PORTS} -N=5 -id=4 -pxport=${PROXY_PORT} -proxy=0 -retries=10 2> /dev/null &
PAXOS_NODE_PID4=$!
sleep 1

# Start paxostest.
${PAXOS_TEST} -port=${TESTER_PORT} -paxosports=${ALL_PORTS} -N=5 -nodeport=${NODE_PORT0} -pxport=${PROXY_PORT} -pidkill=${PAXOS_NODE_PID1} -initwait=10 -repwait=20 -repretries=10

# Kill paxos node.
kill -9 ${PAXOS_NODE_PID0}
kill -9 ${PAXOS_NODE_PID2}
kill -9 ${PAXOS_NODE_PID3}
kill -9 ${PAXOS_NODE_PID4}
wait ${PAXOS_NODE_PID0} 2> /dev/null
wait ${PAXOS_NODE_PID2} 2> /dev/null
wait ${PAXOS_NODE_PID3} 2> /dev/null
wait ${PAXOS_NODE_PID4} 2> /dev/null
