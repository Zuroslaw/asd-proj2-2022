#!/bin/bash

membership=$1
t=$2
pwd=$(pwd)
p2p_port=4000
server_port=5000
initial_membership=$(echo "$membership" | sed -E "s/([^,]+)/\1:$p2p_port/g")
ycsb_nodes=$(echo "$membership" | sed -E "s/([^,]+)/\1:$server_port/g")
client_machine=$3

IFS=,
for m in $membership
do
#  oarsh "$m" "cd $pwd; nohup ./start-local.sh $m $initial_membership $port $pwd &"
  oarsh "$m" "cd $pwd; logfilename=$m nohup java -jar target/asdProj2.jar -conf config.properties interface=bond0 p2p_port=$p2p_port initial_membership=$initial_membership > /dev/null 2>&1 &"
done

sleep 10

payload=100

oarsh "$client_machine" "cd $pwd/client; ./exec.sh $t $payload $ycsb_nodes 50 50 > ./results-$t.log 2>&1"


#172.30.10.117,172.30.10.118
#172.30.10.101,172.30.10.102,172.30.10.103