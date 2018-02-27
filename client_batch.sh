#!/bin/bash
echo "Reading config and spawning clients..." >&2
source client.config
trap "kill 0" EXIT
for ((i=0;i<$num_client;i++)) {
	python3 client.py $i `expr $startPort + $i` $hostname $replicas $msg_drop &
}
wait
