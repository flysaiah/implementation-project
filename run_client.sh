#!/bin/bash
echo "Starting clients..." >&2
source batch_mode.config
trap "kill 0" EXIT

for ((i=0;i<$num_client;i++)) {
	python3 client.py $i `expr $startClientPort + $i` $startPort $printLog $hostname &
}

wait
