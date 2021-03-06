#!/bin/bash
echo "Reading config and spawning replicas..." >&2
source batch_mode.config
trap "kill 0" EXIT
num_replica=`expr 2 \* $num_failure + 1`
# echo $num_replica
for ((i=0;i<$num_replica;i++)) {
	otherRep=""
	for ((j=0;j<$num_replica;j++)) {
		if(($j != $i))
			then otherRep="$otherRep `expr $startPort + $j` localhost"
		fi
	}
	# echo $otherRep
	python3 rep.py $i `expr $startPort + $i` $hostname $otherRep $skipSlot $messageDrop True &
}
wait
