#!/bin/bash
echo "Reading config..." >&2
source replicas.config
trap "kill 0" EXIT
num_replica=`expr 2 \* $num_failure + 1`
# echo $num_replica
for ((i=0;i<$num_replica;i++)) {
	otherRep=""
	for ((j=0;j<$num_replica;j++)) {
		if(($j != $i))
			then otherRep="$otherRep `expr $startPort + $j`"
		fi
	}
	# echo $otherRep
	python3 rep.py $i `expr $startPort + $i` $otherRep $skipSlot $messageDrop &
}
wait
