#!/bin/bash
echo "Reading config..." >&2
source replicas.config
for ((i=0;i<$num_replica;i++)) {
	otherRep=""
	for ((j=0;j<$num_replica;j++)) {
		if(($j != $i))
			then otherRep="$otherRep `expr $startPort + $j`"
		fi
	}
	# echo $otherRep
	if(($i == 0))
		then python3 rep.py $i `expr $startPort + $i` $otherRep $skipSlot $messageDrop &
	# else
	# 	& python3 rep.py $i `expr $startPort + $i` $otherRep $skipSlot $messageDrop
	fi
}
