Implementation of Paxos

Implementation of Paxos to deliver chat log messages in the same order among replicas. View change is triggered when the client's receive times out. When a hole in sequence number is detected, the new primary runs Paxos of ___None___ message on the skipped slot.

arguments to start a replica:
id: id of replica
port: port number of replica
hostname: hostname of replica
otherReplicas: a list of other replicas' pair of (portnumber, hostname)
skipSlot: the slot to be skipped
messageDrop: the probability of messageDrop
batchMode: is this in batchMode

example of usage:
	python3 rep.py 0 8000 localhost 8001 localhost 8002 localhost 8003 localhost 8004 localhost 2 0.01 False

format of request sent by client:




