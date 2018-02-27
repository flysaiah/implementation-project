Implementation of Paxos

Implementation of Paxos to deliver chat log messages in the same order among replicas. View change is triggered when the client's receive times out or the clients detects issues with their request. When a hole in sequence number is detected, the new primary runs Paxos of ___None___ message on the skipped slot.

replica_batch.sh:
	bash script to run batch mode of replicas

client_batch.sh:
	bash script to run batch mode of clients

replicas.config:
	config file for replica_batch.sh

client.config:
	config file for client_batch.sh


REPLICA:
arguments to start a replica:
id: id of replica
port: port number of replica
hostname: hostname of replica
otherReplicas: a list of other replicas' pair of (portnumber, hostname)
skipSlot: the slot to be skipped
messageDrop: the probability of messageDrop (implemented as receive omission)
batchMode: is this in batchMode

example of usage:
	python3 rep.py 0 8000 localhost 8001 localhost 8002 localhost 8003 localhost 8004 localhost 2 0.01 False


CLIENT:
arguments to start a client:
id: id of client
port: port number of client
hostname: hostname of replica
printLog: how often a log will be printed
messageDrop: the probability of messageDrop (implemented as receive omission)
allReplicas: list of infomation of (port, hostname) pair of all the replicas

example of useage:
	python3 client.py 0 9000 localhost 20 0.01 8000 localhost 8001 localhost 8002 localhost 8003 localhost 8004 localhost




