from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import socket
import time
import random
import sys
from queue import Queue


# Restrict to a particular path.
# class RequestHandler(SimpleXMLRPCRequestHandler):
#     rpc_paths = ('/RPC2',)

class Proposer:
    def __init__(self, id, port, numberOfAcceptors, otherReplicas, acceptor):
        self.numberOfAcceptors = numberOfAcceptors
        self.numberOfAcceptorResponsesMap = {}
        self.mostRecentLeaderMap = {}
        self.requiredMessageMap = {} # maps from sequence number to required message
        self.messageMap = {}    # maps from sequence number to message
        self.otherReplicas = otherReplicas
        self.id = id
        self.port = port
        self.acceptor = acceptor

    def setMessage(self, seqNum, m):
        self.messageMap[seqNum] = m

    def receiveYouAreLeader(self, id, port, seqNum, currentAcceptedValue, currentLeader, clientID, clientPort, clientSeqNum):
        print("Received YOU ARE LEADER for seqNum " + str(seqNum))
        seqNum = int(seqNum)
        if seqNum in self.numberOfAcceptorResponsesMap:
            self.numberOfAcceptorResponsesMap[seqNum] += 1
        else:
            self.numberOfAcceptorResponsesMap[seqNum] = 1
        if currentLeader is not None and currentLeader != "None" and (seqNum not in self.mostRecentLeaderMap or int(currentLeader) >= int(self.mostRecentLeaderMap[seqNum])):
            self.mostRecentLeaderMap[seqNum] = currentLeader
            self.requiredMessageMap[seqNum] = currentAcceptedValue

        if self.numberOfAcceptorResponsesMap[seqNum] == (self.numberOfAcceptors // 2 + 1):
            return self.proposeMessage(seqNum, clientID, clientPort, clientSeqNum)

    def proposeMessage(self, seqNum, clientID, clientPort, clientSeqNum):
        print("Proposing message for seqNum " + str(seqNum))
        seqNum = int(seqNum)
        print("Message: ", self.messageMap[seqNum])
        requiredMessage = None
        if seqNum in self.requiredMessageMap:
            requiredMessage = self.requiredMessageMap[seqNum]
        print("Req: ", requiredMessage)
        if requiredMessage is not None and requiredMessage != "None":
            print("This shouldn't be happening")
            # send required message
            resArr = self.acceptor.receiveProposedMessage(self.id, self.port, str(seqNum), requiredMessage)
            for replica in self.otherReplicas:
                resArr.append((replica, (':'.join(['3', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(seqNum), requiredMessage]).encode('utf-8'))))
            return resArr
        else:
            # send original message
            resArr = self.acceptor.receiveProposedMessage(self.id, self.port, str(seqNum), self.messageMap[seqNum], clientID, clientPort, clientSeqNum)
            if resArr is None:
                resArr = []
            for replica in self.otherReplicas:
                resArr.append((replica, (':'.join(['3', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(seqNum), self.messageMap[seqNum]]).encode('utf-8'))))
            return resArr

class Acceptor:
    def __init__(self, id, port, learner, otherReplicas):
        self.currentAcceptedValueMap = {}
        self.currentLeaderMap = {}
        # self.infoToSeq = {}
        self.seqToInfo = {}
        self.infoToSeq = {}
        self.otherReplicas = otherReplicas
        self.learner = learner
        self.id = id
        self.port = port

    def receiveIAmLeader(self, id, port, seqNum, clientID, clientPort, clientSeqNum):
        print("Received I AM LEADER for seqNum " + str(seqNum))
        # NOTE: Use leader ID, not process ID
        seqNum = int(seqNum)
        if seqNum not in self.currentLeaderMap or self.currentLeaderMap[seqNum] <= id:
        # if self.currentLeader is None or self.currentLeader == "None" or self.currentLeader <= id:
            info = clientID + ' ' + clientSeqNum
            self.currentLeaderMap[seqNum] = id
            self.seqToInfo[seqNum] = info
            self.infoToSeq[info] = seqNum
            print("Port: ", port)
            currentAcceptedValue = None
            if seqNum in self.currentAcceptedValueMap:
                currentAcceptedValue = self.currentAcceptedValueMap[seqNum]
            return [(port, (':'.join(['2', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(seqNum), str(currentAcceptedValue) + ';' + str(self.currentLeaderMap[seqNum])]).encode('utf-8')))]

    def receiveProposedMessage(self, id, port, seqNum, message, clientID, clientPort, clientSeqNum):
        print("Received proposed message for seqNum " + str(seqNum))
        # NOTE: We were a little unsure about this rule
        seqNum = int(seqNum)
        print("Message: ", message)
        currentLeader = None
        if seqNum in self.currentLeaderMap:
            currentLeader = self.currentLeaderMap[seqNum]
        else:
            self.seqToInfo[seqNum] = info
            self.infoToSeq[info] = seqNum
        if currentLeader is None or id >= currentLeader:
            return self.acceptMessage(id, seqNum, message, clientID, clientPort, clientSeqNum)

        # NOTE: Do we need to update current leader?
    def acceptMessage(self, id, seqNum, message, clientID, clientPort, clientSeqNum):
        print("Accepting message for seqNum " + str(seqNum))
        # set the current accepted value
        seqNum = int(seqNum)
        self.currentAcceptedValueMap[seqNum] = message
        self.currentLeaderMap[seqNum] = id
        # broadcast accepted value to all learner
        resArr = self.learner.receiveAcceptance(self.id, self.port, seqNum, message, self.currentLeaderMap[seqNum], clientID, clientPort, clientSeqNum)
        if resArr is None:
            resArr = []
        for replica in self.otherReplicas:
            resArr.append((replica, (':'.join(['4', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(seqNum), str(self.currentAcceptedValueMap[seqNum]) + ';' + str(self.currentLeaderMap[seqNum])]).encode('utf-8'))))
        return resArr

class Learner:
    def __init__(self, numberOfAcceptors, clientMap):
        self.numberOfAcceptors = numberOfAcceptors
        self.acceptanceMap = {}
        self.log = ""
        self.deliveryArray = [None] * 100
        self.currentSeqNum = 0
        self.clientMap = clientMap

    def receiveAcceptance(self, id, port, seqNum, message, leader, clientID, clientPort, clientSeqNum):
        print("Learner received acceptance for seqNum " + str(seqNum))
        seqNum = int(seqNum)
        leader = int(leader)
        if (leader, seqNum, message) in self.acceptanceMap:
            print("Count: ", self.acceptanceMap[(leader, seqNum, message)])
        else:
            print("Count: 0")
        if (leader, seqNum, message) not in self.acceptanceMap:
            self.acceptanceMap[(leader, seqNum, message)] = 1
        else:
            self.acceptanceMap[(leader, seqNum, message)] += 1
        print("-----Post-update-----")
        if (leader, seqNum, message) in self.acceptanceMap:
            print("Count: ", self.acceptanceMap[(leader, seqNum, message)])
        else:
            print("Count: 0")

        if self.acceptanceMap[(leader, seqNum, message)] == self.numberOfAcceptors // 2 + 1:
            return self.deliver_message(message, seqNum, clientID, clientPort, clientSeqNum)

    def deliver_message(self, m, seqNum, clientID, clientPort, clientSeqNum):
        seqNum = int(seqNum)
        self.deliveryArray[seqNum] = m
        print("Delivering message")
        print("seqNum: ", seqNum)
        print("currentSeqNum: ", self.currentSeqNum)
        resArr = []
        if (int(seqNum) == self.currentSeqNum):
            self.log += (m + '\n')
            print("Message delivered: ", m)
            self.currentSeqNum += 1
            self.clientMap[clientID] = clientSeqNum
            while self.deliveryArray[self.currentSeqNum] is not None:
                self.log += (self.deliveryArray[self.currentSeqNum] + '\n')
                print("Message delivered: ", self.deliveryArray[self.currentSeqNum])
                self.currentSeqNum += 1
            if self.currentSeqNum > len(self.deliveryArray) / 2:
                self.deliveryArray += [None]*100
        print("------------LOG------------")
        print(self.log)
        print("-----------ENDLOG-----------")

        port = int(clientPort)
        msg = ':'.join(['5', str(clientSeqNum), "Delivered"]).encode('utf-8')

        return [(port, msg)]

class Replica:

    def __init__(self, id, port, otherReplicas):
        self.id = id
        self.clientMap = {}   # Keeps track of most recent client sequence number for each client
        self.learner = Learner(len(otherReplicas) + 1, self.clientMap)
        self.acceptor = Acceptor(self.id, port, self.learner, otherReplicas)
        self.proposer = Proposer(self.id, port, len(otherReplicas) + 1, otherReplicas, self.acceptor)
        self.otherReplicas = otherReplicas
        print("Replicas: ", otherReplicas)
        self.port = port
        self.queue = Queue()
        self.mainSeqNum = -1

    def runPaxos(self, m, seqNum, clientID, clientPort, clientSeqNum):
        print("Running Paxos")
        seqNum = int(seqNum)
        self.proposer.setMessage(seqNum, m)
        self.acceptor.currentLeaderMap[seqNum] = self.id
        currentAcceptedValue = None
        if seqNum in self.acceptor.currentAcceptedValueMap:
            currentAcceptedValue = self.acceptor.currentAcceptedValueMap[seqNum]
        resArr = self.proposer.receiveYouAreLeader(self.acceptor.id, self.acceptor.port, str(seqNum), currentAcceptedValue, self.acceptor.currentLeaderMap[seqNum], clientID, clientPort, clientSeqNum)
        if resArr is None:
            resArr = []
        for replica in self.otherReplicas:
            resArr.append((replica, (':'.join(['1', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(seqNum), '_']).encode('utf-8'))))
            #print("LOOPING")
        return resArr

    def syncMainSeqNum(self, clientID, clientPort, clientSeqNum, m):
        resArr = []
        for replica in otherReplicas:
            resArr.append((replica, (':'.join(['7', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(-1), m]).encode('utf-8'))))
        return resArr;

    def syncSeqNumMap(self, start, end, clientID, clientPort, clientSeqNum, m):
        resArr = []
        for i in range(start, end + 1):
            if i not in acceptor.seqToInfo:
                for replica in otherReplicas:
                    resArr.append((replica, (':'.join(['9', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(-1), str(i) + ";" + m]).encode('utf-8'))))
        return resArr

    def respMainSeqNum(self, id, port, seqNum, clientID, clientPort, clientSeqNum, m):
        msg = ':'.join(['8', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(-1), str(self.mainSeqNum) + ";" + m]).encode('utf-8')
        return [(port, msg)]

    def respSeqNumMap(self, id, port, seqNum, clientID, clientPort, clientSeqNum, m):
        # respond corresponding (cid, cseq) of seqNum
        msg = None
        if(seqNum in self.acceptor.seqToInfo):
            msg = self.acceptor.seqToInfo[seqNum] + ';' + m
        msg = ':'.join(['10', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(seqNum), msg]).encode('utf-8')
        return [(port, msg)]


    def run(self):

        TCP_IP = 'localhost'
        TCP_PORT = self.port
        BUFFER_SIZE = 4096

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((TCP_IP, TCP_PORT))
        s.settimeout(2)
        s.listen(1)

        # reqType of request received:
        #     0. client message
        #     1. IamLeader
        #     2. YouAreLeader
        #     3. proposedValue
        #     4. accceptedValue
        #     5. client message delivered
        #     6. client message resent
        #     7. requestSyncMainSeqNum
        #     8, responseSyncMainSeqNum
        #     9. requestSyncSeqNumMap
        #     10. responseSyncSeqNumMap

        while 1:
            try:
                conn, addr = s.accept()
                print ('Connection address:', addr)
                data = conn.recv(BUFFER_SIZE).decode('utf_8')
                conn.close()
            except:
                if self.queue.empty():
                    #print("Timeout, queue is empty")
                    continue
                else:
                    print("Timeout, queue is not empty")
                    while not self.queue.empty():
                        queueMsg = self.queue.get()
                        cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        try:
                            port = int(queueMsg[0])
                            msg = queueMsg[1]
                            cs.setblocking(0)
                            cs.connect(('localhost', port))
                            cs.sendall(msg)
                            cs.close()
                        except KeyboardInterrupt:
                            raise
                        except:
                            print("Error, moving to next message in queue. Message = ", queueMsg)
                            self.queue.put(queueMsg)
                            cs.close()
                            break
            #print("Request received, data = ", data)
            if not data:
                print("Didn't receive any data")
                continue
            host = addr[0]
            print("Data: ", data)
            reqType, clientID, replicaID, clientPort, replicaPort, clientSeqNum, seqNum, msg = data.split(':')
            print("Request type: ", reqType)
            print("Message: ", msg)
            # Don't process request is this client sequence number request has already been taken care of
            if clientID in self.clientMap:
                print("ClientMap value: ", str(self.clientMap[clientID]))
                print("Client sequence number: ", clientSeqNum)

            resArray = None
            if clientID not in self.clientMap or int(clientSeqNum) >= int(self.clientMap[clientID]):
                if reqType == "0":
                    self.mainSeqNum += 1
                    # First check if client has already sent this message
                    resArray = self.runPaxos(msg, str(self.mainSeqNum), clientID, clientPort, clientSeqNum)
                    # if resArray is not None:
                    #     for res in resArray:
                    #         self.queue.put(res)
                elif reqType == "1":
                    resArray = self.acceptor.receiveIAmLeader(replicaID, replicaPort, str(seqNum), clientID, clientPort, clientSeqNum)
                    # if resArray is not None:
                    #     for res in resArray:
                    #         self.queue.put(res)
                elif reqType == "2":
                    currentAcceptedValue, currentLeader = msg.split(";")
                    resArray = self.proposer.receiveYouAreLeader(replicaID, replicaPort, str(seqNum), currentAcceptedValue, currentLeader, clientID, clientPort, clientSeqNum)
                    # if resArray is not None:
                    #     for res in resArray:
                    #         self.queue.put(res)
                elif reqType == "3":
                    resArray = self.acceptor.receiveProposedMessage(replicaID, replicaPort, str(seqNum), msg, clientID, clientPort, clientSeqNum)
                    # if resArray is not None:
                    #     for res in resArray:
                    #         self.queue.put(res)
                elif reqType == "4":
                    currentAcceptedValue, currentLeader = msg.split(";")
                    resArray = self.learner.receiveAcceptance(replicaID, replicaPort, str(seqNum), currentAcceptedValue, currentLeader, clientID, clientPort, clientSeqNum)
                    # if resArray is not None:
                    #     for res in resArray:
                    #         self.queue.put(res)
                
                elif reqType == "6":
                    # I'm the new primary, run election
                    resArray = []
                    # if the (client, clientSeqNum) combination has been seen before
                    if((str(clientID) + " " + str(clientSeqNum)) in self.acceptor.infoToSeq):
                        seqNum = self.acceptor.infoToSeq[str(clientID) + " " + str(clientSeqNum)]
                        # this message has been delivered, append response to be sent to client
                        if self.learner.currentSeqNum >= seqNum:
                            resArray.append((clientPort, ':'.join(['5', str(clientSeqNum), "Delivered"]).encode('utf-8')))
                        # this message has not been delivered, run Paxos
                        else:
                            resArray.append(self.runPaxos(msg, str(seqNum), clientID, clientPort, clientSeqNum))
                    # find the holes and sync SeqNumMap
                    resArray.append(self.syncSeqNumMap(self.learner.currentSeqNum, self.mainSeqNum, clientID, clientPort, clientSeqNum, m))
                    # Sync the mainSeqNum of all replicas
                    resArray.append(self.syncMainSeqNum(clientID, clientPort, clientSeqNum, m))
               
                elif reqType == "7":
                    # requestSyncMainSeqNum
                    resArray = self.respMainSeqNum(replicaID, msg, clientID, clientPort, clientSeqNum)

                elif reqType == "8":
                    # respSyncMainSeqNum
                    resArray = None
                    mainSeq, m = msg.split(';')
                    if mainSeq > self.mainSeqNum:
                        resArray = self.syncSeqNumMap(mainSeqNum + 1, mainSeq, clientID, clientPort, clientSeqNum, m)
                        mainSeqNum = mainSeq

                elif reqType == "9":
                    # requestSyncSeqNumMap
                    seqNum, m = msg.split(';')
                    resArray = self.respSeqNumMap(replicaID, port, seqNum, clientID, clientPort, clientSeqNum, m)

                elif reqType == "10":
                    # respSyncSeqNumMap
                    if msg != 'None':
                        # if this is the first time we see this (cid, cseq) pair
                        if info not in self.acceptor.infoToSeq:
                            info, m = msg.split(';')
                            cid, cseq = info.split(' ')
                            # if this is the pair we're looking for
                            if cid == clientID and cseq == clientSeqNum:
                                self.acceptor.infoToSeq[info] = seqNum
                                self.acceptor.seqToInfo[seqNum] = seqNum
                                self.runPaxos(m, str(seqNum), clientID, clientPort, clientSeqNum)
                            else:
                                self.acceptor.infoToSeq[info] = seqNum
                                self.acceptor.seqToInfo[seqNum] = seqNum



            
            if resArray is not None:
                for res in resArray:
                    self.queue.put(res)

            # Dequeue and send messages
            while not self.queue.empty():
                queueMsg = self.queue.get()
                cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    port = int(queueMsg[0])
                    msg = queueMsg[1]
                    cs.setblocking(0)
                    cs.settimeout(0.5)
                    cs.connect(('localhost', port))
                    cs.sendall(msg)
                    cs.close()
                # except socket.timeout:
                #     print("Socket timed out, moving to next message in queue. Message = ", queueMsg)
                #     self.queue.put(queueMsg)
                #     break
                # except ConnectionResetError:
                #     print("Connection Reset Error, moving to next message in queue. Message = ", queueMsg)
                #     self.queue.put(queueMsg)
                #     break
                # except BrokenPipeError:
                #     print("Broken pipe error, moving to next message in queue. Message = ", queueMsg)
                #     self.queue.put(queueMsg)
                #     break
                # except OSError:
                #     print("OS error, moving to next message in queue. Message = ", queueMsg)
                #     self.queue.put(queueMsg)
                #     break
                # except TimeoutError:
                #     print("Timeout error, moving to next message in queue. Message = ", queueMsg)
                #     self.queue.put(queueMsg)
                #     break
                except KeyboardInterrupt:
                    raise
                except:
                    print("Error, moving to next message in queue. Message = ", queueMsg)
                    self.queue.put(queueMsg)
                    cs.close()
                    break


def main():
    replica = Replica(int(sys.argv[1]), int(sys.argv[2]), [int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])])
    replica.run()

main()
