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
    def __init__(self, id, port, hostname, numberOfAcceptors, otherReplicas, acceptor):
        self.numberOfAcceptors = numberOfAcceptors
        self.numberOfAcceptorResponsesMap = {}
        self.mostRecentLeaderMap = {}
        self.requiredMessageMap = {} # maps from sequence number to required message
        self.messageMap = {}    # maps from sequence number to message
        self.otherReplicas = otherReplicas
        self.id = id
        self.port = port
        self.acceptor = acceptor
        self.hostname = hostname

    def setMessage(self, seqNum, m):
        self.messageMap[seqNum] = m

    def receiveYouAreLeader(self, id, port, hostname, seqNum, currentAcceptedValue, currentLeader, clientID, clientPort, clientHostName, clientSeqNum):
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
            return self.proposeMessage(seqNum, clientID, clientPort, clientHostName, clientSeqNum)

    def proposeMessage(self, seqNum, clientID, clientPort, clientHostName, clientSeqNum):
        print("Proposing message for seqNum " + str(seqNum))
        seqNum = int(seqNum)
        print("Message: ", self.messageMap[seqNum])
        requiredMessage = None
        if seqNum in self.requiredMessageMap:
            requiredMessage = self.requiredMessageMap[seqNum]
        print("Req: ", requiredMessage)
        if requiredMessage is not None and requiredMessage != "None":
            print("update required message")
            # send required message
            resArr = self.acceptor.receiveProposedMessage(self.id, self.port, self.hostname, str(seqNum), requiredMessage, clientID, clientPort, clientHostName, clientSeqNum)
            for replica in self.otherReplicas:
                resArr.append((replica, (':'.join(['3', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, clientHostName, str(clientSeqNum), str(seqNum), requiredMessage]).encode('utf-8'))))
            return resArr
        else:
            # send original message
            resArr = self.acceptor.receiveProposedMessage(self.id, self.port, self.hostname, str(seqNum), self.messageMap[seqNum], clientID, clientPort, clientHostName, clientSeqNum)
            if resArr is None:
                resArr = []
            for replica in self.otherReplicas:
                resArr.append((replica, (':'.join(['3', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, clientHostName, str(clientSeqNum), str(seqNum), self.messageMap[seqNum]]).encode('utf-8'))))
            return resArr

class Acceptor:
    def __init__(self, id, port, hostname, otherReplicas):
        self.currentAcceptedValueMap = {}
        self.currentLeaderMap = {}
        # self.infoToSeq = {}
        self.seqToInfo = {}
        self.infoToSeq = {}
        self.clientPortMap = {}
        self.clientHostMap = {}
        self.otherReplicas = otherReplicas
        # self.learner = learner
        self.id = id
        self.port = port
        self.hostname = hostname

    def receiveIAmLeader(self, id, port, hostname, seqNum, clientID, clientPort, clientHostName, clientSeqNum):
        print("Received I AM LEADER for seqNum " + str(seqNum) + ", clientSeqNum " + str(clientSeqNum))
        # NOTE: Use leader ID, not process ID
        seqNum = int(seqNum)
        self.clientPortMap[int(clientID)] = int(clientPort)
        self.clientHostMap[int(clientID)] = clientHostName
        # print("clientHostName: ", clientHostName)
        if seqNum not in self.currentLeaderMap or self.currentLeaderMap[seqNum] <= int(id):
            currentAcceptedValue = None
            # need to send previous leader to the current leader
            prevLeader = None
            if seqNum in self.currentAcceptedValueMap:
                currentAcceptedValue = self.currentAcceptedValueMap[seqNum]
                prevLeader = self.currentLeaderMap[seqNum]
            info = clientID + ' ' + clientSeqNum
            self.currentLeaderMap[seqNum] = int(id)
            self.seqToInfo[seqNum] = info
            self.infoToSeq[info] = seqNum
            print("Port: ", port)

            return [((port, hostname), (':'.join(['2', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, clientHostName, str(clientSeqNum), str(seqNum), str(currentAcceptedValue) + ';' + str(prevLeader)]).encode('utf-8')))]

    def receiveProposedMessage(self, id, port, hostname, seqNum, message, clientID, clientPort, clientHostName, clientSeqNum):
        print("Received proposed message for seqNum " + str(seqNum))
        # NOTE: We were a little unsure about this rule
        info = str(clientID) + ' ' + str(clientSeqNum)
        seqNum = int(seqNum)
        self.clientPortMap[int(clientID)] = int(clientPort)
        self.clientHostMap[int(clientID)] = clientHostName
        print("Message: ", message)
        currentLeader = None
        if seqNum in self.currentLeaderMap:
            currentLeader = self.currentLeaderMap[seqNum]
        else:
            self.seqToInfo[seqNum] = info
            self.infoToSeq[info] = seqNum
            self.currentLeaderMap[seqNum] = int(id)
        if currentLeader is None or int(id) >= currentLeader:
            return self.acceptMessage(id, seqNum, message, clientID, clientPort, clientHostName, clientSeqNum)

        # NOTE: Do we need to update current leader?
    def acceptMessage(self, id, seqNum, message, clientID, clientPort, clientHostName, clientSeqNum):
        print("Accepting message for seqNum " + str(seqNum))
        # set the current accepted value
        seqNum = int(seqNum)
        self.clientPortMap[int(clientID)] = int(clientPort)
        self.clientHostMap[int(clientID)] = clientHostName
        self.currentAcceptedValueMap[seqNum] = message
        self.currentLeaderMap[seqNum] = int(id)
        info = str(clientID) + ' ' + str(clientSeqNum)
        seqNum = int(seqNum)
        self.seqToInfo[seqNum] = info
        self.infoToSeq[info] = seqNum
        # broadcast accepted value to all learner
        resArr = []
        resArr.append(((self.port, self.hostname), (':'.join(['4', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, clientHostName, str(clientSeqNum), str(seqNum), message + ';' + str(id)]).encode('utf-8'))))
        # if resArr is None:
        #     resArr = []
        for replica in self.otherReplicas:
            resArr.append((replica, (':'.join(['4', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, clientHostName, str(clientSeqNum), str(seqNum), message + ';' + str(id)]).encode('utf-8'))))
        return resArr

class Learner:
    def __init__(self, numberOfAcceptors, clientMap, acceptor, batchMode):
        self.numberOfAcceptors = numberOfAcceptors
        self.acceptanceMap = {}
        self.log = ""
        self.deliveryArray = [None] * 100
        self.currentSeqNum = 0
        self.clientMap = clientMap
        self.acceptor = acceptor
        self.batchMode = batchMode

    def receiveAcceptance(self, id, port, hostname, seqNum, message, leader, clientID, clientPort, clientHostName, clientSeqNum):
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
            return self.deliver_message(message, seqNum, clientID, clientPort, clientHostName, clientSeqNum)

    def deliver_message(self, m, seqNum, clientID, clientPort, clientHostName, clientSeqNum):
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
            port = int(clientPort)
            msg = ':'.join(['5', str(clientSeqNum), "Delivered"]).encode('utf-8')
            resArr.append(((port, clientHostName), msg))
            while self.deliveryArray[self.currentSeqNum] is not None:
                info = self.acceptor.seqToInfo[self.currentSeqNum]
                clientID, clientSeqNum = info.split(' ')
                port = self.acceptor.clientPortMap[int(clientID)]
                host = self.acceptor.clientHostMap[int(clientID)]
                msg = ':'.join(['5', str(clientSeqNum), "Delivered"]).encode('utf-8')
                resArr.append(((port, host), msg))

                self.log += (self.deliveryArray[self.currentSeqNum] + '\n')
                print("Message delivered: ", self.deliveryArray[self.currentSeqNum])
                self.currentSeqNum += 1
            if seqNum > len(self.deliveryArray) / 2 or self.currentSeqNum > len(self.deliveryArray) / 2:
                self.deliveryArray += [None]*100
            if self.batchMode == 'False':
                print("------------LOG------------")
                print(self.log)
                print("-----------ENDLOG-----------")
        return resArr

    def printlog(self):
        print("------------LOG------------")
        print(self.log)
        print("-----------ENDLOG-----------")

class Replica:

    def __init__(self, id, port, hostname, otherReplicas, skipSlot, messageDrop, batchMode):
        self.id = id
        self.clientMap = {}   # Keeps track of most recent client sequence number for each client
        self.hostname = hostname
        self.acceptor = Acceptor(self.id, port, hostname, otherReplicas)
        self.learner = Learner(len(otherReplicas) + 1, self.clientMap, self.acceptor, batchMode)
        self.proposer = Proposer(self.id, port, hostname, len(otherReplicas) + 1, otherReplicas, self.acceptor)
        self.otherReplicas = otherReplicas
        print("Replicas: ", otherReplicas)
        self.port = port
        self.hostname = hostname
        self.queue = Queue()
        self.mainSeqNum = -1
        self.curView = 0
        self.recType6 = False
        self.syncCount = 0
        self.recType6Map = {}
        self.skipSlot = skipSlot   # Integer of seqNum that we will skip
        self.messageDrop = messageDrop   # percentage of messages we will fail to send
        self.batchMode = batchMode

    def runPaxos(self, m, seqNum, clientID, clientPort, clientHostName, clientSeqNum):
        print("Running Paxos")
        seqNum = int(seqNum)
        self.proposer.setMessage(seqNum, m)
        self.acceptor.currentLeaderMap[seqNum] = self.id
        currentAcceptedValue = None
        if seqNum in self.acceptor.currentAcceptedValueMap:
            currentAcceptedValue = self.acceptor.currentAcceptedValueMap[seqNum]
        resArr = self.proposer.receiveYouAreLeader(self.acceptor.id, self.acceptor.port, self.acceptor.hostname, str(seqNum), currentAcceptedValue, self.acceptor.currentLeaderMap[seqNum], clientID, clientPort, clientHostName, clientSeqNum)
        if resArr is None:
            resArr = []
        for replica in self.otherReplicas:
            resArr.append((replica, (':'.join(['1', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, clientHostName, str(clientSeqNum), str(seqNum), '_']).encode('utf-8'))))
            #print("LOOPING")
        return resArr

    # def syncMainSeqNum(self, clientID, clientPort, clientSeqNum, m):
    #     resArr = []
    #     for replica in self.otherReplicas:
    #         resArr.append((replica, (':'.join(['7', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(-1), m]).encode('utf-8'))))
    #     return resArr;

    def syncSeqNumMap(self, start, seq, clientID, clientPort, clientHostName, clientSeqNum):
        # request to sync seqNumMap
        resArr = []
        for replica in self.otherReplicas:
            resArr.append((replica, (':'.join(['9', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, clientHostName, str(clientSeqNum), str(-1), str(start)]).encode('utf-8'))))
        return resArr

    # def respMainSeqNum(self, id, port, clientID, clientPort, clientSeqNum, m):
    #     msg = ':'.join(['8', str(clientID), str(self.id), str(clientPort), str(self.port), str(clientSeqNum), str(-1), str(self.mainSeqNum) + ";" + m]).encode('utf-8')
    #     return [(port, msg)]

    def respSeqNumMap(self, id, port, hostname, seqNum, clientID, clientPort, clientHostName, clientSeqNum):
        # respond corresponding (cid, cseq) of seqNum
        res = {}
        seqNum = int(seqNum)
        for i in range(seqNum, self.mainSeqNum):
            if i in self.acceptor.seqToInfo:
                res[i] = self.acceptor.seqToInfo[i]

        # if(seqNum in self.acceptor.seqToInfo):
        #     msg = self.acceptor.seqToInfo[seqNum]
        tmp = []
        for key in list(res.keys()):
            tmp.append(str(key) + "_" + str(res[key]))
        resString = "|".join(tmp)
        msg = ':'.join(['10', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, clientHostName, str(clientSeqNum), str(seqNum), resString]).encode('utf-8')
        return [((port, hostname), msg)]


    def run(self):

        TCP_IP = self.hostname
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
        #     7. requestPrintLog
        #     9. requestSyncSeqNumMap
        #     10. responseSyncSeqNumMap

        while 1:
            try:
                conn, addr = s.accept()
                print ('Connection address:', addr)
                data = conn.recv(BUFFER_SIZE).decode('utf_8')
                conn.close()
            except KeyboardInterrupt:
                raise
            except Exception as e:
                # print("Exception: ", str(e))
                if self.queue.empty():
                    #print("Timeout, queue is empty")
                    continue
                else:
                    print("Timeout, queue is not empty")
                    while not self.queue.empty():
                        queueMsg = self.queue.get()
                        print("try to send message: ", queueMsg)
                        cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        try:
                            port = int(queueMsg[0][0])
                            hostname = queueMsg[0][1]
                            if port < self.curView + 8000:
                                print("outdated view: ", port)
                                continue

                            if port == -1:
                                print("hole, sending to -1")
                                continue

                            msg = queueMsg[1]
                            #cs.setblocking(0)
                            cs.settimeout(.5)
                            cs.connect((hostname, port))
                            cs.sendall(msg)
                            cs.close()
                        except KeyboardInterrupt:
                            raise
                        except Exception as e:
                            print("Error = " + str(e) + ", moving to next message in queue. Message = ", queueMsg)
                            self.queue.put(queueMsg)
                            cs.close()
                            break
                    continue
            #print("Request received, data = ", data)
            if not data:
                print("Didn't receive any data")
                continue
            r = random.random()
            if r <= self.messageDrop:
                print("dropping message: ", data)
                continue
            host = addr[0]
            print("Data: ", data)
            reqType, clientID, replicaID, clientPort, replicaPort, replicaHostName, clientHostName, clientSeqNum, seqNum, msg = data.split(':')
            seqNum = int(seqNum)
            self.mainSeqNum = max(self.mainSeqNum, seqNum)
            # self.mainSeqNum = max(self.mainSeqNum, seqNum)
            print("Request type: ", reqType)
            print("Message: ", msg)
            # Don't process request is this client sequence number request has already been taken care of
            if clientID in self.clientMap:
                print("ClientMap value: ", str(self.clientMap[clientID]))
                print("Client sequence number: ", clientSeqNum)

            resArray = None
            if clientID not in self.clientMap or (int(clientSeqNum) >= int(self.clientMap[clientID])):
                # First check if client has already sent this message
                prev = -1
                if clientID in self.clientMap:
                    prev = self.clientMap[clientID]
                self.clientMap[clientID] = max(int(clientSeqNum), int(prev))
                if reqType == "0" and (clientID not in self.clientMap or int(clientSeqNum) > int(prev)):
                    self.clientMap[clientID] = clientSeqNum
                    self.mainSeqNum += 1
                    if (self.mainSeqNum == self.skipSlot):
                        self.mainSeqNum += 1
                    resArray = self.runPaxos(msg, str(self.mainSeqNum), clientID, clientPort, clientHostName, clientSeqNum)

                elif reqType == "1":
                    resArray = self.acceptor.receiveIAmLeader(replicaID, replicaPort, replicaHostName, str(seqNum), clientID, clientPort, clientHostName, clientSeqNum)

                elif reqType == "2":
                    currentAcceptedValue, currentLeader = msg.split(";")
                    resArray = self.proposer.receiveYouAreLeader(replicaID, replicaPort, replicaHostName, str(seqNum), currentAcceptedValue, currentLeader, clientID, clientPort, clientHostName, clientSeqNum)

                elif reqType == "3":
                    resArray = self.acceptor.receiveProposedMessage(replicaID, replicaPort, replicaHostName, str(seqNum), msg, clientID, clientPort, clientHostName, clientSeqNum)

                elif reqType == "4":
                    currentAcceptedValue, currentLeader = msg.split(";")
                    resArray = self.learner.receiveAcceptance(replicaID, replicaPort, replicaHostName, str(seqNum), currentAcceptedValue, currentLeader, clientID, clientPort, clientHostName, clientSeqNum)

                elif reqType == "6":
                    # I'm the new primary, run election
                    resArray = []
                    self.curView = self.id
                    if self.recType6 == False:
                        self.recType6 = True
                        resArray = self.syncSeqNumMap(self.learner.currentSeqNum, self.mainSeqNum, clientID, clientPort, clientHostName, clientSeqNum)

                    print("changing view: ", self.curView)
                    # if the (client, clientSeqNum) combination has been seen before
                    if((str(clientID) + " " + str(clientSeqNum)) in self.acceptor.infoToSeq):
                        seqNum = self.acceptor.infoToSeq[str(clientID) + " " + str(clientSeqNum)]
                        print("SEQNUM = " + str(seqNum))
                        print("CURRENTSEQNUM = ", self.learner.currentSeqNum)
                        # this message has been delivered, append response to be sent to client
                        if self.learner.currentSeqNum > seqNum:
                            resArray.append(((clientPort, clientHostName), ':'.join(['5', str(clientSeqNum), "Delivered"]).encode('utf-8')))
                        # this message has not been delivered, run Paxos
                        else:
                            print("WOO RUNNING PAXOS")
                            resArray = resArray + self.runPaxos(msg, str(seqNum), clientID, clientPort, clientHostName, clientSeqNum)
                    else:
                        self.recType6Map[(clientID + " " + clientSeqNum)] = (msg + "|||" + clientPort + "|||" + clientHostName)
                        # self.recType6Map.put((clientID, clientSeqNum, msg))



                    # find the holes and sync SeqNumMap
                    # resArray = resArray + self.syncSeqNumMap(self.learner.currentSeqNum, self.mainSeqNum, clientID, clientPort, clientSeqNum, msg)
                    # # Sync the mainSeqNum of all replicas
                    # resArray = resArray + self.syncMainSeqNum(clientID, clientPort, clientSeqNum, msg)

                elif reqType == "7":
                    # requestPrintLog
                    self.learner.printlog();
                    if self.curView == self.id:
                        resArray = []
                        for replica in self.otherReplicas:
                            resArray.append((replica, (':'.join(['7', str(clientID), str(self.id), str(clientPort), str(self.port), self.hostname, str(clientHostName), str(clientSeqNum), str(-1), str(-1)]).encode('utf-8'))))
                    else:
                        resArray = None
                # elif reqType == "8":
                #     # respSyncMainSeqNum
                #     resArray = None
                #     mainSeq, m = msg.split(';')
                #     if int(mainSeq) > self.mainSeqNum:
                #         resArray = self.syncSeqNumMap(self.mainSeqNum + 1, mainSeq, clientID, clientPort, clientSeqNum, m)
                #         self.mainSeqNum = int(mainSeq)

                elif reqType == "9":
                    # requestSyncSeqNumMap
                    if(int(replicaID) > self.curView):
                        self.curView = int(replicaID)
                        print("changing view: ", self.curView)
                    seqNum = msg
                    resArray = self.respSeqNumMap(replicaID, replicaPort, replicaHostName, seqNum, clientID, clientPort, clientHostName, clientSeqNum)

                elif reqType == "10":
                    resArray = []
                    self.syncCount += 1
                    # respSyncSeqNumMap
                    if msg != '':
                        res = {}
                        tmp = msg.split("|")
                        for pair in tmp:
                            res[pair.split("_")[0]] = pair.split("_")[1]
                        for seqNum in list(res.keys()):
                            info = res[seqNum]
                            if info not in self.acceptor.infoToSeq:
                                self.acceptor.infoToSeq[info] = seqNum
                                self.acceptor.seqToInfo[seqNum] = seqNum
                                self.mainSeqNum = max(self.mainSeqNum, seqNum)
                                if info in self.recType6Map:
                                    clientID, clientSeqNum = info.split(" ")
                                    msg, clientPort, clientHostName = self.recType6Map[info].split("|||")
                                    resArray = resArray + self.runPaxos(msg, str(seqNum), clientID, clientPort, clientHostName, clientSeqNum)
                                    del self.recType6Map[info]
                    if(self.syncCount == len(self.otherReplicas) // 2):
                        # detect the holes and propose None for the holes
                        for i in range(self.learner.currentSeqNum, self.mainSeqNum + 1):
                            if i not in self.acceptor.seqToInfo:
                                print("detect hole for seq num ", i)
                                resArray += self.runPaxos('___None___', str(i), -1, -1, "nothing", -1)
                        # clear the map
                        for info in list(self.recType6Map.keys()):
                            self.mainSeqNum += 1
                            clientID, clientSeqNum = info.split(" ")
                            msg, clientPort, clientHostName = self.recType6Map[info].split("|||")
                            print("main seq before running paxos:", self.mainSeqNum)
                            resArray = resArray + self.runPaxos(msg, str(self.mainSeqNum), clientID, clientPort, clientHostName, clientSeqNum)
                        self.recType6Map = {}




            if resArray is not None:
                for res in resArray:
                    self.queue.put(res)

            # Dequeue and send messages
            while not self.queue.empty():
                queueMsg = self.queue.get()
                print("try to send message: ", queueMsg)
                cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    port = int(queueMsg[0][0])
                    hostname = queueMsg[0][1]
                    if port < self.curView + 8000:
                        print("outdated view: ", port)
                        continue
                    if port == -1:
                        print("hole, sending to -1")
                        continue

                    msg = queueMsg[1]
                    #cs.setblocking(0)
                    cs.settimeout(.5)
                    cs.connect((hostname, port))
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
                except Exception as e:
                    print("Error = " + str(e) + ", moving to next message in queue. Message = ", queueMsg)
                    self.queue.put(queueMsg)
                    cs.close()
                    break


def main():
    argc = len(sys.argv)
    print(sys.argv)
    otherReplicas = []
    for i in range(4, argc - 3, 2):
        otherReplicas.append((int(sys.argv[i]), sys.argv[i+1]))
    # print(otherReplicas)
    replica = Replica(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], otherReplicas, int(sys.argv[argc-3]), float(sys.argv[argc-2]), sys.argv[argc-1])
    replica.run()

main()
