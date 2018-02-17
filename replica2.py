from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import socket

# Restrict to a particular path.
# class RequestHandler(SimpleXMLRPCRequestHandler):
#     rpc_paths = ('/RPC2',)

class Proposer:
    def __init__(self, id, port, numberOfAcceptors, otherReplicas, acceptor):
        self.numberOfAcceptors = numberOfAcceptors
        self.numberOfAcceptorResponsesMap = {}
        self.mostRecentLeaderMap = {}
        self.requiredMessageMap = {}
        self.messageMap = {}
        self.otherReplicas = otherReplicas
        self.id = id
        self.port = port
        self.acceptor = acceptor

    def setMessage(self, seqNum, m):
        self.messageMap[seqNum] = m

    def receiveYouAreLeader(self, id, port, seqNum, currentAcceptedValue, currentLeader):
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
            self.proposeMessage(seqNum)

    def proposeMessage(self, seqNum):
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
            self.acceptor.receiveProposedMessage(self.id, self.port, str(seqNum), requiredMessage)
            for replica in self.otherReplicas:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', replica))
                s.sendall((':'.join(['3', str(self.id), str(self.port), str(seqNum), requiredMessage]).encode('utf-8')))
                s.close()
        else:
            # send original message
            self.acceptor.receiveProposedMessage(self.id, self.port, str(seqNum), self.messageMap[seqNum])
            for replica in self.otherReplicas:
                print("About to propose message for seqNum: " + str(seqNum) + " to replica w/port " + str(replica))
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', replica))
                s.sendall((':'.join(['3', str(self.id), str(self.port), str(seqNum), self.messageMap[seqNum]]).encode('utf-8')))
                s.close()
                print("Proposed")

class Acceptor:
    def __init__(self, id, port, learner, otherReplicas):
        self.currentAcceptedValueMap = {}
        self.currentLeaderMap = {}
        self.otherReplicas = otherReplicas
        self.learner = learner
        self.id = id
        self.port = port

    def receiveIAmLeader(self, id, port, seqNum):
        print("Received I AM LEADER for seqNum " + str(seqNum))
        # NOTE: Use leader ID, not process ID
        seqNum = int(seqNum)
        if seqNum not in self.currentLeaderMap or self.currentLeaderMap[seqNum] <= id:
        # if self.currentLeader is None or self.currentLeader == "None" or self.currentLeader <= id:
            self.currentLeaderMap[seqNum] = id
            print("Port: ", port)
            currentAcceptedValue = None
            if seqNum in self.currentAcceptedValueMap:
                currentAcceptedValue = self.currentAcceptedValueMap[seqNum]
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', int(port)))
            s.sendall(':'.join(['2', str(self.id), str(self.port), str(seqNum), str(currentAcceptedValue) + ';' + str(self.currentLeaderMap[seqNum])]).encode('utf-8'))
            s.close()

    def receiveProposedMessage(self, id, port, seqNum, message):
        print("Received proposed message for seqNum " + str(seqNum))
        # NOTE: We were a little unsure about this rule
        seqNum = int(seqNum)
        print("Message: ", message)
        currentLeader = None
        if seqNum in self.currentLeaderMap:
            currentLeader = self.currentLeaderMap[seqNum]
        if currentLeader is None or id >= currentLeader:
            self.acceptMessage(id, seqNum, message)

        # NOTE: Do we need to update current leader?
    def acceptMessage(self, id, seqNum, message):
        print("Accepting message for seqNum " + str(seqNum))
        # set the current accepted value
        seqNum = int(seqNum)
        self.currentAcceptedValueMap[seqNum] = message
        self.currentLeaderMap[seqNum] = id
        # broadcast accepted value to all learner
        self.learner.receiveAcceptance(self.id, self.port, seqNum, message, self.currentLeaderMap[seqNum])
        for replica in self.otherReplicas:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # print("Replica: ", replica)
            s.connect(('localhost', replica))
            # should send the id of leader here
            s.sendall(':'.join(['4', str(self.id), str(self.port), str(seqNum), str(self.currentAcceptedValueMap[seqNum]) + ';' + str(self.currentLeaderMap[seqNum])]).encode('utf-8'))
            s.close()

class Learner:
    def __init__(self, numberOfAcceptors):
        self.numberOfAcceptors = numberOfAcceptors
        self.acceptanceMap = {}
        self.log = ""
        self.deliveryArray = [None] * 100
        self.currentSeqNum = 0

    def receiveAcceptance(self, id, port, seqNum, message, leader):
        print("Learner received acceptance for seqNum " + str(seqNum))
        seqNum = int(seqNum)
        leader = int(leader)
        # print("Leader: ", leader)
        # print("Type of leader: ", type(leader))
        # print("seqNum: ", seqNum)
        # print("Type of seqNum: ", type(seqNum))
        # print("message: ", message)
        # print("Type of message: ", type(message))
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
            self.deliver_message(message, seqNum)

    def deliver_message(self, m, seqNum):
        seqNum = int(seqNum)
        self.deliveryArray[seqNum] = m
        print("Delivering message")
        print("seqNum: ", seqNum)
        print("currentSeqNum: ", self.currentSeqNum)
        if (int(seqNum) == self.currentSeqNum):
            self.log += (m + '\n')
            print("Message delivered: ", m)
            self.currentSeqNum += 1
            while self.deliveryArray[self.currentSeqNum] is not None:
                self.log += (deliveryArray[self.currentSeqNum] + '\n')
                print("Message delivered: ", self.deliveryArray[self.currentSeqNum])
                self.currentSeqNum += 1
            if self.currentSeqNum > len(self.deliveryArray) / 2:
                self.deliveryArray += [None]*100
        print("------------LOG------------")
        print(self.log)
        print("-----------ENDLOG-----------")

class Replica:

    def __init__(self, id, port, otherReplicas):
        self.id = id
        self.learner = Learner(len(otherReplicas) + 1)
        self.acceptor = Acceptor(self.id, port, self.learner, otherReplicas)
        self.proposer = Proposer(self.id, port, len(otherReplicas) + 1, otherReplicas, self.acceptor)
        self.otherReplicas = otherReplicas
        print("Replicas: ", otherReplicas)
        self.port = port

    def runPaxos(self, m, seqNum):
        print("Running Paxos")
        seqNum = int(seqNum)
        self.proposer.setMessage(seqNum, m)
        self.acceptor.currentLeaderMap[seqNum] = self.id
        currentAcceptedValue = None
        if seqNum in self.acceptor.currentAcceptedValueMap:
            currentAcceptedValue = self.acceptor.currentAcceptedValueMap[seqNum]
        self.proposer.receiveYouAreLeader(self.acceptor.id, self.acceptor.port, str(seqNum), currentAcceptedValue, self.acceptor.currentLeaderMap[seqNum])
        for replica in self.otherReplicas:
            #print("LOOPING")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # NOTE: Will have to change this to include other hostnames
            s.connect(('localhost', replica))
            # Send I AM LEADER message
            s.sendall(':'.join(['1', str(self.id), str(self.port), str(seqNum), '_']).encode('utf-8'))
            s.close()
            #print("ENDLOOP 1")


    def run(self):

        TCP_IP = 'localhost'
        TCP_PORT = self.port
        BUFFER_SIZE = 4096

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((TCP_IP, TCP_PORT))
        s.listen(1)

        mainSeqNum = -1

        # reqType of request received:
        #     0. client message
        #     1. IamLeader
        #     2. YouAreLeader
        #     3. proposedValue
        #     4. accceptedValue

        while 1:
            conn, addr = s.accept()
            print ('Connection address:', addr)
            data = conn.recv(BUFFER_SIZE).decode('utf_8')
            conn.close()
            #print("Request received, data = ", data)
            if not data: break
            host = addr[0]
            port = addr[1]
            print("Data: ", data)
            reqType, clientID, port, seqNum, msg = data.split(':')
            print("Request type: ", reqType)
            print("Message: ", msg)
            if reqType == "0":
                mainSeqNum += 1
                self.runPaxos(msg, str(mainSeqNum))
            elif reqType == "1":
                self.acceptor.receiveIAmLeader(clientID, port, str(seqNum))
            elif reqType == "2":
                currentAcceptedValue, currentLeader = msg.split(";")
                self.proposer.receiveYouAreLeader(clientID, port, str(seqNum), currentAcceptedValue, currentLeader)
            elif reqType == "3":
                self.acceptor.receiveProposedMessage(clientID, port, str(seqNum), msg)
            elif reqType == "4":
                currentAcceptedValue, currentLeader = msg.split(";")
                self.learner.receiveAcceptance(clientID, port, str(seqNum), currentAcceptedValue, currentLeader)


def main():
    replica = Replica(1, 8001, [8003, 8002])
    replica.run()

main()
