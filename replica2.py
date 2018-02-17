from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import time
import socket

# Restrict to a particular path.
# class RequestHandler(SimpleXMLRPCRequestHandler):
#     rpc_paths = ('/RPC2',)

class Proposer:
    def __init__(self, id, port, numberOfAcceptors, otherReplicas, acceptor):
        self.numberOfAcceptors = numberOfAcceptors
        self.numberOfAcceptorResponses = 0
        self.mostRecentLeader = 0
        self.requiredMessage = None
        self.message = None
        self.otherReplicas = otherReplicas
        self.id = id
        self.port = port
        self.acceptor = acceptor

    def setMessage(self, m):
        self.message = m

    def receiveYouAreLeader(self, id, port, currentAcceptedValue, currentLeader):
        self.numberOfAcceptorResponses += 1
        print("Current leader: ", currentLeader)
        print("Most recent leader: ", self.mostRecentLeader)
        if currentLeader is not None and currentLeader != "None" and int(currentLeader) >= int(self.mostRecentLeader):
            self.mostRecentLeader = currentLeader
            self.requiredMessage = currentAcceptedValue

        if self.numberOfAcceptorResponses == (self.numberOfAcceptors // 2 + 1):
            self.proposeMessage()

    def proposeMessage(self):
        print("Proposing message")
        print("Message: ", self.message)
        print("Req: ", self.requiredMessage)
        if self.requiredMessage is not None and self.requiredMessage != "None":
            print("This shouldn't be happening")
            # send required message
            self.acceptor.receiveProposedMessage(self.id, self.port, self.requiredMessage)
            for replica in self.otherReplicas:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', replica))
                s.sendall((':'.join(['3', str(self.id), str(self.port), self.requiredMessage]).encode('utf-8')))
                s.close()
        else:
            # send original message
            self.acceptor.receiveProposedMessage(self.id, self.port, self.message)
            for replica in self.otherReplicas:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', replica))
                s.sendall((':'.join(['3', str(self.id), str(self.port), self.message]).encode('utf-8')))
                s.close()


class Acceptor:
    def __init__(self, id, port, otherReplicas):
        self.currentAcceptedValue = None
        self.currentLeader = None
        self.otherReplicas = otherReplicas
        self.id = id
        self.port = port

    def receiveIAmLeader(self, id, port):
        # NOTE: Use leader ID, not process ID
        if self.currentLeader is None or self.currentLeader == "None" or self.currentLeader <= id:
            self.currentLeader = id
            print("Port: ", port)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', int(port)))
            s.sendall(':'.join(['2', str(self.id), str(self.port), str(self.currentAcceptedValue) + ';' + str(self.currentLeader)]).encode('utf-8'))
            s.close()


    def receiveProposedMessage(self, id, port, message):
        # NOTE: We were a little unsure about this rule
        print("Message: ", message)
        if id >= self.currentLeader:
            self.acceptMessage(id, message)

        # NOTE: Do we need to update current leader?
    def acceptMessage(self, id, message):
        # set the current accepted value
        self.currentAcceptedValue = message
        self.currentLeader = id
        # broadcast accepted value to all learner
        for replica in self.otherReplicas:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("Replica: ", replica)
            s.connect(('localhost', replica))
            # should send the id of leader here
            s.sendall(':'.join(['4', str(self.id), str(self.port), str(self.currentAcceptedValue) + ';' + str(self.currentLeader)]).encode('utf-8'))
            s.close()


class Learner:
    def __init__(self, numberOfAcceptors):
        self.numberOfAcceptors = numberOfAcceptors
        self.acceptanceMap = {}

    def receiveAcceptance(self, id, port, message, leader):
        if (leader, message) not in self.acceptanceMap:
            self.acceptanceMap[(leader, message)] = 1
        else:
            self.acceptanceMap[(leader, message)] += 1

        if self.acceptanceMap[(leader, message)] > self.numberOfAcceptors / 2:
            self.deliver_message(message)

    def deliver_message(self, m):
        print("Message delivered: ", m)

class Replica:

    def __init__(self, id, port, otherReplicas):
        self.id = id
        self.acceptor = Acceptor(self.id, port, otherReplicas)
        self.proposer = Proposer(self.id, port, len(otherReplicas) + 1, otherReplicas, self.acceptor)
        self.learner = Learner(len(otherReplicas) + 1)
        self.otherReplicas = otherReplicas
        print("Replicas: ", otherReplicas)
        self.port = port

    def receive_request(self, m):
        self.runPaxos(m)
        return "Okay"

    def runPaxos(self, m):
        print("Running Paxos")
        self.proposer.setMessage(m)
        #self.receiveIAmLeader(self.id, self.port, True)
        self.acceptor.currentLeader = self.id
        self.proposer.receiveYouAreLeader(self.acceptor.id, self.acceptor.port, self.acceptor.currentAcceptedValue, self.acceptor.currentLeader)
        for replica in self.otherReplicas:
            print("LOOPING")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # NOTE: Will have to change this to include other hostnames
            s.connect(('localhost', replica))
            # Send I AM LEADER message
            s.sendall(':'.join(['1', str(self.id), str(self.port), '_']).encode('utf-8'))
            s.close()
            print("ENDLOOP 1")


    def run(self):
        TCP_IP = 'localhost'
        TCP_PORT = self.port
        BUFFER_SIZE = 1024

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((TCP_IP, TCP_PORT))
        s.listen(1)

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
            reqType, clientID, port, msg = data.split(':')
            print("Request type: ", reqType)
            print("Message: ", msg)
            if reqType == "0":
                self.runPaxos(msg)
            elif reqType == "1":
                self.acceptor.receiveIAmLeader(clientID, port)
            elif reqType == "2":
                currentAcceptedValue, currentLeader = msg.split(";")
                self.proposer.receiveYouAreLeader(clientID, port, currentAcceptedValue, currentLeader)
            elif reqType == "3":
                self.acceptor.receiveProposedMessage(clientID, port, msg)
            elif reqType == "4":
                currentAcceptedValue, currentLeader = msg.split(";")
                self.learner.receiveAcceptance(clientID, port, currentAcceptedValue, currentLeader)


def main():
    replica = Replica(1, 8001, [8000, 8002])
    replica.run()

main()
