from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import socket

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class Proposer:
    def __init__(self, id, port, numberOfAcceptors, otherReplicas):
        self.numberOfAcceptors = numberOfAcceptors
        self.numberOfAcceptorResponses = 0
        self.mostRecentLeader = 0
        self.requiredMessage = None
        self.message = None
        self.otherReplicas = otherReplicas
        self.id = id
        self.port = port

    def setMessage(self, m):
        self.message = m

    def receiveYouAreLeader(self, id, port, currentAcceptedValue, currentLeader):
        self.numberOfAcceptorResponses += 1
        print("Uh oh 2")
        print(currentLeader)
        print(self.mostRecentLeader)
        if currentLeader is not None and currentLeader >= self.mostRecentLeader:
            self.mostRecentLeader = currentLeader
            self.requiredMessage = currentAcceptedValue
        print("Made it here")

        if self.numberOfAcceptorResponses > (self.numberOfAcceptors / 2):
            self.proposeMessage()

    def proposeMessage(self):
        if self.requiredMessage is not None:
            # send required message
            self.acceptor.receiveProposedMessage(self.id, self.port, self.requiredMessage)
            for replica in self.otherReplicas():
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', replica))
                s.sendall(('3:', self.id, ' ', self.port, ' ', self.requiredMessage).encode('utf-8'))
                s.close()

        else:
            # send original message
            self.acceptor.receiveProposedMessage(self.id, self.port, self.message)
            for replica in self.otherReplicas():
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', replica))
                s.sendall(('3:', self.id, ' ', self.port, ' ', self.message).encode('utf-8'))
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
        if(self.currentLeader <= id):
            self.currentLeader = id
            print(port)
            print("Okay here")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', port))
            s.sendall(('2:', self.currentAcceptedValue, ' ', self.currentLeader).encode('utf-8'))
            s.close()


    def receiveProposedMessage(self, id, port, message):
        # NOTE: We were a little unsure about this rule
        print("Uh oh")
        print(id)
        if id >= self.currentLeader:
            self.acceptMessage(id, message)
        
        # NOTE: Do we need to update current leader?
    def acceptMessage(self, id, message):
        # set the current accepted value
        self.currentAcceptedValue = message
        self.currentLeader = id
        # broadcast accepted value to all learner
        for replica in self.otherReplicas():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', replica))
            # should send the id of leader here
            s.sendall(('4:', self.currentLeader, ' ', self.port, ' ', self.currentAcceptedValue).encode('utf-8'))
            s.close()


class Learner:
    def __init__(self, numberOfAcceptors):
        self.numberOfAcceptors = numberOfAcceptors
        self.acceptanceMap = {}

    def receiveAcceptance(self, id, port, message):
        if (id, message) not in self.acceptanceMap:
            self.acceptanceMap[(id, message)] = 1
        else:
            self.acceptanceMap[(id, messagee)] += 1

        if self.acceptanceMap[(id, message)] > self.numberOfAcceptors / 2:
            self.deliver_message(message)

    def deliver_message(self, m):
        print("Message delivered: ", m)

class Replica:

    def __init__(self, port, otherReplicas):
        self.id = 0
        self.proposer = Proposer(self.id, port, len(otherReplicas) + 1, otherReplicas)
        self.acceptor = Acceptor(self.id, port, otherReplicas)
        self.learner = Learner(len(otherReplicas) + 1)
        self.otherReplicas = otherReplicas
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
        print("Self stuff done")
        for replica in self.otherReplicas:
            print("LOOPING")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', replica))
            s.sendall(('1:', self.id, ' ', self.port).encode('utf-8'))
            s.close()
            print("ENDLOOP 1")


    def run(self):        
        TCP_IP = 'localhost'
        TCP_PORT = 8001
        BUFFER_SIZE = 1024

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((TCP_IP, TCP_PORT))
        s.listen(1)

        # type of request received:
        #     0. client message
        #     1. IamLeader
        #     2. YouAreLeader
        #     3. proposedValue
        #     4. accceptedValue 

        while 1:
            conn, addr = s.accept()
            print ('Connection addres:', addr)
            data = conn.recv(BUFFER_SIZE).decode('utf_8')
            conn.close()
            print(data)
            if not data: break
            type, msg = data.split(':')
            print(type)
            print(msg)
            if type == 0:
                self.proposer.receive_request(msg)
            elif type == 1:
                self.acceptor.receiveIAmLeader(id, port)
            elif type == 2:
                self.proposer.receiveYouAreLeader(id, port, currentAcceptedValue, currentLeader)
            elif type == 3:
                self.acceptor.receiveProposedMessage(id, port, message)
            elif type == 4:
                self.learner.receiveAcceptance(id, port, message)




        # with SimpleXMLRPCServer(("localhost", 8000),
        #                         requestHandler=RequestHandler, allow_none=True) as server:
        #     server.register_introspection_functions()

        #     server.register_function(self.receive_request, 'receive_request')
        #     server.register_function(self.receiveIAmLeader, 'receiveIAmLeader')
        #     server.register_function(self.receiveYouAreLeader, 'receiveYouAreLeader')
        #     server.register_function(self.receiveProposedMessage, 'receiveProposedMessage')
        #     server.register_function(self.receiveAcceptance, 'receiveAcceptance')

        #     # Run the server's main loop
        #     server.serve_forever()


def main():
    replica = Replica(8000, [8001, 8002])
    replica.run()

main()
