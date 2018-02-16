from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class Proposer:
    def __init__(self, id, address, numberOfAcceptors, otherReplicas):
        self.numberOfAcceptors = numberOfAcceptors
        self.numberOfAcceptorResponses = 0
        self.mostRecentLeader = 0
        self.requiredMessageToSend = None
        self.message = None
        self.otherReplicas = otherReplicas
        self.id = id
        self.address = address

    def setMessage(self, m):
        self.message = m

    def receiveYouAreLeader(self, id, address, currentAcceptedValue, currentLeader):
        self.numberOfAcceptorResponses += 1
        print("Uh oh 2")
        print(currentLeader)
        print(self.mostRecentLeader)
        if currentLeader is not None and currentLeader >= self.mostRecentLeader:
            self.mostRecentLeader = currentLeader
            self.requiredMessageToSend = currentAcceptedValue
        print("Made it here")

        if self.numberOfAcceptorResponses > (self.numberOfAcceptors / 2):
            self.proposeMessage()

    def proposeMessage(self):
        if self.requiredMessageToSend is not None:
            # send required message
            for replica in self.otherReplicas():
                s = xmlrpc.client.ServerProxy(replica)
                s.receiveProposedMessage(self.id, self.address, self.requiredMessageToSend)
        else:
            # send original message
            self.acceptor.receiveProposedMessage(self.id, self.address, self.message)
            for replica in self.otherReplicas():
                s = xmlrpc.client.ServerProxy(replica)
                s.receiveProposedMessage(self.id, self.address, self.message)
class Acceptor:
    def __init__(self, id, address, otherReplicas):
        self.currentAcceptedValue = None
        self.currentLeader = None
        self.otherReplicas = otherReplicas
        self.id = id
        self.address = address

    def receiveIAmLeader(self, id, address):
        # NOTE: Use leader ID, not process ID
        self.currentLeader = id
        print(address)
        print("Okay here")
        return self.currentAcceptedValue, self.currentLeader

    def receiveProposedMessage(self, id, address, message):
        # NOTE: We were a little unsure about this rule
        print("Uh oh")
        print(id)
        if id >= self.currentLeader:
            self.acceptMessage(message)
        # NOTE: Do we need to update current leader?
    def acceptMessage(self, message):
        # send required message
        for replica in self.otherReplicas():
            s = xmlrpc.client.ServerProxy(replica)
            s.receiveAcceptance(self.id, self.address, message)

class Learner:
    def __init__(self, numberOfAcceptors):
        self.numberOfAcceptors = numberOfAcceptors
        self.acceptanceMap = {}

    def receiveAcceptance(self, id, address, message):
        if (id, message) not in self.acceptanceMap:
            self.acceptanceMap[(id, message)] = 1
        else:
            self.acceptanceMap[(id, messagee)] += 1

        if self.acceptanceMap[(id, message)] > self.numberOfAcceptors / 2:
            self.deliver_message(message)

    def deliver_message(m):
        print("Message delivered: ", m)

class Replica:

    def __init__(self, address, otherReplicas):
        self.id = 0
        self.proposer = Proposer(self.id, address, len(otherReplicas) + 1, otherReplicas)
        self.acceptor = Acceptor(self.id, address, otherReplicas)
        self.learner = Learner(len(otherReplicas) + 1)
        self.otherReplicas = otherReplicas
        self.address = address

    def receive_request(self, m):
        self.runPaxos(m)
        return "Okay"

    def runPaxos(self, m):
        print("Running Paxos")
        self.proposer.setMessage(m)
        #self.receiveIAmLeader(self.id, self.address, True)
        self.acceptor.currentLeader = self.id
        self.proposer.receiveYouAreLeader(self.acceptor.id, self.acceptor.address, self.acceptor.currentAcceptedValue, self.acceptor.currentLeader)
        print("Self stuff done")
        for replica in self.otherReplicas:
            print("LOOPING")
            s = xmlrpc.client.Server(replica)
            currentAcceptedValue, currentLeader = s.receiveIAmLeader(self.id, self.address)
            self.receiveYouAreLeader(self.id, self.address, currentAcceptedValue, currentLeader)
            print("ENDLOOP 1")


    def receiveIAmLeader(self, id, address):
        print("OKAY")
        self.acceptor.receiveIAmLeader(id, address)

    def receiveYouAreLeader(self, id, address, currentAcceptedValue, currentLeader):
        print("PPLLLLZZZZ")
        self.proposer.receiveYouAreLeader(id, address, currentAcceptedValue, currentLeader)

    def receiveProposedMessage(self, id, address, message):
        self.acceptor.receiveProposedMessage(id, address, message)

    def receiveAcceptance(self, id, address, message):
        self.learner.receiveAcceptance(id, address, message)

    def run(self):

        with SimpleXMLRPCServer(("localhost", 8000),
                                requestHandler=RequestHandler, allow_none=True) as server:
            server.register_introspection_functions()

            server.register_function(self.receive_request, 'receive_request')
            server.register_function(self.receiveIAmLeader, 'receiveIAmLeader')
            server.register_function(self.receiveYouAreLeader, 'receiveYouAreLeader')
            server.register_function(self.receiveProposedMessage, 'receiveProposedMessage')
            server.register_function(self.receiveAcceptance, 'receiveAcceptance')

            # Run the server's main loop
            server.serve_forever()


def main():
    replica = Replica("http://localhost:8000", ["http://localhost:8001", "http://localhost:8002" ])
    replica.run()

main()
