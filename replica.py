from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class Proposer:
    def __init__(self):
        pass

    def receive_message(m):
        # When message is received
        pass

class Acceptor:
    def __init__(self):
        pass
class Learner:
    def __init__(self):
        pass

class Replica:

    def __init__(self, clients):
        self.proposer = Proposer()
        self.acceptor = Acceptor()
        self.learner = Learner()
        self.clients = clients

    def receive_message(self, m):
        self.runPaxos(m)

    def runPaxos(m):
        pass

    def run():

        with SimpleXMLRPCServer(("localhost", 8000),
                                requestHandler=RequestHandler) as server:
            server.register_introspection_functions()

            # def adder_function(x,y):
            #     return x + y
            # server.register_function(adder_function, 'add')

            # Register an instance; all the methods of the instance are
            # published as XML-RPC methods (in this case, just 'mul').
            class MyFuncs:
                def mul(self, x, y):
                    return x * y

            server.register_instance(MyFuncs())

            # Run the server's main loop
            server.serve_forever()


def main():
    replica = Replica(0)
    replica.run()
