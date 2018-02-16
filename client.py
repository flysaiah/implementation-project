from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client

s = xmlrpc.client.ServerProxy("http://localhost:8000")
s.receive_request("PLEASE DELIVER ME")
