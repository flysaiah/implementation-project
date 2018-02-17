# from xmlrpc.server import SimpleXMLRPCServer
# from xmlrpc.server import SimpleXMLRPCRequestHandler
# import xmlrpc.client
#
# s = xmlrpc.client.ServerProxy("http://localhost:8000", allow_none=True)
# s.receive_request("PLEASE DELIVER ME")


import socket

TCP_IP = 'localhost'
TCP_PORT = 8000
BUFFER_SIZE = 1024

MESSAGE = "0:-1:23923:Hello, World!"
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))
s.sendall(MESSAGE.encode('utf-8'))
rep = s.recv(BUFFER_SIZE)
s.close()
