import socket

TCP_IP = 'localhost'
TCP_PORT = 8001
BUFFER_SIZE = 1024

MESSAGE = "0:Hello, World!"
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))
s.sendall(MESSAGE.encode('utf-8'))
rep = s.recv(BUFFER_SIZE)
s.close()