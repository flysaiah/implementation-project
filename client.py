# from xmlrpc.server import SimpleXMLRPCServer
# from xmlrpc.server import SimpleXMLRPCRequestHandler
# import xmlrpc.client
#
# s = xmlrpc.client.ServerProxy("http://localhost:8000", allow_none=True)
# s.receive_request("PLEASE DELIVER ME")


import socket
import sys
import time

myID = int(sys.argv[1])
currentLeader = 0

TCP_IP = 'localhost'
TCP_PORT = "800" + str(currentLeader)
BUFFER_SIZE = 1024


MESSAGE = ":".join(["0", str(myID), "-1",  str(sys.argv[2]), "-1", "0", "-1", "Hello world!" + str(myID) + "--0"])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, int(TCP_PORT)))
s.sendall(MESSAGE.encode('utf-8'))
rep = s.recv(BUFFER_SIZE)
s.close()

TCP_IP = 'localhost'
TCP_PORT = int(sys.argv[2])
BUFFER_SIZE = 4096

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.settimeout(10)
s.listen(1)

mySeqNum = 0

while 1:
    try:
        conn, addr = s.accept()
        #print ('Connection address:', addr)
        data = conn.recv(BUFFER_SIZE).decode('utf_8')
        conn.close()
        #print("Request received, data = ", data)
        if not data:
            print("Didn't receive any data")
            continue
        host = addr[0]
        print("Received data: ", data)
        reqType, seqNum, msg = data.split(':')

        if int(mySeqNum) == int(seqNum):

            print("Client ID " + str(myID) + " sequence number from " + str(mySeqNum) + " to " + str(mySeqNum + 1))

            mySeqNum += 1

            myID = sys.argv[1]

            TCP_IP = 'localhost'
            TCP_PORT = "800" + str(currentLeader)
            BUFFER_SIZE = 4096

            MESSAGE = ":".join(["0", str(myID), "-1",  str(sys.argv[2]), "-1", str(mySeqNum), "-1", "Hello world!" + str(myID) + "--" + str(mySeqNum)])
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect((TCP_IP, int(TCP_PORT)))
            c.sendall(MESSAGE.encode('utf-8'))
            rep = c.recv(BUFFER_SIZE)
            c.close()
    except KeyboardInterrupt:
        raise
    except socket.timeout:
    # except socket.timeout:
        print("Detecting leader failure, changing view")
        currentLeader += 1
        TCP_IP = 'localhost'
        TCP_PORT = "800" + str(currentLeader)
        BUFFER_SIZE = 4096

        MESSAGE = ":".join(["6", str(myID), "-1",  str(sys.argv[2]), "-1", str(mySeqNum), "-1", "Hello world!" + str(myID) + "--" + str(mySeqNum)])
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect((TCP_IP, int(TCP_PORT)))
        c.sendall(MESSAGE.encode('utf-8'))
        rep = c.recv(BUFFER_SIZE)
        c.close()
        continue
    except:
        continue
