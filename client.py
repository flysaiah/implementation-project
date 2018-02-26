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
hostname = str(sys.argv[3])
currentLeader = 0

<<<<<<< Updated upstream
TCP_IP = 'localhost'
# TCP_PORT = "800" + str(currentLeader)
TCP_PORT = int(sys.argv[3])
=======
TCP_IP = hostname
TCP_PORT = "800" + str(currentLeader)
>>>>>>> Stashed changes
BUFFER_SIZE = 1024

mySeqNum = 0


MESSAGE = ":".join(["0", str(myID), "-1",  str(sys.argv[2]), "-1", "none", hostname, str(mySeqNum), "-1", "Hello world!" + str(myID) + "--0"])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, int(TCP_PORT)))
s.sendall(MESSAGE.encode('utf-8'))
rep = s.recv(BUFFER_SIZE)
s.close()

<<<<<<< Updated upstream
TCP_IP = 'localhost'
MY_PORT = int(sys.argv[2])
print_log = int(sys.argv[4])
=======
TCP_IP = hostname
TCP_PORT = int(sys.argv[2])
>>>>>>> Stashed changes
BUFFER_SIZE = 4096

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, MY_PORT))
s.settimeout(10)
s.listen(1)


while 1:
    try:
        seqNum = mySeqNum
        # does not need feedback for print request
        if mySeqNum == 0 or mySeqNum % print_log != 0:
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

            # TCP_IP = 'localhost'
            # TCP_PORT = "800" + str(currentLeader)
            # BUFFER_SIZE = 4096

<<<<<<< Updated upstream
            MESSAGE = ":".join(["0", str(myID), "-1",  str(sys.argv[2]), "-1", str(mySeqNum), "-1", "Hello world!" + str(myID) + "--" + str(mySeqNum)])
            if mySeqNum != 0 and mySeqNum % print_log == 0:
                MESSAGE = ":".join(["7", str(myID), "-1",  str(sys.argv[2]), "-1", str(mySeqNum), "-1", ""])
            
=======
            MESSAGE = ":".join(["0", str(myID), "-1",  str(sys.argv[2]), "-1", "none", hostname, str(mySeqNum), "-1", "Hello world!" + str(myID) + "--" + str(mySeqNum)])
>>>>>>> Stashed changes
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect((TCP_IP, int(TCP_PORT)))
            c.sendall(MESSAGE.encode('utf-8'))
            c.close()

    except KeyboardInterrupt:
        raise
    except socket.timeout:
    # except socket.timeout:
        print("Detecting leader failure, changing view")
        currentLeader += 1
<<<<<<< Updated upstream
        # TCP_IP = 'localhost'
        TCP_PORT += 1
        BUFFER_SIZE = 4096
        print("current port: ", TCP_PORT)
        MESSAGE = ":".join(["6", str(myID), "-1",  str(sys.argv[2]), "-1", str(mySeqNum), "-1", "Hello world!" + str(myID) + "--" + str(mySeqNum)])
=======
        TCP_IP = hostname
        TCP_PORT = "800" + str(currentLeader)
        BUFFER_SIZE = 4096

        MESSAGE = ":".join(["6", str(myID), "-1",  str(sys.argv[2]), "-1", "none", hostname, str(mySeqNum), "-1", "Hello world!" + str(myID) + "--" + str(mySeqNum)])
>>>>>>> Stashed changes
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect((TCP_IP, int(TCP_PORT)))
        c.sendall(MESSAGE.encode('utf-8'))
        rep = c.recv(BUFFER_SIZE)
        c.close()
        continue
    except:
        continue
