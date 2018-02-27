import socket
import sys
import time
import copy
import random

class Client:

    def __init__(self, id, port, hostname, printLog, messageDrop, allReplicas, tryNumber, timeout):
        self.id = id
        self.port = port
        self.hostname = hostname
        self.printLog = printLog
        self.allReplicas = allReplicas
        self.seqNum = 0
        self.currentLeaderIndex = 0
        self.tryNumberLimit = tryNumber
        self.timeout = timeout
        self.messageDrop = messageDrop

    def run(self):
        viewChange = False
        tryNumber = 0
        while self.currentLeaderIndex <= (len(self.allReplicas) // 2 + 1):
            try:
                BUFFER_SIZE = 1024
                reqType = "0"
                if viewChange:
                    reqType = "6"
                MESSAGE = ":".join([reqType, str(self.id), "-1",  str(self.port), "-1", "none", self.hostname, str(self.seqNum), "-1", "Hello world!" + str(self.id) + "--" + str(self.seqNum)])
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(self.timeout)
                port = self.allReplicas[self.currentLeaderIndex][0]
                hostname = self.allReplicas[self.currentLeaderIndex][1]
                s.connect((hostname, int(port)))
                s.sendall(MESSAGE.encode('utf-8'))
                rep = s.recv(BUFFER_SIZE)
                s.close()
                break
            except KeyboardInterrupt:
                raise
            except:
                tryNumber += 1
                if tryNumber == self.tryNumberLimit:
                    print("Initiate view change from " + str(self.currentLeaderIndex) + " to " + str(self.currentLeaderIndex + 1))
                    # Initiate view change
                    viewChange = True
                    tryNumber = 0
                    self.currentLeaderIndex += 1
                continue
        if self.currentLeaderIndex > len(self.allReplicas) // 2 + 1:
            print("More than f replicas have failed; I'm quitting now")
            return

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, int(self.port)))
        print((self.hostname, int(self.port)))
        s.settimeout(20)
        s.listen(1)

        while 1:
            reqType = None
            otherSeqNum = None
            msg = None
            try:
                otherSeqNum = self.seqNum
                # does not need feedback for print request
                if self.seqNum == 0 or self.seqNum % self.printLog != 0:
                    conn, addr = s.accept()
                    #print ('Connection address:', addr)
                    data = conn.recv(BUFFER_SIZE).decode('utf_8')
                    conn.close()
                    #print("Request received, data = ", data)
                    if not data:
                        # print("Didn't receive any data")
                        continue
                    r = random.random()
                    if r <= self.messageDrop:
                        print("dropping message: ", data)
                        continue
                    print("Received data: ", data)
                    reqType, otherSeqNum, msg = data.split(':')

                if int(self.seqNum) == int(otherSeqNum):
                    print("Client ID " + str(self.id) + " sequence number from " + str(self.seqNum) + " to " + str(self.seqNum + 1))
                    self.seqNum += 1
            except socket.timeout:
                # Leader failure
                print("Initiate view change from " + str(self.currentLeaderIndex) + " to " + str(self.currentLeaderIndex + 1))
                self.currentLeaderIndex += 1
                reqType = 6
            except Exception as e:
                print("Exception: ", str(e))
                continue

            if reqType is None:
                continue

            viewChange = False
            if reqType == 6:
                viewChange = True
            tryNumber = 0
            while self.currentLeaderIndex <= (len(self.allReplicas) // 2 + 1):
                try:
                    reqType = "0"
                    if viewChange:
                        reqType = "6"
                    MESSAGE = ":".join([reqType, str(self.id), "-1",  str(self.port), "-1", "none", self.hostname, str(self.seqNum), "-1", "Hello world!" + str(self.id) + "--" + str(self.seqNum)])
                    if self.seqNum != 0 and self.seqNum % self.printLog == 0:
                        MESSAGE = ":".join(["7", str(self.id), "-1",  str(self.port), "-1", "none", self.hostname, str(self.seqNum), "-1", ""])

                    port = self.allReplicas[self.currentLeaderIndex][0]
                    hostname = self.allReplicas[self.currentLeaderIndex][1]
                    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    c.settimeout(self.timeout)
                    port = self.allReplicas[self.currentLeaderIndex][0]
                    hostname = self.allReplicas[self.currentLeaderIndex][1]
                    c.connect((hostname, int(port)))
                    c.sendall(MESSAGE.encode('utf-8'))
                    c.close()
                    break
                except KeyboardInterrupt:
                    raise
                except socket.timeout:
                    tryNumber += 1
                    if tryNumber == self.tryNumberLimit:
                        print("Initiate view change from " + str(self.currentLeaderIndex) + " to " + str(self.currentLeaderIndex + 1))
                        # Initiate view change
                        viewChange = True
                        tryNumber = 0
                        self.currentLeaderIndex += 1
                    continue
                except ConnectionRefusedError:
                    tryNumber += 1
                    if tryNumber == self.tryNumberLimit:
                        print("Initiate view change from " + str(self.currentLeaderIndex) + " to " + str(self.currentLeaderIndex + 1))
                        # Initiate view change
                        viewChange = True
                        tryNumber = 0
                        self.currentLeaderIndex += 1
                    continue
                except Exception as e:
                    print("Exception = ", str(e))
                    continue


def main():
    argc = len(sys.argv)
    allReplicas = []
    for i in range(6, argc, 2):
        allReplicas.append((int(sys.argv[i]), sys.argv[i+1]))
    #print(allReplicas)
    c = Client(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), float(sys.argv[5]), allReplicas, 20, 5)
    c.run()

main()
