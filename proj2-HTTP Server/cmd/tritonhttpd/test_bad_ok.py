from socket import socket
import time

# Create connection to the server
s = socket()
s.connect(("localhost", 8080))

s1 = socket()
s1.connect(("localhost",8080))

# Compose the message/HTTP request we want to send to the server
#msgPart1 = b"foobar\r\nHost: website1\r\nConnection: close\r\n\r\n"
#msgPart1 = b"GET /index.html HTTP/1.1\r\nHost: website1\r\n\r\nGET /notfound.html HTTP/1.1\r\nHost: website1\r\nUser-Agent: gotest\r\n\r\n"
msgPart1 = b"GET / HTTP/1.1\r\nHost: website1\r\n\r\n"
msgBadReq = b"GET / HTTP/1.2\r\nHost: website1\r\n\r\n"

# Send out the request
s1.sendall(msgBadReq)
s.sendall(msgPart1)


# Listen for response and print it out
print (s1.recv(4096))
print (s.recv(4096))
#print (s.recv(4096))

# print (s1.recv(4096))
#time.sleep(1)


s.close()
time.sleep(1)
s1.close()