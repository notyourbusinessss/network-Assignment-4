# CS 464/564 – Assignment 4: Replicated Bulletin Board Server

## server side
The entire thing cna be simply compiled with 
```sh
make
```

after this simply run 

```sh
./rbserv
```

now on the client side you can 

## client side 

the client can be nc or telnet. run it with for example if the ip of the server is 192.168.1.2, here well use telnet so run ;
```sh
telnet 192.168.1.2 7777
```

after this the server should greet you, like such :
```cpp
"0.0 WELCOME ver 1.0: USER READ WRITE REPLACE QUIT spoken here"
```

you should then use the user command to tell the server who you are

like : 

```sh
USER George
```
The the server is gonna respond by welcoming you with your name

```cpp
"1.0 HELLO " + name + " welcome"
```

After this you can do the following commands
### Commands

READ : Read takes an argument which should be an id like so : 
```sh 
READ <ID>
```
If the id exists, the server replies with the message:
```cpp
"2.0 MESSAGE " + id + " " + poster + "/" + text
```
If the id doesn't exist you'll get:
```cpp
"2.1 UNKNOWN " + id + " no such message"
```
And if what you typed after READ isn't a number:
```cpp
"2.2 ERROR READ invalid message number"
```

WRITE : Write posts a new message to the board under your current user name. Everything after the space becomes the message body:
```sh
WRITE <text>
```
On success the server assigns a fresh id and replies:
```cpp
"3.0 WROTE " + id
```
If the server is replicated (has PEER lines) and any peer can't agree, the write is aborted everywhere and you get:
```cpp
"3.2 ERROR WRITE replication failed"
```
If the local file write fails:
```cpp
"3.2 ERROR WRITE could not store message"
```

REPLACE : Replace overwrites an existing message. The format uses `/` as a separator between the id, the new poster name, and the new text:
```sh
REPLACE <id>/<poster>/<text>
```
On success:
```cpp
"3.0 REPLACED " + id
```
Possible errors:
```cpp
"3.2 ERROR REPLACE bad format"        // missing one of the '/' separators
"3.2 ERROR REPLACE bad message id"    // id wasn't a valid integer
"3.2 ERROR REPLACE no such message"   // id isn't in the bulletin board
"3.2 ERROR REPLACE replication failed" // a peer rejected the 2PC round
```

QUIT : Closes the connection. The server replies and shuts down the socket on its end:
```sh
QUIT
```
```cpp
"9.0 BYE goodbye"
```

Any other command is rejected with:
```cpp
"5.0 ERROR unknown command"
```


