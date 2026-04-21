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

## config file

the server reads its config from `bbserv.conf` by default. if you want to use a different file just pass it as an argument :

```sh
./rbbserv myconfig.conf
```

the config is a plain text file, one key per line, `#` for comments. here's what the server accepts :

`THMAX` is the max number of worker threads, default 25. `THINCR` is how many threads get pre-allocated at startup and added in chunks as the server grows, default 5.

`BBPORT` is the tcp port for client connections, default 9000. `RPORT` is the tcp port for inter-replica (2PC) connections, default 9001.

`BBFILE` is the path to the bulletin board data file, default `messages.txt`.

`FOREGROUND` defaults to 0. set it to 1 to stay attached to the terminal and log to stdout, useful for testing.

`PDEBUG` defaults to 0. set it to 1 to log every message sent or received (client and 2PC). handy for watching the 2PC handshake.

`PEER` is a host:port of another replica. repeat the line once per peer. no peer lines = standalone mode, just like A3.

booleans accept `0` / `1` or `true` / `false`, whatever you prefer.

example config for a simple standalone server :

```conf
THMAX      10
THINCR     2
BBPORT     7777
BBFILE     messages.txt
FOREGROUND 1
```

## signals

the server reacts to a few signals :

`SIGHUP` triggers a graceful restart. idle workers quit, active ones finish their current request and then quit, the config is re-read, and the server keeps going with the new settings.

`SIGQUIT` is a graceful shutdown. same drain as SIGHUP then exit.

`SIGINT` is treated the same as SIGQUIT, but only when `FOREGROUND 1` (so Ctrl-C works in foreground mode). everything else is ignored.

to send them :

```sh
kill -HUP  $(cat run/rbbserv.pid)   # reload
kill -QUIT $(cat run/rbbserv.pid)   # shutdown
./stop.sh                           # wrapper around SIGQUIT
```

## replication

as soon as you put one or more `PEER` lines in the config, the server switches into replicated mode. every `WRITE` and `REPLACE` is then done via two-phase commit across all peers before the operation is committed anywhere. `READ` stays local and doesn't touch the peers.

if any peer is unreachable or says no, the write is aborted everywhere and the client gets `3.2 ERROR WRITE replication failed`. the bulletin board file stays untouched on every replica — either the write happens on all of them or on none of them.

the full wire protocol (state machines, timeouts, failure paths) lives in `replproto.txt`.

### multi-replica example

two replicas on the same machine, each pointing at the other.

rep1 config :
```conf
BBPORT     7101
RPORT      9101
BBFILE     messages.txt
FOREGROUND 1
PDEBUG     1
PEER       localhost:9102
```

rep2 config :
```conf
BBPORT     7102
RPORT      9102
BBFILE     messages.txt
FOREGROUND 1
PDEBUG     1
PEER       localhost:9101
```

start them in two terminals :

```sh
# terminal 1
cd rep1 && ./rbbserv bbserv.conf

# terminal 2
cd rep2 && ./rbbserv bbserv.conf
```

then from a third terminal, write to one and read from the other :

```sh
telnet localhost 7101
# USER alice, WRITE something, QUIT

telnet localhost 7102
# USER bob, READ 1, QUIT  — same message shows up
```

with `PDEBUG 1` you can watch the whole 2PC handshake (PRECOMMIT / ACK OK / COMMIT / DONE OK / SUCCESS) scroll by in each server's output.

## files on disk

the server uses a `run/` subdirectory (next to the binary) for its runtime files :

- `run/rbbserv.pid` — the server's PID, locked with `flock`. a second `./rbbserv` in the same folder will refuse to start and print the PID of the existing instance.
- `run/bbserv.log` — log file in daemon mode. in foreground mode the logs go straight to stdout instead.

the `run/` directory gets auto-created on startup if it doesn't exist. `messages.txt` (or whatever `BBFILE` is set to) lives next to the server, not inside `run/`.

## see also

- `replproto.txt` — full spec of the 2PC wire protocol (required by the assignment)
- `Report` — implementation details, design choices, and testing notes