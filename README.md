# CS 464/564 – Assignment 4: Replicated Bulletin Board Server

## Overview

`rbbserv` is a production-grade, distributed bulletin board server built on
top of the Assignment 3 solution. It adds:

- Full daemonisation (detach from terminal, PID lock file, log file)
- Dynamic reconfiguration via `SIGHUP`
- Graceful shutdown via `SIGQUIT` / `Ctrl-C`
- Distributed replication using the **two-phase commit (2PC)** protocol

---

## Files

| File | Description |
|------|-------------|
| `rbbserv.cpp` | Main server source (all-in-one) |
| `Makefile` | Build rules – produces `rbbserv` and `client` |
| `bbserv.conf` | Example configuration file |
| `replproto.txt` | Full 2PC application protocol specification |
| `test.cpp` | Interactive test client (unchanged from A3) |
| `messages.txt` | Default bulletin board data file |
| `run/` | Safe directory – created automatically on startup |

---

## Building

```bash
make          # builds both rbbserv and client
make clean    # removes binaries and run/ artefacts
```

Requires: `g++` with C++17 support and POSIX threads (`-pthread`).

---

## Configuration File

The server accepts all Assignment 3 keys plus the following new ones:

```
# ── Existing keys ──────────────────────────────
THMAX    30          # maximum number of worker threads
THINCR   10          # threads pre-allocated at startup (and per growth step)
BBPORT   7777        # port for client connections
BBFILE   messages.txt

# ── New keys (Assignment 4) ────────────────────
FOREGROUND  0        # 1 = stay attached to terminal (useful for testing)
PDEBUG      0        # 1 = log every wire message (client + 2PC)
RPORT       9001     # port for inter-replica (2PC) connections

# One PEER line per replica (host:port on that replica's RPORT):
# PEER  hostname:9001
# PEER  192.168.1.42:9002
```

Boolean values accept `0`/`1` or `false`/`true`.  
All new keys are **optional** and default as shown above.  
Multiple `PEER` lines are supported; no `PEER` lines means standalone mode.

---

## Running

```bash
# Default config (bbserv.conf in current directory):
./rbbserv

# Custom config file:
./rbbserv /path/to/myconfig.conf

# Foreground mode (for testing / debugging):
# Set FOREGROUND 1 in your config, then:
./rbbserv bbserv.conf
```

On startup the server:
1. Reads the configuration file
2. Creates the `run/` subdirectory if it does not exist
3. Daemonises (unless `FOREGROUND 1`)
4. Writes its PID to `run/rbbserv.pid` (prevents duplicate instances)
5. Logs to `run/bbserv.log` (or stdout in foreground mode)
6. Ensures the bulletin board file exists
7. Begins accepting client connections on `BBPORT` and replica connections on `RPORT`

---

## Signals

| Signal | Effect |
|--------|--------|
| `SIGHUP` | Graceful **restart** – active requests finish, idle threads quit, config is re-read, server resumes |
| `SIGQUIT` | Graceful **shutdown** – same drain as SIGHUP, then exit |
| `SIGINT` (Ctrl-C) | Same as SIGQUIT, **only when** `FOREGROUND 1` |
| All others | Ignored |

```bash
kill -HUP  $(cat run/rbbserv.pid)   # restart / reload config
kill -QUIT $(cat run/rbbserv.pid)   # graceful shutdown
```

---

## Client Protocol (unchanged from A3)

Connect with telnet or the provided `client` binary:

```
0.0 WELCOME ...                    ← server greeting

USER <name>        → 1.0 HELLO <name> welcome
                   → 1.2 BAD <name> invalid user name

READ <id>          → 2.0 MESSAGE <id> <poster>/<text>
                   → 2.1 UNKNOWN <id> no such message

WRITE <text>       → 3.0 WROTE <id>
                   → 3.2 ERROR WRITE ...

REPLACE <id>/<poster>/<text>
                   → 3.0 REPLACED <id>
                   → 3.2 ERROR REPLACE ...

QUIT               → 9.0 BYE goodbye
```

---

## Replication (Two-Phase Commit)

When `PEER` lines are present, every `WRITE` and `REPLACE` command triggers
a 2PC round across all peers before the operation is committed anywhere.
`READ` is always served locally with no inter-server communication.

**Protocol summary:**

```
Phase 1 – Precommit
  Master → each slave:  "2PC PRECOMMIT"
  Slave  → master:      "2PC ACK OK"  (or "2PC ACK FAIL" if busy)

  If any slave NAKs → master broadcasts "2PC ABORT", replies 3.2 to client.

Phase 2 – Commit
  Master → each slave:  "2PC COMMIT WRITE <poster> <msg>"
                        "2PC COMMIT REPLACE <id> <poster> <msg>"
  Master also performs the operation locally.
  Slave  → master:      "2PC DONE OK"  (or "2PC DONE FAIL")

  If all OK → master broadcasts "2PC SUCCESS", replies 3.0 to client.
  Otherwise → master broadcasts "2PC UNSUCCESS", all nodes roll back,
              master replies 3.2 to client.
```

Timeout on any phase: **5 seconds** (treated as FAIL/NAK).  
Full protocol specification: see `replproto.txt`.

---

## Testing Multiple Replicas (J118 Machines)

The assignment recommends using your user ID to pick unique ports:

```bash
echo $((8000+$(id -u)))   # client port
echo $((9000+$(id -u)))   # replica port
```

**Example: two replicas on machine1 and machine2**

`bbserv_1.conf` (on machine1):
```
BBPORT  8042
RPORT   9042
BBFILE  messages.txt
FOREGROUND 1
PDEBUG 1
PEER machine2:9042
```

`bbserv_2.conf` (on machine2):
```
BBPORT  8042
RPORT   9042
BBFILE  messages.txt
FOREGROUND 1
PDEBUG 1
PEER machine1:9042
```

Start both:
```bash
# machine1:
./rbbserv bbserv_1.conf

# machine2:
./rbbserv bbserv_2.conf
```

Then connect a client to either server and WRITE – the message will appear
on both bulletin boards, or on neither if replication fails.

```bash
./client machine1 8042
> USER alice
> WRITE hello replicated world
> QUIT

./client machine2 8042
> READ 1          # should see the same message
```

---

## Notes

- If the server is already running, a second invocation will refuse to start
  and print the PID of the existing instance (detected via `flock` on the
  PID file).
- With no `PEER` lines the server behaves identically to Assignment 3.
- `PDEBUG 1` logs every wire message to the log file (or stdout), which is
  useful for watching the 2PC handshake in real time.
- The `run/` directory is relative to the submission root, as required.
# network-Assignment-4
