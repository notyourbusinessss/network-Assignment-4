/*
 * rbbserv.cpp  –  CS 464/564 Assignment 4
 *
 * Replicated bulletin-board server with:
 *   - Daemonisation (PID file, log file, safe-dir, fd cleanup)
 *   - FOREGROUND / PDEBUG / RPORT / PEER config keys
 *   - SIGHUP  → graceful restart (re-read config, re-allocate threads)
 *   - SIGQUIT / SIGINT (fg only) → graceful shutdown
 *   - Thread pool (THMAX / THINCR)
 *   - Readers-writer lock on the bulletin-board file
 *   - Two-phase commit (2PC) for WRITE / REPLACE across replica peers
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <map>
#include <chrono>

#include <queue>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cerrno>
#include <cassert>

#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/file.h>     // flock
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <syslog.h>

/* ─────────────────────────────────────────────
   Constants
   ───────────────────────────────────────────── */
static const int    TIMEOUT_SEC    = 5;   // 2PC phase timeout
static const char*  SAFE_DIR       = "run";
static const char*  PID_FILE       = "run/rbbserv.pid";
static const char*  LOG_FILE       = "run/bbserv.log";
static const char*  DEFAULT_CONF   = "bbserv.conf";

/* ─────────────────────────────────────────────
   Config
   ───────────────────────────────────────────── */
struct Peer { std::string host; int port; };

struct Config {
    int  thmax      = 25;
    int  thincr     = 5;
    int  bbport     = 9000;
    int  rport      = 9001;
    bool foreground = false;
    bool pdebug     = false;
    std::string bbfile = "messages.txt";
    std::vector<Peer> peers;
};

/* Global state */
static Config        g_cfg;
static std::string   g_configPath = DEFAULT_CONF;
static std::mutex    g_logMtx;
static FILE*         g_logFp      = nullptr;   // null → use stdout

/* ─────────────────────────────────────────────
   Logging
   ───────────────────────────────────────────── */
static void logMsg(const std::string& msg) {
    std::lock_guard<std::mutex> lk(g_logMtx);
    FILE* fp = g_logFp ? g_logFp : stdout;
    fprintf(fp, "%s\n", msg.c_str());
    fflush(fp);
}

static void pdebugLog(const std::string& msg) {
    if (g_cfg.pdebug) logMsg("[PDEBUG] " + msg);
}

/* ─────────────────────────────────────────────
   Config parsing
   ───────────────────────────────────────────── */
static bool strToBool(const std::string& s) {
    return s == "1" || s == "true";
}

static bool loadConfig(const std::string& path, Config& out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror("open config"); return false; }

    std::string raw;
    char buf[1024];
    for (;;) {
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n > 0)  { raw.append(buf, (size_t)n); continue; }
        if (n == 0) break;
        if (errno == EINTR) continue;
        perror("read config"); close(fd); return false;
    }
    close(fd);

    out.peers.clear();
    std::istringstream ss(raw);
    std::string line;
    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        std::istringstream ls(line);
        std::string key, val;
        ls >> key >> val;
        if (key.empty() || key[0] == '#') continue;

        if      (key == "THMAX"      && !val.empty()) out.thmax      = std::stoi(val);
        else if (key == "THINCR"     && !val.empty()) out.thincr     = std::stoi(val);
        else if (key == "BBPORT"     && !val.empty()) out.bbport     = std::stoi(val);
        else if (key == "RPORT"      && !val.empty()) out.rport      = std::stoi(val);
        else if (key == "FOREGROUND" && !val.empty()) out.foreground = strToBool(val);
        else if (key == "PDEBUG"     && !val.empty()) out.pdebug     = strToBool(val);
        else if (key == "BBFILE"     && !val.empty()) out.bbfile     = val;
        else if (key == "PEER"       && !val.empty()) {
            // val is "host:port"
            auto col = val.rfind(':');
            if (col != std::string::npos) {
                Peer p;
                p.host = val.substr(0, col);
                p.port = std::stoi(val.substr(col + 1));
                out.peers.push_back(p);
            }
        }
    }
    return true;
}

/* ─────────────────────────────────────────────
   Signal handling
   ───────────────────────────────────────────── */
static std::atomic<bool> g_doRestart{false};
static std::atomic<bool> g_doQuit{false};

static void sigHandler(int sig) {
    if      (sig == SIGHUP)            g_doRestart = true;
    else if (sig == SIGQUIT || sig == SIGINT) g_doQuit = true;
}

static void setupSignals(bool fg) {
    struct sigaction sa{};
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    sa.sa_handler = sigHandler;
    sigaction(SIGHUP,  &sa, nullptr);
    sigaction(SIGQUIT, &sa, nullptr);
    if (fg) sigaction(SIGINT, &sa, nullptr);
    else {
        sa.sa_handler = SIG_IGN;
        sigaction(SIGINT, &sa, nullptr);
    }

    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGTSTP, &sa, nullptr);
    sigaction(SIGTTIN, &sa, nullptr);
    sigaction(SIGTTOU, &sa, nullptr);
}

/* ─────────────────────────────────────────────
   Daemonisation
   ───────────────────────────────────────────── */
static int g_pidFd = -1;   // kept open to hold the flock

static bool writePidFile() {
    // Open (create if needed)
    g_pidFd = open(PID_FILE, O_RDWR | O_CREAT, 0644);
    if (g_pidFd < 0) { perror("open pid file"); return false; }

    // Exclusive non-blocking lock → detect duplicate
    if (flock(g_pidFd, LOCK_EX | LOCK_NB) < 0) {
        if (errno == EWOULDBLOCK) {
            char existing[32] = {};
            (void)read(g_pidFd, existing, sizeof(existing)-1);
            fprintf(stderr, "Server already running (pid %s)\n", existing);
        } else {
            perror("flock pid file");
        }
        close(g_pidFd);
        g_pidFd = -1;
        return false;
    }

    (void)ftruncate(g_pidFd, 0);
    lseek(g_pidFd, 0, SEEK_SET);
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "%d\n", (int)getpid());
    (void)write(g_pidFd, buf, (size_t)n);
    fsync(g_pidFd);
    // Do NOT close – the lock lives as long as the fd is open
    return true;
}

static void removePidFile() {
    if (g_pidFd >= 0) {
        flock(g_pidFd, LOCK_UN);
        close(g_pidFd);
        g_pidFd = -1;
    }
    unlink(PID_FILE);
}

static void daemonize() {
    // 1. Fork so parent exits → shell prompt returns
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); exit(1); }
    if (pid > 0) exit(0);   // parent exits

    // 2. New session
    if (setsid() < 0) { perror("setsid"); exit(1); }

    // 3. Second fork: detach from session leader so we can never re-acquire a tty
    pid = fork();
    if (pid < 0) { perror("fork2"); exit(1); }
    if (pid > 0) exit(0);

    // 4. Close all unnecessary fds (keep 0/1/2 for now, redirect below)
    int maxFd = (int)sysconf(_SC_OPEN_MAX);
    if (maxFd < 0) maxFd = 1024;
    for (int i = 3; i < maxFd; i++) close(i);

    // 5. Redirect stdin/stdout/stderr to /dev/null
    int devNull = open("/dev/null", O_RDWR);
    if (devNull >= 0) {
        dup2(devNull, STDIN_FILENO);
        dup2(devNull, STDOUT_FILENO);
        dup2(devNull, STDERR_FILENO);
        if (devNull > 2) close(devNull);
    }
}

/* ─────────────────────────────────────────────
   Readers-Writer lock
   ───────────────────────────────────────────── */
class RWLock {
    std::mutex              mtx;
    std::condition_variable cv;
    int  readers = 0;
    bool writer  = false;
public:
    void lockRead()  {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&]{ return !writer; });
        ++readers;
    }
    void unlockRead() {
        std::unique_lock<std::mutex> lk(mtx);
        if (--readers == 0) cv.notify_all();
    }
    void lockWrite() {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&]{ return !writer && readers == 0; });
        writer = true;
    }
    void unlockWrite() {
        std::unique_lock<std::mutex> lk(mtx);
        writer = false;
        cv.notify_all();
    }
};

static RWLock          g_fileLock;
static std::atomic<int> g_nextId{1};

/* ─────────────────────────────────────────────
   Network helpers
   ───────────────────────────────────────────── */
static bool sendLine(int fd, const std::string& line) {
    std::string out = line + "\n";
    size_t sent = 0;
    while (sent < out.size()) {
        ssize_t n = send(fd, out.c_str() + sent, out.size() - sent, 0);
        if (n > 0) { sent += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        return false;
    }
    pdebugLog("SEND fd=" + std::to_string(fd) + " [" + line + "]");
    return true;
}

static bool recvLine(int fd, std::string& line) {
    line.clear();
    char ch;
    for (;;) {
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n == 0) return false;
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (ch == '\n') break;
        if (ch != '\r') line.push_back(ch);
    }
    pdebugLog("RECV fd=" + std::to_string(fd) + " [" + line + "]");
    return true;
}

/* recvLine with a timeout (seconds). Returns false on timeout or error. */
static bool recvLineTimeout(int fd, std::string& line, int timeoutSec) {
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(fd, &fds);
    struct timeval tv{ timeoutSec, 0 };
    int r = select(fd + 1, &fds, nullptr, nullptr, &tv);
    if (r <= 0) return false;   // 0 = timeout, <0 = error
    return recvLine(fd, line);
}

/* Connect to host:port with a timeout. Returns fd or -1. */
static int connectTCP(const std::string& host, int port) {
    addrinfo hints{};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* res = nullptr;
    std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) return -1;

    int fd = -1;
    for (addrinfo* p = res; p; p = p->ai_next) {
        fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;

        // Non-blocking connect with timeout
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        int rc = connect(fd, p->ai_addr, p->ai_addrlen);
        if (rc == 0) { fcntl(fd, F_SETFL, flags); break; }
        if (errno == EINPROGRESS) {
            fd_set wfds;
            FD_ZERO(&wfds);
            FD_SET(fd, &wfds);
            struct timeval tv{ TIMEOUT_SEC, 0 };
            if (select(fd + 1, nullptr, &wfds, nullptr, &tv) > 0) {
                int err = 0;
                socklen_t len = sizeof(err);
                getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
                if (err == 0) { fcntl(fd, F_SETFL, flags); break; }
            }
        }
        close(fd); fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

/* ─────────────────────────────────────────────
   Bulletin-board file operations
   ───────────────────────────────────────────── */
static bool bbRead(int id, std::string& out) {
    g_fileLock.lockRead();
    std::ifstream in(g_cfg.bbfile);
    bool found = false;
    if (in.is_open()) {
        std::string ln;
        while (std::getline(in, ln)) {
            auto sl = ln.find('/');
            if (sl == std::string::npos) continue;
            try {
                if (std::stoi(ln.substr(0, sl)) == id) {
                    out = ln.substr(sl + 1);
                    found = true;
                    break;
                }
            } catch (...) {}
        }
    }
    g_fileLock.unlockRead();
    return found;
}

/* Append a new message; assignedId is set on success */
static bool bbWrite(const std::string& poster, const std::string& msg, int& assignedId) {
    g_fileLock.lockWrite();
    std::ofstream out(g_cfg.bbfile, std::ios::app);
    bool ok = false;
    if (out.is_open()) {
        assignedId = g_nextId.fetch_add(1);
        out << assignedId << "/" << poster << "/" << msg << "\n";
        out.flush();
        ok = out.good();
    }
    g_fileLock.unlockWrite();
    return ok;
}

/* Replace message <id> with new poster/msg. Returns false if id unknown. */
static bool bbReplace(int id, const std::string& poster, const std::string& msg) {
    g_fileLock.lockWrite();
    std::ifstream in(g_cfg.bbfile);
    if (!in.is_open()) { g_fileLock.unlockWrite(); return false; }

    std::vector<std::string> lines;
    bool found = false;
    std::string ln;
    while (std::getline(in, ln)) {
        auto sl = ln.find('/');
        if (sl != std::string::npos) {
            try {
                if (std::stoi(ln.substr(0, sl)) == id) {
                    lines.push_back(std::to_string(id) + "/" + poster + "/" + msg);
                    found = true;
                    continue;
                }
            } catch (...) {}
        }
        lines.push_back(ln);
    }
    in.close();

    if (found) {
        std::ofstream out(g_cfg.bbfile, std::ios::trunc);
        for (auto& l : lines) out << l << "\n";
        out.flush();
    }
    g_fileLock.unlockWrite();
    return found;
}

/* Undo a WRITE: remove the last-written line with the given id */
static void bbUndoWrite(int id) {
    g_fileLock.lockWrite();
    std::ifstream in(g_cfg.bbfile);
    if (!in.is_open()) { g_fileLock.unlockWrite(); return; }

    std::vector<std::string> lines;
    std::string ln;
    bool removed = false;
    while (std::getline(in, ln)) {
        auto sl = ln.find('/');
        if (!removed && sl != std::string::npos) {
            try {
                if (std::stoi(ln.substr(0, sl)) == id) { removed = true; continue; }
            } catch (...) {}
        }
        lines.push_back(ln);
    }
    in.close();
    if (removed) {
        std::ofstream out(g_cfg.bbfile, std::ios::trunc);
        for (auto& l : lines) out << l << "\n";
    }
    g_fileLock.unlockWrite();
}

/* Undo a REPLACE: restore original line – called from slave rollback */

static void initNextId() {
    g_fileLock.lockRead();
    std::ifstream in(g_cfg.bbfile);
    int mx = 0;
    if (in.is_open()) {
        std::string ln;
        while (std::getline(in, ln)) {
            auto sl = ln.find('/');
            if (sl == std::string::npos) continue;
            try { int id = std::stoi(ln.substr(0, sl)); if (id > mx) mx = id; }
            catch (...) {}
        }
    }
    g_fileLock.unlockRead();
    g_nextId = mx + 1;
}

/* ─────────────────────────────────────────────
   2PC – Master side
   ───────────────────────────────────────────── */
/*
 * Protocol lines (plain text, newline-terminated):
 *
 *   Master → Slave:
 *     2PC PRECOMMIT
 *     2PC ABORT
 *     2PC COMMIT WRITE <poster> <message>
 *     2PC COMMIT REPLACE <id> <poster> <message>
 *     2PC SUCCESS
 *     2PC UNSUCCESS
 *
 *   Slave → Master:
 *     2PC ACK OK
 *     2PC ACK FAIL
 *     2PC DONE OK
 *     2PC DONE FAIL
 */

/* Result of contacting one slave */
struct SlaveResult { int fd; bool ackOk; bool doneOk; };

/* Send PRECOMMIT to one peer; return open fd if peer acked OK, else -1 */
static int precommitPeer(const Peer& p) {
    int fd = connectTCP(p.host, p.port);
    if (fd < 0) {
        logMsg("[2PC] cannot connect to " + p.host + ":" + std::to_string(p.port));
        return -1;
    }
    pdebugLog("2PC connected to " + p.host + ":" + std::to_string(p.port));

    sendLine(fd, "2PC PRECOMMIT");

    std::string resp;
    if (!recvLineTimeout(fd, resp, TIMEOUT_SEC) || resp != "2PC ACK OK") {
        logMsg("[2PC] precommit NACK from " + p.host + ":" + std::to_string(p.port));
        close(fd);
        return -1;
    }
    return fd;
}

/* Full 2PC master for WRITE.
   Returns 0 on success (all replicated), -1 on failure.
   On success, assignedId is the id actually written. */
static int twoPhaseWrite(const std::string& poster, const std::string& msg, int& assignedId) {
    const auto& peers = g_cfg.peers;

    // ── Phase 1: PRECOMMIT ──────────────────────
    std::vector<int> slaveFds;
    bool aborted = false;

    for (auto& p : peers) {
        int fd = precommitPeer(p);
        if (fd < 0) { aborted = true; break; }
        slaveFds.push_back(fd);
    }

    if (aborted) {
        // Send ABORT to whoever already acked
        for (int fd : slaveFds) { sendLine(fd, "2PC ABORT"); close(fd); }
        return -1;
    }

    // ── Phase 2: COMMIT ─────────────────────────
    // Construct the commit message
    std::string commitMsg = "2PC COMMIT WRITE " + poster + " " + msg;
    for (int fd : slaveFds) sendLine(fd, commitMsg);

    // Now master performs its own write
    bool masterOk = bbWrite(poster, msg, assignedId);

    // Collect slave DONE responses
    bool allOk = masterOk;
    for (int fd : slaveFds) {
        std::string resp;
        if (!recvLineTimeout(fd, resp, TIMEOUT_SEC) || resp != "2PC DONE OK") {
            allOk = false;
        }
    }

    // ── Outcome broadcast ───────────────────────
    if (allOk) {
        for (int fd : slaveFds) { sendLine(fd, "2PC SUCCESS"); close(fd); }
        return 0;
    } else {
        for (int fd : slaveFds) { sendLine(fd, "2PC UNSUCCESS"); close(fd); }
        if (masterOk) bbUndoWrite(assignedId);
        return -1;
    }
}

/* Full 2PC master for REPLACE. Returns 0 on success, -1 on failure. */
static int twoPhaseReplace(int id, const std::string& poster, const std::string& msg) {
    const auto& peers = g_cfg.peers;

    // ── Phase 1: PRECOMMIT ──────────────────────
    std::vector<int> slaveFds;
    bool aborted = false;
    for (auto& p : peers) {
        int fd = precommitPeer(p);
        if (fd < 0) { aborted = true; break; }
        slaveFds.push_back(fd);
    }
    if (aborted) {
        for (int fd : slaveFds) { sendLine(fd, "2PC ABORT"); close(fd); }
        return -1;
    }

    // ── Phase 2: COMMIT ─────────────────────────
    std::string commitMsg = "2PC COMMIT REPLACE " + std::to_string(id) + " " + poster + " " + msg;
    for (int fd : slaveFds) sendLine(fd, commitMsg);

    // Save original for master rollback
    std::string origContent;
    bool existed = bbRead(id, origContent);
    bool masterOk = existed && bbReplace(id, poster, msg);

    bool allOk = masterOk;
    for (int fd : slaveFds) {
        std::string resp;
        if (!recvLineTimeout(fd, resp, TIMEOUT_SEC) || resp != "2PC DONE OK") {
            allOk = false;
        }
    }

    if (allOk) {
        for (int fd : slaveFds) { sendLine(fd, "2PC SUCCESS"); close(fd); }
        return 0;
    } else {
        for (int fd : slaveFds) { sendLine(fd, "2PC UNSUCCESS"); close(fd); }
        if (masterOk) {
            // Restore original
            std::string origPoster, origMsg;
            auto sl = origContent.find('/');
            if (sl != std::string::npos) {
                origPoster = origContent.substr(0, sl);
                origMsg    = origContent.substr(sl + 1);
            }
            bbReplace(id, origPoster, origMsg);
        }
        return -1;
    }
}

/* ─────────────────────────────────────────────
   2PC – Slave side (one connection)
   ───────────────────────────────────────────── */
/*
 * Slave state machine:
 *   Idle → receive "2PC PRECOMMIT"
 *     → if busy: "2PC ACK FAIL"; done
 *     → else: "2PC ACK OK"
 *   Precommit acknowledged → receive "2PC COMMIT ..." or "2PC ABORT"
 *     → ABORT: idle
 *     → COMMIT WRITE <poster> <msg>: perform bbWrite; "2PC DONE OK/FAIL"
 *     → COMMIT REPLACE <id> <poster> <msg>: perform bbReplace; "2PC DONE OK/FAIL"
 *   Committed → receive "2PC SUCCESS" or "2PC UNSUCCESS"
 *     → UNSUCCESS: undo; idle
 *     → SUCCESS: idle
 */

// "Slave busy" flag – only one 2PC at a time per server
static std::mutex              g_slaveMtx;
static std::atomic<bool>       g_slaveBusy{false};

static void handleReplica(int fd) {
    std::string line;

    // Expect PRECOMMIT
    if (!recvLineTimeout(fd, line, TIMEOUT_SEC) || line != "2PC PRECOMMIT") {
        close(fd); return;
    }

    // Can we participate?
    bool gotLock = false;
    {
        std::lock_guard<std::mutex> lk(g_slaveMtx);
        if (!g_slaveBusy) { g_slaveBusy = true; gotLock = true; }
    }

    if (!gotLock) {
        sendLine(fd, "2PC ACK FAIL");
        close(fd);
        return;
    }
    sendLine(fd, "2PC ACK OK");

    // Wait for COMMIT or ABORT
    if (!recvLineTimeout(fd, line, TIMEOUT_SEC)) {
        g_slaveBusy = false; close(fd); return;
    }

    if (line == "2PC ABORT") {
        logMsg("[2PC SLAVE] received ABORT");
        g_slaveBusy = false; close(fd); return;
    }

    // Parse COMMIT
    bool opOk = false;
    int  writeId = -1;
    bool didWrite   = false;
    bool didReplace = false;
    std::string savedOrigContent;  // for replace rollback
    int  replaceId  = -1;

    if (line.rfind("2PC COMMIT WRITE ", 0) == 0) {
        // "2PC COMMIT WRITE <poster> <msg>"
        std::string rest = line.substr(17);  // after "2PC COMMIT WRITE "
        auto sp = rest.find(' ');
        std::string poster = (sp != std::string::npos) ? rest.substr(0, sp) : rest;
        std::string msg    = (sp != std::string::npos) ? rest.substr(sp + 1) : "";
        opOk = bbWrite(poster, msg, writeId);
        didWrite = opOk;
    }
    else if (line.rfind("2PC COMMIT REPLACE ", 0) == 0) {
        // "2PC COMMIT REPLACE <id> <poster> <msg>"
        std::string rest = line.substr(19);  // after "2PC COMMIT REPLACE "
        std::istringstream is(rest);
        std::string idStr, poster, msg;
        is >> idStr >> poster;
        std::getline(is, msg);
        if (!msg.empty() && msg[0] == ' ') msg = msg.substr(1);
        replaceId = std::stoi(idStr);
        // Save original for rollback
        bbRead(replaceId, savedOrigContent);
        opOk = bbReplace(replaceId, poster, msg);
        didReplace = opOk;
    }

    sendLine(fd, opOk ? "2PC DONE OK" : "2PC DONE FAIL");

    // Wait for SUCCESS / UNSUCCESS
    if (!recvLineTimeout(fd, line, TIMEOUT_SEC)) {
        // Timeout → rollback to be safe
        if (didWrite)   bbUndoWrite(writeId);
        if (didReplace) {
            auto sl = savedOrigContent.find('/');
            std::string op = (sl!=std::string::npos)?savedOrigContent.substr(0,sl):"";
            std::string om = (sl!=std::string::npos)?savedOrigContent.substr(sl+1):"";
            bbReplace(replaceId, op, om);
        }
        g_slaveBusy = false; close(fd); return;
    }

    if (line == "2PC UNSUCCESS") {
        logMsg("[2PC SLAVE] rolling back");
        if (didWrite)   bbUndoWrite(writeId);
        if (didReplace) {
            auto sl = savedOrigContent.find('/');
            std::string op = (sl!=std::string::npos)?savedOrigContent.substr(0,sl):"";
            std::string om = (sl!=std::string::npos)?savedOrigContent.substr(sl+1):"";
            bbReplace(replaceId, op, om);
        }
    } else {
        logMsg("[2PC SLAVE] committed");
    }

    g_slaveBusy = false;
    close(fd);
}

/* ─────────────────────────────────────────────
   Client session handler
   ───────────────────────────────────────────── */
static void handleClient(int clientFd) {
    std::string user = "anonymous";
    sendLine(clientFd, "0.0 WELCOME rbbserv: USER READ WRITE REPLACE QUIT");

    std::string line;
    while (recvLine(clientFd, line)) {
        if (g_doQuit || g_doRestart) break;   // server shutting down

        if (line.rfind("USER ", 0) == 0) {
            std::string name = line.substr(5);
            if (name.find('/') != std::string::npos) {
                sendLine(clientFd, "1.2 BAD " + name + " invalid user name");
            } else {
                user = name;
                sendLine(clientFd, "1.0 HELLO " + name + " welcome");
            }
        }
        else if (line.rfind("READ ", 0) == 0) {
            try {
                int id = std::stoi(line.substr(5));
                std::string content;
                if (bbRead(id, content)) {
                    sendLine(clientFd, "2.0 MESSAGE " + std::to_string(id) + " " + content);
                } else {
                    sendLine(clientFd, "2.1 UNKNOWN " + std::to_string(id) + " no such message");
                }
            } catch (...) {
                sendLine(clientFd, "2.2 ERROR READ invalid message number");
            }
        }
        else if (line.rfind("WRITE ", 0) == 0) {
            std::string msg = line.substr(6);
            if (g_cfg.peers.empty()) {
                // standalone
                int newId = 0;
                if (bbWrite(user, msg, newId)) {
                    sendLine(clientFd, "3.0 WROTE " + std::to_string(newId));
                } else {
                    sendLine(clientFd, "3.2 ERROR WRITE could not store message");
                }
            } else {
                int newId = 0;
                if (twoPhaseWrite(user, msg, newId) == 0) {
                    sendLine(clientFd, "3.0 WROTE " + std::to_string(newId));
                } else {
                    sendLine(clientFd, "3.2 ERROR WRITE replication failed");
                }
            }
        }
        else if (line.rfind("REPLACE ", 0) == 0) {
            // REPLACE <id>/<poster>/<message>
            std::string rest = line.substr(8);
            auto sl1 = rest.find('/');
            auto sl2 = (sl1 != std::string::npos) ? rest.find('/', sl1+1) : std::string::npos;
            if (sl1 == std::string::npos || sl2 == std::string::npos) {
                sendLine(clientFd, "3.2 ERROR REPLACE bad format");
            } else {
                try {
                    int id          = std::stoi(rest.substr(0, sl1));
                    std::string poster = rest.substr(sl1+1, sl2-sl1-1);
                    std::string msg    = rest.substr(sl2+1);
                    if (g_cfg.peers.empty()) {
                        if (bbReplace(id, poster, msg)) {
                            sendLine(clientFd, "3.0 REPLACED " + std::to_string(id));
                        } else {
                            sendLine(clientFd, "3.2 ERROR REPLACE no such message");
                        }
                    } else {
                        if (twoPhaseReplace(id, poster, msg) == 0) {
                            sendLine(clientFd, "3.0 REPLACED " + std::to_string(id));
                        } else {
                            sendLine(clientFd, "3.2 ERROR REPLACE replication failed or no such message");
                        }
                    }
                } catch (...) {
                    sendLine(clientFd, "3.2 ERROR REPLACE bad message id");
                }
            }
        }
        else if (line.rfind("QUIT", 0) == 0) {
            sendLine(clientFd, "9.0 BYE goodbye");
            break;
        }
        else {
            sendLine(clientFd, "5.0 ERROR unknown command");
        }
    }

    shutdown(clientFd, SHUT_RDWR);
    close(clientFd);
}

/* ─────────────────────────────────────────────
   Thread pool
   ───────────────────────────────────────────── */
class ThreadPool {
public:
    ThreadPool() = default;
    ~ThreadPool() { shutdown(); }

    // Allocate 'n' worker threads
    void spawn(int n) {
        std::lock_guard<std::mutex> lk(mtx);
        for (int i = 0; i < n; ++i) {
            workers.emplace_back([this]{ workerLoop(); });
            ++totalWorkers;
        }
    }

    // Enqueue a client fd; returns false if queue is full / shutting down
    bool enqueue(int fd) {
        std::unique_lock<std::mutex> lk(mtx);
        if (stopping) return false;
        q.push(fd);
        cv.notify_one();
        return true;
    }

    // Graceful shutdown: signal all threads to stop, join them
    void shutdown() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            stopping = true;
        }
        cv.notify_all();
        for (auto& t : workers) if (t.joinable()) t.join();
        workers.clear();
        totalWorkers = 0;
    }

    int size() const { return totalWorkers; }

private:
    std::queue<int>         q;
    std::vector<std::thread> workers;
    std::mutex              mtx;
    std::condition_variable cv;
    std::atomic<bool>       stopping{false};
    int                     totalWorkers{0};

    void workerLoop() {
        for (;;) {
            int fd = -1;
            {
                std::unique_lock<std::mutex> lk(mtx);
                cv.wait(lk, [&]{ return stopping || !q.empty(); });
                if (stopping && q.empty()) return;
                fd = q.front(); q.pop();
            }
            handleClient(fd);
        }
    }
};

/* ─────────────────────────────────────────────
   Listener sockets (client + replica)
   ───────────────────────────────────────────── */
static int makeListener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons((uint16_t)port);
    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0 ||
        listen(fd, 64) < 0) {
        close(fd); return -1;
    }
    return fd;
}

/* ─────────────────────────────────────────────
   Main accept loops
   ───────────────────────────────────────────── */
static std::atomic<bool> g_running{true};
static int g_clientListenFd = -1;
static int g_replicaListenFd = -1;

// Thread: accepts replica (2PC slave) connections
static void replicaAcceptLoop() {
    while (g_running) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(g_replicaListenFd, &fds);
        struct timeval tv{1, 0};
        int r = select(g_replicaListenFd + 1, &fds, nullptr, nullptr, &tv);
        if (r <= 0) continue;
        int fd = accept(g_replicaListenFd, nullptr, nullptr);
        if (fd < 0) { if (errno == EINTR) continue; break; }
        // Handle replica in detached thread
        std::thread(handleReplica, fd).detach();
    }
}

/* ─────────────────────────────────────────────
   Server lifecycle
   ───────────────────────────────────────────── */
static void runServer();

int main(int argc, char* argv[]) {
    if (argc == 2)       g_configPath = argv[1];
    else if (argc > 2) { fprintf(stderr, "Usage: rbbserv [config]\n"); return 1; }

    if (!loadConfig(g_configPath, g_cfg)) return 1;

    // Create safe dir
    mkdir(SAFE_DIR, 0755);

    // Daemonize unless FOREGROUND
    if (!g_cfg.foreground) {
        daemonize();
        // After fork, open log file
        g_logFp = fopen(LOG_FILE, "a");
    }

    // PID file (in safe dir)
    if (!writePidFile()) return 1;

    // Move CWD to safe dir so relative paths go there
    // (bbfile and config were already opened/stored as strings)
    if (chdir(SAFE_DIR) != 0) { logMsg("WARNING: chdir to safe dir failed"); }

    // Ensure bbfile exists (use absolute-ish path; if not absolute, prepend ../)
    {
        std::string& bf = g_cfg.bbfile;
        if (bf[0] != '/') bf = "../" + bf;
    }

    {
        int fd = open(g_cfg.bbfile.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd >= 0) close(fd);
    }
    initNextId();

    setupSignals(g_cfg.foreground);

    logMsg("rbbserv starting on client-port=" + std::to_string(g_cfg.bbport)
           + " replica-port=" + std::to_string(g_cfg.rport)
           + " peers=" + std::to_string(g_cfg.peers.size()));

    // Main loop: run → SIGHUP → restart → run → SIGQUIT → stop
    for (;;) {
        g_doRestart = false;
        g_doQuit    = false;
        runServer();

        if (g_doQuit) {
            logMsg("rbbserv shutting down (SIGQUIT/SIGINT)");
            break;
        }
        if (g_doRestart) {
            logMsg("rbbserv restarting (SIGHUP)");
            // Re-read config
            Config newCfg;
            if (loadConfig(g_configPath, newCfg)) g_cfg = newCfg;
            // Re-open log if needed
            if (!g_cfg.foreground && g_logFp) {
                fclose(g_logFp);
                g_logFp = fopen(LOG_FILE, "a");
            }
        }
    }

    removePidFile();
    if (g_logFp) fclose(g_logFp);
    return 0;
}

/* One server run epoch (until signal fires) */
static void runServer() {
    g_running = true;

    g_clientListenFd = makeListener(g_cfg.bbport);
    if (g_clientListenFd < 0) { logMsg("ERROR: cannot bind client port"); return; }

    g_replicaListenFd = makeListener(g_cfg.rport);
    if (g_replicaListenFd < 0) { logMsg("ERROR: cannot bind replica port"); return; }

    ThreadPool pool;
    pool.spawn(g_cfg.thincr);   // pre-allocate initial threads

    // Start replica accept loop in background
    std::thread replicaThread(replicaAcceptLoop);

    logMsg("Listening: clients=" + std::to_string(g_cfg.bbport)
           + " replicas=" + std::to_string(g_cfg.rport));

    // Client accept loop
    while (!g_doQuit && !g_doRestart) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(g_clientListenFd, &fds);
        struct timeval tv{1, 0};
        int r = select(g_clientListenFd + 1, &fds, nullptr, nullptr, &tv);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) continue;

        int clientFd = accept(g_clientListenFd, nullptr, nullptr);
        if (clientFd < 0) { if (errno == EINTR) continue; break; }

        // Grow pool if needed
        if (pool.size() < g_cfg.thmax) pool.spawn(g_cfg.thincr);

        if (!pool.enqueue(clientFd)) { close(clientFd); }
    }

    // Shutdown
    g_running = false;
    pool.shutdown();

    replicaThread.join();

    close(g_clientListenFd);  g_clientListenFd  = -1;
    close(g_replicaListenFd); g_replicaListenFd = -1;
}
