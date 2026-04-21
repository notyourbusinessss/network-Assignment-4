
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cerrno>

#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

/* ─────────────────────────────────────────────
   Constants
   ───────────────────────────────────────────── */
static const int    TIMEOUT_SEC  = 5;
static const char*  SAFE_DIR     = "run";
static const char*  PID_FILE     = "run/rbbserv.pid";
static const char*  LOG_FILE     = "run/bbserv.log";
static const char*  DEFAULT_CONF = "bbserv.conf";

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
Config            g_cfg;
std::string       g_configPath = DEFAULT_CONF;
std::string       g_origCwd;     // cwd at startup, for resolving relative BBFILE after chdir / SIGHUP
std::mutex        g_logMtx;
int               g_pidFd      = -1;
FILE*             g_logFp      = nullptr;

std::vector<std::thread> g_workers;

std::queue<int>          g_queue;
std::mutex               g_queueMtx;
std::condition_variable  g_queueCv;

std::mutex        g_slaveMtx;
std::mutex        g_fileMtx;
std::atomic<bool> g_slaveBusy{false};
std::atomic<bool> g_running{true};

std::atomic<int>  g_nextId{1};
std::atomic<bool> g_doRestart{false};
std::atomic<bool> g_doQuit{false};

/* ─────────────────────────────────────────────
   Helpers
   ───────────────────────────────────────────── */
bool strToBool(const std::string& s) {
    return s == "true" || s == "1";
}

std::string trimCR(const std::string& s) {
    std::string out = s;
    while (!out.empty() && (out.back() == '\r' || out.back() == '\n')) {
        out.pop_back();
    }
    return out;
}

void logMsg(const std::string& msg) {
    std::lock_guard<std::mutex> lk(g_logMtx);
    if (g_logFp) {
        fprintf(g_logFp, "%s\n", msg.c_str());
        fflush(g_logFp);
    } else {
        fprintf(stdout, "%s\n", msg.c_str());
        fflush(stdout);
    }
}

/* ─────────────────────────────────────────────
   Initialize
   ───────────────────────────────────────────── */
bool initialize(const char* configPath, Config& cfg) {
    // Parse into a local Config so a failed reload doesn't clobber the
    // currently-running configuration. On success we move the result into cfg,
    // which also ensures PEER lines and other fields don't accumulate across
    // SIGHUP reloads.
    Config next;

    int fd = open(configPath, O_RDONLY);
    if (fd < 0) {
        perror("open config");
        return false;
    }

    std::string file;
    char buf[1024];

    while (true) {
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n > 0) {
            file.append(buf, static_cast<size_t>(n));
        }
        else if (n == 0) {
            break;
        }
        else {
            if (errno == EINTR) continue;
            perror("read config");
            close(fd);
            return false;
        }
    }
    close(fd);

    std::size_t start = 0;
    while (start < file.size()) {
        std::size_t end = file.find('\n', start);
        if (end == std::string::npos) end = file.size();

        std::string line = file.substr(start, end - start);
        line = trimCR(line);

        if (!line.empty() && line[0] != '#') {
            std::stringstream ss(line);
            std::string key, val;
            ss >> key >> val;

            if (key == "THMAX" && !val.empty()) {
                next.thmax = std::stoi(val);
            }
            else if (key == "THINCR" && !val.empty()) {
                next.thincr = std::stoi(val);
            }
            else if (key == "BBPORT" && !val.empty()) {
                next.bbport = std::stoi(val);
            }
            else if (key == "FOREGROUND" && !val.empty()) {
                next.foreground = strToBool(val);
            }
            else if (key == "PDEBUG" && !val.empty()) {
                next.pdebug = strToBool(val);
            }
            else if (key == "RPORT" && !val.empty()) {
                next.rport = std::stoi(val);
            }
            else if (key == "BBFILE" && !val.empty()) {
                next.bbfile = val;
            }
            else if (key == "PEER" && !val.empty()) {
                auto col = val.rfind(':');
                if (col != std::string::npos) {
                    Peer p;
                    p.host = val.substr(0, col);
                    p.port = std::stoi(val.substr(col + 1));
                    next.peers.push_back(p);
                }
            }
        }

        start = end + 1;
    }

    if (next.bbfile.empty()) {
        std::cerr << "Configuration error: BBFILE is required\n";
        return false;
    }

    std::cout << "THMAX      = " << next.thmax      << "\n";
    std::cout << "THINCR     = " << next.thincr     << "\n";
    std::cout << "BBPORT     = " << next.bbport     << "\n";
    std::cout << "FOREGROUND = " << (next.foreground ? "true" : "false") << "\n";
    std::cout << "PDEBUG     = " << (next.pdebug     ? "true" : "false") << "\n";
    std::cout << "RPORT      = " << next.rport      << "\n";
    std::cout << "BBFILE     = " << next.bbfile     << "\n";
    std::cout << "PEERS      = " << next.peers.size() << "\n";
    for (auto& p : next.peers) {
        std::cout << "  PEER     = " << p.host << ":" << p.port << "\n";
    }
    std::cout.flush();

    cfg = std::move(next);
    return true;
}

/* ─────────────────────────────────────────────
   Daemonize
   ───────────────────────────────────────────── */
void daemonize() {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); exit(1); }
    if (pid > 0) exit(0);

    if (setsid() < 0) { perror("setsid"); exit(1); }

    pid = fork();
    if (pid < 0) { perror("fork2"); exit(1); }
    if (pid > 0) exit(0);

    int maxFd = (int)sysconf(_SC_OPEN_MAX);
    if (maxFd < 0) maxFd = 1024;
    for (int i = 3; i < maxFd; i++) close(i);

    int devNull = open("/dev/null", O_RDWR);
    if (devNull >= 0) {
        dup2(devNull, STDIN_FILENO);
        dup2(devNull, STDOUT_FILENO);
        dup2(devNull, STDERR_FILENO);
        if (devNull > 2) close(devNull);
    }
}

/* ─────────────────────────────────────────────
   PID file
   ───────────────────────────────────────────── */
void removePidFile() {
    if (g_pidFd >= 0) {
        flock(g_pidFd, LOCK_UN);
        close(g_pidFd);
        g_pidFd = -1;
    }
    unlink(PID_FILE);
}

/* ─────────────────────────────────────────────
   Signals
   ───────────────────────────────────────────── */
void sigHandler(int sig) {
    if (sig == SIGHUP)  g_doRestart = true;
    if (sig == SIGQUIT) g_doQuit    = true;
    if (sig == SIGINT)  g_doQuit    = true;
}

void setupSignals(bool foreground) {
    struct sigaction sa{};
    sigemptyset(&sa.sa_mask);
    sa.sa_flags   = 0;
    sa.sa_handler = sigHandler;

    sigaction(SIGHUP,  &sa, nullptr);
    sigaction(SIGQUIT, &sa, nullptr);

    if (foreground) {
        sigaction(SIGINT, &sa, nullptr);
    } else {
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
   Next message ID
   ───────────────────────────────────────────── */
void initNextId() {
    std::ifstream in(g_cfg.bbfile);
    int maxId = 0;
    std::string line;
    while (std::getline(in, line)) {
        auto slash = line.find('/');
        if (slash == std::string::npos) continue;
        try {
            int id = std::stoi(line.substr(0, slash));
            if (id > maxId) maxId = id;
        } catch (...) {}
    }
    g_nextId = maxId + 1;
}

/* ─────────────────────────────────────────────
   Network helpers
   ───────────────────────────────────────────── */
bool sendLine(int fd, const std::string& line) {
    std::string out = line + "\n";
    size_t sent = 0;
    while (sent < out.size()) {
        ssize_t n = send(fd, out.c_str() + sent, out.size() - sent, 0);
        if (n > 0) {
            sent += (size_t)n;
        }
        else if (n < 0 && errno == EINTR) {
            continue;
        }
        else {
            return false;
        }
    }
    if (g_cfg.pdebug) logMsg("SEND [" + line + "]");
    return true;
}

bool recvLine(int fd, std::string& line) {
    line.clear();
    char ch;
    while (true) {
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n == 0) {
            return false;
        }
        else if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (ch == '\n') break;
        if (ch != '\r') line.push_back(ch);
    }
    if (g_cfg.pdebug) logMsg("RECV [" + line + "]");
    return true;
}

bool recvLineTimeout(int fd, std::string& line, int timeoutSec) {
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(fd, &fds);
    struct timeval tv{ timeoutSec, 0 };
    int r = select(fd + 1, &fds, nullptr, nullptr, &tv);
    if (r <= 0) return false;
    return recvLine(fd, line);
}

int connectTCP(const std::string& host, int port) {
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

        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        int rc = connect(fd, p->ai_addr, p->ai_addrlen);
        if (rc == 0) {
            fcntl(fd, F_SETFL, flags);
            break;
        }
        if (errno == EINPROGRESS) {
            fd_set wfds;
            FD_ZERO(&wfds);
            FD_SET(fd, &wfds);
            struct timeval tv{ TIMEOUT_SEC, 0 };
            if (select(fd + 1, nullptr, &wfds, nullptr, &tv) > 0) {
                int err = 0;
                socklen_t len = sizeof(err);
                getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
                if (err == 0) {
                    fcntl(fd, F_SETFL, flags);
                    break;
                }
            }
        }
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

/* ─────────────────────────────────────────────
   Bulletin board file operations
   ───────────────────────────────────────────── */
bool readMessageById(int id, std::string& out) {
    std::lock_guard<std::mutex> lk(g_fileMtx);
    std::ifstream in(g_cfg.bbfile);
    if (!in.is_open()) return false;
    std::string line;
    while (std::getline(in, line)) {
        auto sl = line.find('/');
        if (sl == std::string::npos) continue;
        try {
            if (std::stoi(line.substr(0, sl)) == id) {
                out = line.substr(sl + 1);
                return true;
            }
        } catch (...) {}
    }
    return false;
}

bool appendMessage(const std::string& poster, const std::string& msg, int& assignedId) {
    std::lock_guard<std::mutex> lk(g_fileMtx);
    std::ofstream out(g_cfg.bbfile, std::ios::app);
    if (!out.is_open()) return false;
    assignedId = g_nextId.fetch_add(1);
    out << assignedId << "/" << poster << "/" << msg << "\n";
    out.flush();
    return out.good();
}

bool replaceMessage(int id, const std::string& poster, const std::string& msg) {
    std::lock_guard<std::mutex> lk(g_fileMtx);
    std::ifstream in(g_cfg.bbfile);
    if (!in.is_open()) return false;

    std::vector<std::string> lines;
    std::string line;
    bool found = false;

    while (std::getline(in, line)) {
        auto sl = line.find('/');
        if (sl != std::string::npos) {
            try {
                if (std::stoi(line.substr(0, sl)) == id) {
                    lines.push_back(std::to_string(id) + "/" + poster + "/" + msg);
                    found = true;
                    continue;
                }
            } catch (...) {}
        }
        lines.push_back(line);
    }
    in.close();

    if (!found) return false;

    std::ofstream out(g_cfg.bbfile, std::ios::trunc);
    for (auto& l : lines) out << l << "\n";
    out.flush();
    return true;
}

void undoWrite(int id) {
    std::lock_guard<std::mutex> lk(g_fileMtx);
    std::ifstream in(g_cfg.bbfile);
    if (!in.is_open()) return;

    std::vector<std::string> lines;
    std::string line;
    bool removed = false;

    while (std::getline(in, line)) {
        auto sl = line.find('/');
        if (!removed && sl != std::string::npos) {
            try {
                if (std::stoi(line.substr(0, sl)) == id) {
                    removed = true;
                    continue;
                }
            } catch (...) {}
        }
        lines.push_back(line);
    }
    in.close();

    if (removed) {
        std::ofstream out(g_cfg.bbfile, std::ios::trunc);
        for (auto& l : lines) out << l << "\n";
    }
}

/* ─────────────────────────────────────────────
   Two phase commit - master
   ───────────────────────────────────────────── */
int twoPhaseWrite(const std::string& poster, const std::string& msg, int& assignedId) {
    std::vector<int> slaveFds;

    // phase 1 - precommit
    for (auto& p : g_cfg.peers) {
        int fd = connectTCP(p.host, p.port);
        if (fd < 0) {
            logMsg("[2PC] cannot connect to " + p.host + ":" + std::to_string(p.port));
            for (int sfd : slaveFds) { sendLine(sfd, "2PC ABORT"); close(sfd); }
            return -1;
        }
        sendLine(fd, "2PC PRECOMMIT");
        std::string resp;
        if (!recvLineTimeout(fd, resp, TIMEOUT_SEC) || resp != "2PC ACK OK") {
            logMsg("[2PC] precommit failed from " + p.host);
            close(fd);
            for (int sfd : slaveFds) { sendLine(sfd, "2PC ABORT"); close(sfd); }
            return -1;
        }
        slaveFds.push_back(fd);
    }

    // phase 2 - commit
    std::string commitMsg = "2PC COMMIT WRITE " + poster + " " + msg;
    for (int fd : slaveFds) sendLine(fd, commitMsg);

    bool masterOk = appendMessage(poster, msg, assignedId);

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
        if (masterOk) undoWrite(assignedId);
        return -1;
    }
}

int twoPhaseReplace(int id, const std::string& poster, const std::string& msg) {
    std::vector<int> slaveFds;

    // phase 1 - precommit
    for (auto& p : g_cfg.peers) {
        int fd = connectTCP(p.host, p.port);
        if (fd < 0) {
            logMsg("[2PC] cannot connect to " + p.host + ":" + std::to_string(p.port));
            for (int sfd : slaveFds) { sendLine(sfd, "2PC ABORT"); close(sfd); }
            return -1;
        }
        sendLine(fd, "2PC PRECOMMIT");
        std::string resp;
        if (!recvLineTimeout(fd, resp, TIMEOUT_SEC) || resp != "2PC ACK OK") {
            logMsg("[2PC] precommit failed from " + p.host);
            close(fd);
            for (int sfd : slaveFds) { sendLine(sfd, "2PC ABORT"); close(sfd); }
            return -1;
        }
        slaveFds.push_back(fd);
    }

    // phase 2 - commit
    std::string commitMsg = "2PC COMMIT REPLACE " + std::to_string(id) + " " + poster + " " + msg;
    for (int fd : slaveFds) sendLine(fd, commitMsg);

    // save original for rollback
    std::string origContent;
    bool existed = readMessageById(id, origContent);
    bool masterOk = existed && replaceMessage(id, poster, msg);

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
            auto sl = origContent.find('/');
            if (sl != std::string::npos) {
                replaceMessage(id,
                               origContent.substr(0, sl),
                               origContent.substr(sl + 1));
            }
        }
        return -1;
    }
}

/* ─────────────────────────────────────────────
   Two phase commit - slave
   ───────────────────────────────────────────── */
void handleReplica(int fd) {
    std::string line;

    // step 1 - receive PRECOMMIT
    if (!recvLineTimeout(fd, line, TIMEOUT_SEC) || line != "2PC PRECOMMIT") {
        close(fd);
        return;
    }

    // check if we are free
    {
        std::lock_guard<std::mutex> lk(g_slaveMtx);
        if (g_slaveBusy) {
            sendLine(fd, "2PC ACK FAIL");
            close(fd);
            return;
        }
        g_slaveBusy = true;
    }
    sendLine(fd, "2PC ACK OK");

    // step 2 - receive COMMIT or ABORT
    if (!recvLineTimeout(fd, line, TIMEOUT_SEC)) {
        g_slaveBusy = false;
        close(fd);
        return;
    }

    if (line == "2PC ABORT") {
        logMsg("[2PC SLAVE] aborted");
        g_slaveBusy = false;
        close(fd);
        return;
    }

    bool opOk      = false;
    int  writeId   = -1;
    int  replaceId = -1;
    bool didWrite   = false;
    bool didReplace = false;
    std::string savedOrigContent;

    if (line.rfind("2PC COMMIT WRITE ", 0) == 0) {
        std::string rest = line.substr(17);
        auto sp = rest.find(' ');
        std::string poster = (sp != std::string::npos) ? rest.substr(0, sp) : rest;
        std::string msg    = (sp != std::string::npos) ? rest.substr(sp + 1) : "";
        opOk = appendMessage(poster, msg, writeId);
        didWrite = opOk;
    }
    else if (line.rfind("2PC COMMIT REPLACE ", 0) == 0) {
        std::string rest = line.substr(19);
        std::istringstream is(rest);
        std::string idStr, poster, msg;
        is >> idStr >> poster;
        std::getline(is, msg);
        if (!msg.empty() && msg[0] == ' ') msg = msg.substr(1);
        replaceId = std::stoi(idStr);
        readMessageById(replaceId, savedOrigContent);
        opOk = replaceMessage(replaceId, poster, msg);
        didReplace = opOk;
    }

    sendLine(fd, opOk ? "2PC DONE OK" : "2PC DONE FAIL");

    // step 3 - receive SUCCESS or UNSUCCESS
    if (!recvLineTimeout(fd, line, TIMEOUT_SEC)) {
        logMsg("[2PC SLAVE] timeout - rolling back");
        if (didWrite) undoWrite(writeId);
        if (didReplace) {
            auto sl = savedOrigContent.find('/');
            if (sl != std::string::npos) {
                replaceMessage(replaceId,
                               savedOrigContent.substr(0, sl),
                               savedOrigContent.substr(sl + 1));
            }
        }
        g_slaveBusy = false;
        close(fd);
        return;
    }

    if (line == "2PC UNSUCCESS") {
        logMsg("[2PC SLAVE] rolling back");
        if (didWrite) undoWrite(writeId);
        if (didReplace) {
            auto sl = savedOrigContent.find('/');
            if (sl != std::string::npos) {
                replaceMessage(replaceId,
                               savedOrigContent.substr(0, sl),
                               savedOrigContent.substr(sl + 1));
            }
        }
    } else {
        logMsg("[2PC SLAVE] committed");
    }

    g_slaveBusy = false;
    close(fd);
}

/* ─────────────────────────────────────────────
   Client handler
   ───────────────────────────────────────────── */
void handleClient(int clientFd) {
    std::string currentUser = "anonymous coward";

    sendLine(clientFd, "0.0 WELCOME ver 1.0: USER READ WRITE REPLACE QUIT spoken here");

    std::string line;
    while (recvLine(clientFd, line)) {
        if (g_doQuit || g_doRestart) break;

        if (line.rfind("USER ", 0) == 0) {
            std::string name = line.substr(5);
            if (name.find('/') != std::string::npos) {
                sendLine(clientFd, "1.2 BAD " + name + " invalid user name");
            } else {
                currentUser = name;
                sendLine(clientFd, "1.0 HELLO " + name + " welcome");
            }
        }
        else if (line.rfind("READ ", 0) == 0) {
            std::string numStr = line.substr(5);
            try {
                int id = std::stoi(numStr);
                std::string content;
                bool found = readMessageById(id, content);
                if (found) {
                    sendLine(clientFd, "2.0 MESSAGE " + std::to_string(id) + " " + content);
                } else {
                    sendLine(clientFd, "2.1 UNKNOWN " + std::to_string(id) + " no such message");
                }
            } catch (...) {
                sendLine(clientFd, "2.2 ERROR READ invalid message number");
            }
        }
        else if (line.rfind("WRITE ", 0) == 0) {
            std::string message = line.substr(6);
            int newId = 0;
            if (g_cfg.peers.empty()) {
                if (appendMessage(currentUser, message, newId)) {
                    sendLine(clientFd, "3.0 WROTE " + std::to_string(newId));
                } else {
                    sendLine(clientFd, "3.2 ERROR WRITE could not store message");
                }
            } else {
                if (twoPhaseWrite(currentUser, message, newId) == 0) {
                    sendLine(clientFd, "3.0 WROTE " + std::to_string(newId));
                } else {
                    sendLine(clientFd, "3.2 ERROR WRITE replication failed");
                }
            }
        }
        else if (line.rfind("REPLACE ", 0) == 0) {
            std::string rest = line.substr(8);
            auto sl1 = rest.find('/');
            auto sl2 = (sl1 != std::string::npos) ? rest.find('/', sl1 + 1) : std::string::npos;
            if (sl1 == std::string::npos || sl2 == std::string::npos) {
                sendLine(clientFd, "3.2 ERROR REPLACE bad format");
            } else {
                try {
                    int id             = std::stoi(rest.substr(0, sl1));
                    std::string poster = rest.substr(sl1 + 1, sl2 - sl1 - 1);
                    std::string msg    = rest.substr(sl2 + 1);
                    if (g_cfg.peers.empty()) {
                        if (replaceMessage(id, poster, msg)) {
                            sendLine(clientFd, "3.0 REPLACED " + std::to_string(id));
                        } else {
                            sendLine(clientFd, "3.2 ERROR REPLACE no such message");
                        }
                    } else {
                        if (twoPhaseReplace(id, poster, msg) == 0) {
                            sendLine(clientFd, "3.0 REPLACED " + std::to_string(id));
                        } else {
                            sendLine(clientFd, "3.2 ERROR REPLACE replication failed");
                        }
                    }
                } catch (...) {
                    sendLine(clientFd, "3.2 ERROR REPLACE bad message id");
                }
            }
        }
        else if (line.rfind("QUIT", 0) == 0) {
            sendLine(clientFd, "9.0 BYE goodbye");
            shutdown(clientFd, SHUT_RDWR);
            close(clientFd);
            return;
        }
        else {
            sendLine(clientFd, "5.0 ERROR unknown command");
        }
    }

    sendLine(clientFd, "9.0 BYE goodbye");
    shutdown(clientFd, SHUT_RDWR);
    close(clientFd);
}

/* ─────────────────────────────────────────────
   Thread pool
   ───────────────────────────────────────────── */
void workerLoop() {
    while (true) {
        int fd;
        {
            std::unique_lock<std::mutex> lk(g_queueMtx);
            g_queueCv.wait(lk, []{ return !g_queue.empty() || g_doQuit; });
            if (g_doQuit && g_queue.empty()) return;
            fd = g_queue.front();
            g_queue.pop();
        }
        handleClient(fd);
    }
}

void spawnThreads(int n) {
    for (int i = 0; i < n; i++) {
        g_workers.emplace_back(workerLoop);
    }
}

void enqueueClient(int fd) {
    std::lock_guard<std::mutex> lk(g_queueMtx);
    g_queue.push(fd);
    g_queueCv.notify_one();
}

/* ─────────────────────────────────────────────
   Listener socket
   ───────────────────────────────────────────── */
int makeListener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons((uint16_t)port);

    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }

    if (listen(fd, 64) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }

    return fd;
}

/* ─────────────────────────────────────────────
   Run server
   ───────────────────────────────────────────── */
void runServer() {
    g_running = true;

    int clientFd = makeListener(g_cfg.bbport);
    if (clientFd < 0) {
        logMsg("ERROR: cannot bind client port");
        return;
    }

    int replicaFd = makeListener(g_cfg.rport);
    if (replicaFd < 0) {
        logMsg("ERROR: cannot bind replica port");
        close(clientFd);
        return;
    }

    spawnThreads(std::min(g_cfg.thincr, g_cfg.thmax));

    // replica accept loop in background thread
    std::thread replicaThread([replicaFd]() {
        while (g_running) {
            fd_set fds;
            FD_ZERO(&fds);
            FD_SET(replicaFd, &fds);
            struct timeval tv{1, 0};
            int r = select(replicaFd + 1, &fds, nullptr, nullptr, &tv);
            if (r <= 0) continue;
            int fd = accept(replicaFd, nullptr, nullptr);
            if (fd < 0) {
                if (errno == EINTR) continue;
                break;
            }
            std::thread(handleReplica, fd).detach();
        }
    });

    logMsg("Server listening on client port " + std::to_string(g_cfg.bbport)
           + " replica port " + std::to_string(g_cfg.rport));

    // client accept loop
    while (!g_doQuit && !g_doRestart) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(clientFd, &fds);
        struct timeval tv{1, 0};
        int r = select(clientFd + 1, &fds, nullptr, nullptr, &tv);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) continue;

        int newFd = accept(clientFd, nullptr, nullptr);
        if (newFd < 0) {
            if (errno == EINTR) continue;
            break;
        }

        int current = (int)g_workers.size();
        if (current < g_cfg.thmax) {
            int toSpawn = std::min(g_cfg.thincr, g_cfg.thmax - current);
            if (toSpawn > 0) spawnThreads(toSpawn);
        }

        enqueueClient(newFd);
    }

    // clean up
    g_running = false;
    g_doQuit  = true;
    g_queueCv.notify_all();

    for (auto& t : g_workers) {
        if (t.joinable()) t.join();
    }
    g_workers.clear();

    replicaThread.join();

    close(clientFd);
    close(replicaFd);
}

/* ─────────────────────────────────────────────
   Main
   ───────────────────────────────────────────── */
// Resolve a (possibly relative) BBFILE path against the original cwd, so
// it stays valid after we chdir into SAFE_DIR and across SIGHUP reloads.
static void resolveBbfilePath(Config& cfg) {
    if (!cfg.bbfile.empty() && cfg.bbfile[0] != '/') {
        cfg.bbfile = g_origCwd + "/" + cfg.bbfile;
    }
}

int main(int argc, char* argv[]) {
    const char* path = DEFAULT_CONF;

    if (argc > 2) {
        std::cout << "Usage: rbbserv [config-file]\n";
        return 1;
    }

    if (argc == 2) {
        path = argv[1];
        int fd = open(path, O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error: cannot open config file: " << path << "\n";
            return 1;
        }
        close(fd);
    }

    // Record the launch cwd before any chdir so we can resolve BBFILE
    // (and any other relative paths) consistently across SIGHUP reloads.
    {
        char cwdbuf[4096];
        if (getcwd(cwdbuf, sizeof(cwdbuf)) != nullptr) {
            g_origCwd = cwdbuf;
        } else {
            g_origCwd = ".";
        }
    }

    if (!initialize(path, g_cfg)) {
        std::cerr << "Initialize failed, exiting\n";
        return 1;
    }

    mkdir(SAFE_DIR, 0755);

    if (!g_cfg.foreground) {
        daemonize();
        g_logFp = fopen(LOG_FILE, "a");
    }

    g_pidFd = open(PID_FILE, O_RDWR | O_CREAT, 0644);
    if (g_pidFd < 0) {
        perror("open pid file");
        return 1;
    }

    if (flock(g_pidFd, LOCK_EX | LOCK_NB) < 0) {
        std::cerr << "Server already running\n";
        close(g_pidFd);
        return 1;
    }

    if (ftruncate(g_pidFd, 0) < 0) {
        perror("ftruncate pid file");
    }
    lseek(g_pidFd, 0, SEEK_SET);
    char buf[32];
    int n = snprintf(buf, 32, "%d\n", (int)getpid());
    if (write(g_pidFd, buf, (size_t)n) != n) {
        perror("write pid file");
    }
    fsync(g_pidFd);

    if (chdir(SAFE_DIR) != 0) {
        perror("chdir");
        return 1;
    }

    resolveBbfilePath(g_cfg);

    int bbfd = open(g_cfg.bbfile.c_str(), O_RDWR | O_CREAT, 0644);
    if (bbfd >= 0) close(bbfd);

    initNextId();

    setupSignals(g_cfg.foreground);

    while (true) {
        g_doQuit    = false;
        g_doRestart = false;

        runServer();

        if (g_doQuit) {
            logMsg("Server shutting down");
            break;
        }
        if (g_doRestart) {
            logMsg("Server restarting");
            if (initialize(path, g_cfg)) {
                // re-resolve BBFILE against the original cwd since
                // initialize() resets the config struct and we're still
                // chdir'd inside SAFE_DIR.
                resolveBbfilePath(g_cfg);
            } else {
                logMsg("Reload failed, continuing with previous config");
            }
        }
    }

    removePidFile();
    if (g_logFp) fclose(g_logFp);
    return 0;
}
