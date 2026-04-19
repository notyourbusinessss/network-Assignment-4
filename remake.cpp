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
std::mutex        g_logMtx;
int               g_pidFd      = -1;
FILE*             g_logFp      = nullptr;

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
                cfg.thmax = std::stoi(val);
            }
            else if (key == "THINCR" && !val.empty()) {
                cfg.thincr = std::stoi(val);
            }
            else if (key == "BBPORT" && !val.empty()) {
                cfg.bbport = std::stoi(val);
            }
            else if (key == "FOREGROUND" && !val.empty()) {
                cfg.foreground = strToBool(val);
            }
            else if (key == "PDEBUG" && !val.empty()) {
                cfg.pdebug = strToBool(val);
            }
            else if (key == "RPORT" && !val.empty()) {
                cfg.rport = std::stoi(val);
            }
            else if (key == "BBFILE" && !val.empty()) {
                cfg.bbfile = val;
            }
            else if (key == "PEER" && !val.empty()) {
                auto col = val.rfind(':');
                if (col != std::string::npos) {
                    Peer p;
                    p.host = val.substr(0, col);
                    p.port = std::stoi(val.substr(col + 1));
                    cfg.peers.push_back(p);
                }
            }
        }

        start = end + 1;
    }

    if (cfg.bbfile.empty()) {
        std::cerr << "Configuration error: BBFILE is required\n";
        return false;
    }

    std::cout << "THMAX      = " << cfg.thmax      << "\n";
    std::cout << "THINCR     = " << cfg.thincr     << "\n";
    std::cout << "BBPORT     = " << cfg.bbport     << "\n";
    std::cout << "FOREGROUND = " << (cfg.foreground ? "true" : "false") << "\n";
    std::cout << "PDEBUG     = " << (cfg.pdebug     ? "true" : "false") << "\n";
    std::cout << "RPORT      = " << cfg.rport      << "\n";
    std::cout << "BBFILE     = " << cfg.bbfile     << "\n";
    std::cout << "PEERS      = " << cfg.peers.size() << "\n";
    for (auto& p : cfg.peers) {
        std::cout << "  PEER     = " << p.host << ":" << p.port << "\n";
    }
    std::cout.flush();

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
   Run server
   ───────────────────────────────────────────── */
void runServer() {

}

/* ─────────────────────────────────────────────
   Main
   ───────────────────────────────────────────── */
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

    ftruncate(g_pidFd, 0);
    lseek(g_pidFd, 0, SEEK_SET);
    char buf[32];
    int n = snprintf(buf, 32, "%d\n", (int)getpid());
    write(g_pidFd, buf, (size_t)n);
    fsync(g_pidFd);

    if (chdir(SAFE_DIR) != 0) {
        perror("chdir");
        return 1;
    }

    if (g_cfg.bbfile[0] != '/') {
        g_cfg.bbfile = "../" + g_cfg.bbfile;
    }

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
            initialize(path, g_cfg);
        }
    }

    removePidFile();
    if (g_logFp) fclose(g_logFp);
    return 0;
}