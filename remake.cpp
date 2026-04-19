#include <iostream>
#include <fstream>
#include <sched.h>
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
Config      g_cfg;
std::string g_configPath  = DEFAULT_CONF;
std::mutex  g_logMtx;
int         g_pidFd       = -1;
FILE*       g_logFp       = nullptr;   // null → use stdout

// THE ATOMICSSS !!!!
std::atomic<int> g_nextId{1};
std::atomic<bool> g_doRestart{false};
std::atomic<bool> g_doQuit{false};




/*
 * Initialization yayyy
 *  + helpers of course 
 */

bool strToBool(const std::string& s) {
    return s == "true" || s == "1";
}

/*
    removes \r and \n from the end of a string
*/
std::string trimCR(const std::string& s) {
    std::string out = s;

    while (!out.empty() && (out.back() == '\r' || out.back() == '\n')) {
        out.pop_back();
    }

    return out;
}

/* The actual stuff */
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
            else if (key == "PDEBUG" && !val.empty()) {
                cfg.pdebug = strToBool(val);
            }
            else if (key == "BBFILE" && !val.empty()) {
                cfg.bbfile = val;
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

// MAIN HELPERS 

void daemonize(){
    pid_t pid = fork();
    if(pid > 0 ){
        exit(0);
    }
    setsid();
    pid = fork();
    if(pid > 0){
        exit(0);
    }

    int maxFD = sysconf(_SC_OPEN_MAX);

    for (int i = 3; i < maxFD; i++) {
        close(i);
    }

    int devNull = open("/dev/null", O_RDWR);
    dup2(devNull, STDIN_FILENO);
    dup2(devNull, STDOUT_FILENO);
    dup2(devNull, STDERR_FILENO);
}

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

void runserver(){
    
}

void sigHandler(int sig) {
    if (sig == SIGHUP)  g_doRestart = true;
    if (sig == SIGQUIT) g_doQuit = true;
    if (sig == SIGINT)  g_doQuit = true;
}

void setupSignals(bool foreground) {
    struct sigaction sa{};
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sa.sa_handler = sigHandler;

    sigaction(SIGHUP, &sa, nullptr);
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
int main(int argc, char* argv[]){ 
    const char* path = DEFAULT_CONF;
    if(argc > 2){
        std::cout << "you must run with wither none or one argument, which should be the path to the configuration file\n exiting...\n";
        return 1;
    }

    
    if(argc == 2 ){
        path = argv[1];
        int fd = open(path, O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error: cannot open config file or does not exist : " << path << "\n";
            return 1;
        }
        close(fd);
    }


    //initiall steps
    if(!initialize(path, g_cfg)){
        std::cout << "Initialize did not work ending program! ";
    }

    // make the directory 
    mkdir("run", 0755);

    // 
    if(!g_cfg.foreground){
        daemonize();
        g_logFp = fopen(LOG_FILE,"a");
    }
    g_pidFd = open(PID_FILE, O_RDWR | O_CREAT, 0644);
    if(g_pidFd < 0 ){
        perror("Fail pid file");
        return 1;
    }
    if(flock(g_pidFd,LOCK_EX | LOCK_NB) < 0){
        perror("server already runnning");
        return 1;
    }
    ftruncate(g_pidFd, 0);
    lseek(g_pidFd, 0, SEEK_SET);
    int size = 32;
    char buf[size];
    int n = snprintf(buf, size,"%d\n", (int)getpid());
    write(g_pidFd, buf, size);
    fsync(g_pidFd);

    if(chdir("run") != 0){
        perror("chdir not working");
        return 1;
    }

    if (g_cfg.bbfile[0] != '/') {
        g_cfg.bbfile = "../" + g_cfg.bbfile;
    }

    int bbfd = open(g_cfg.bbfile.c_str(),O_RDWR | O_CREAT, 0644 );
    if(bbfd >= 0){
        close(bbfd);
    }

    initNextId();

    setupSignals(g_cfg.foreground);

    while(true){
        g_doQuit = false;
        g_doRestart = false;

        runserver();

        if(g_doQuit){
            logMsg("server shuttinmg down");
            break;
        }
        if (g_doRestart) {
            logMsg("server restart initiated");
            initialize(path, g_cfg);
        }
    }

    flock(g_pidFd, LOCK_UN);
    fclose(g_logFp);
    return 0;
}



