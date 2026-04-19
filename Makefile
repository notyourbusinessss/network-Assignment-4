CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -pthread

# Assignment 4 deliverables
RBBSERV  := rbbserv
CLIENT   := client

.PHONY: all clean run

all: $(RBBSERV) $(CLIENT)

$(RBBSERV): rbbserv.cpp
	$(CXX) $(CXXFLAGS) $< -o $@

$(CLIENT): test.cpp
	$(CXX) $(CXXFLAGS) $< -o $@

clean:
	rm -f $(RBBSERV) $(CLIENT)
	rm -f run/rbbserv.pid run/bbserv.log
