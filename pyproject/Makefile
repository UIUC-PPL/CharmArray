CHARMC=/home/adityapb/charm/charm/netlrts-linux-x86_64/bin/charmc
BASE_DIR=/home/adityapb/charm/LibAum
LIBS_DIR=$(BASE_DIR)
OPTS=-c++-option -std=c++17 -O3

all: server

.PHONY: clean server

server_ci: server.ci
	$(CHARMC) -E server.ci

server: server.cpp server_ci
	$(CHARMC) $< -L$(LIBS_DIR)/aum -laum -I$(BASE_DIR) -I$(BASE_DIR)/aum/backend -o $@.out $(OPTS)

clean:
	rm *.decl.h *.def.h *.out charmrun
