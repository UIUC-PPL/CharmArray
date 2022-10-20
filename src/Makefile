CHARMC=/home/adityapb/charm/charm/netlrts-linux-x86_64/bin/charmc
BASE_DIR=/home/adityapb/charm/LibCharmtyles
LIBS_DIR=$(BASE_DIR)
EIGEN_DIR = /usr/include/eigen3
OPTS=-c++-option -std=c++17 -O3 -DNDEBUG -I$(EIGEN_DIR)

all: server

.PHONY: clean server.out

server_ci: server.ci
	$(CHARMC) -E server.ci

server: server.cpp server_ci
	$(CHARMC) $< -L$(LIBS_DIR)/charmtyles -lcharmtyles -I$(BASE_DIR) -I$(BASE_DIR)/charmtyles/backend -o $@.out $(OPTS)

run-server: server.out
	./charmrun +p4 ./server.out ++server ++server-port 10000 ++local

clean:
	rm *.decl.h *.def.h *.out charmrun
