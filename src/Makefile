CHARMC=/u/mgartenhaus/charm/netlrts-linux-x86_64/bin/charmc
BASE_DIR=/u/mgartenhaus/LibCharmtyles
LIBS_DIR=$(BASE_DIR)
EIGEN_DIR = /u/mgartenhaus/eigen
OPTS=-c++-option -std=c++17 -O3 -I$(EIGEN_DIR) -DNDEBUG -g

all: server

.PHONY: clean server.out

server_ci: server.ci
	$(CHARMC) -E server.ci

server: server.cpp server_ci
	$(CHARMC) $< -L$(LIBS_DIR)/charmtyles -lcharmtyles -I$(BASE_DIR) -I$(BASE_DIR)/charmtyles/backend -o $@.out $(OPTS)

run-server: server.out
	./charmrun +p1 ./server.out ++server ++server-port 10000

clean:
	rm *.decl.h *.def.h *.out charmrun
