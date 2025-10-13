#include "ast.hpp"

#include "server.decl.h"

using buffer_t = std::tuple<int, uint8_t, char *>;
std::stack<uint8_t> client_ids;

CProxy_Main main_proxy;

enum class opkind : uint8_t
{
  creation = 0,
  operation = 1,
  fetch = 2,
  deletion = 3,
  disconnect = 4,
  sync = 5
};

class Main : public CBase_Main
{
public:
  std::priority_queue<buffer_t, std::vector<buffer_t>, std::greater<buffer_t>> command_buffer;
  int EPOCH;

  Main(CkArgMsg *msg);

  void register_handlers();

  void send_reply(int epoch, int size, char *msg);

  void handle_command(int epoch, uint8_t kind, uint32_t size, char *cmd);

  void execute_command(int epoch, uint8_t kind, int size, char *cmd);

  void execute_operation(int epoch, int size, char *cmd);

  void execute_creation(int epoch, int size, char *cmd);

  void execute_delete(int epoch, int size, char *cmd);

  void execute_fetch(int epoch, int size, char *cmd);

  void execute_disconnect(int epoch, int size, char *cmd);

  void execute_sync(int epoch, int size, char *cmd);
};

class Server
{
public:
  std::unordered_map<int, CcsDelayedReply> reply_buffer;

  static void initialize()
  {
    for (int16_t i = 255; i >= 0; i--)
      client_ids.push((uint8_t)i);
  }

  inline static uint8_t get_client_id()
  {
    if (client_ids.empty())
      CmiAbort("Too many clients connected to the server");
    uint8_t client_id = client_ids.top();
    client_ids.pop();
    return client_id;
  }
};

#include "server.def.h"
