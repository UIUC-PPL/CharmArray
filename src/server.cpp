#include <charmtyles/charmtyles.hpp>
#include "server.hpp"
#include "converse.h"
#include "conv-ccs.h"

Server server;
double start_time;

void operation_handler(char* msg)
{
    char* cmd = msg + CmiMsgHeaderSizeBytes;
    int epoch = extract<int>(cmd);
    uint32_t size = extract<uint32_t>(cmd);
    driver_proxy.receive_command(epoch, (uint8_t) opkind::operation, size, cmd);
}

void creation_handler(char* msg)
{
    char* cmd = msg + CmiMsgHeaderSizeBytes;
    int epoch = extract<int>(cmd);
    if (epoch == 0)
        start_time = CkTimer();
    uint32_t size = extract<uint32_t>(cmd);
    driver_proxy.receive_command(epoch, (uint8_t) opkind::creation, size, cmd);
}

void delete_handler(char* msg)
{
    char* cmd = msg + CmiMsgHeaderSizeBytes;
    int epoch = extract<int>(cmd);
    uint32_t size = extract<uint32_t>(cmd);
    driver_proxy.receive_command(epoch, (uint8_t) opkind::deletion, size, cmd);
}

void fetch_handler(char* msg)
{
    char* cmd = msg + CmiMsgHeaderSizeBytes;
    int epoch = extract<int>(cmd);
    uint32_t size = extract<uint32_t>(cmd);
    CkPrintf("Fetch at epoch %i\n", epoch);
    server.reply_buffer[epoch] = CcsDelayReply();
    driver_proxy.receive_command(epoch, (uint8_t) opkind::fetch, size, cmd);
}

void connection_handler(char* msg)
{
    uint8_t client_id = Server::get_client_id();
    CcsSendReply(1, (void*) &client_id);
}

void disconnection_handler(char* msg)
{
    char* cmd = msg + CmiMsgHeaderSizeBytes;
    int epoch = extract<int>(cmd);
    uint32_t size = extract<uint32_t>(cmd);
    driver_proxy.receive_command(epoch, (uint8_t) opkind::disconnect, size, cmd);
}

void sync_handler(char* msg)
{
    char* cmd = msg + CmiMsgHeaderSizeBytes;
    int epoch = extract<int>(cmd);
    uint32_t size = extract<uint32_t>(cmd);
    server.reply_buffer[epoch] = CcsDelayReply();
    driver_proxy.receive_command(epoch, (uint8_t) opkind::sync, size, cmd);
}

inline void exit_server(char* msg)
{
    CkExit();
}

Main::Main(CkArgMsg* msg) 
{
    main_proxy = thisProxy;
    server = Server();
    Server::initialize();
    register_handlers();
    driver_proxy = CProxy_Driver::ckNew();
#ifndef NDEBUG
    CkPrintf("Initialization done\n");
#endif
}

void Main::register_handlers()
{
    CcsRegisterHandler("aum_connect", (CmiHandler) connection_handler);
    CcsRegisterHandler("aum_disconnect", (CmiHandler) disconnection_handler);
    CcsRegisterHandler("aum_operation", (CmiHandler) operation_handler);
    CcsRegisterHandler("aum_creation", (CmiHandler) creation_handler);
    CcsRegisterHandler("aum_fetch", (CmiHandler) fetch_handler);
    CcsRegisterHandler("aum_delete", (CmiHandler) delete_handler);
    CcsRegisterHandler("aum_sync", (CmiHandler) sync_handler);
    CcsRegisterHandler("aum_exit", (CmiHandler) exit_server);
}

void Main::send_reply(int epoch, int size, char* msg)
{
    auto reply = server.reply_buffer.find(epoch);
    if (reply == server.reply_buffer.end())
        CkAbort("Reply without token\n");
    CcsSendDelayedReply(reply->second, size, msg);
    server.reply_buffer.erase(epoch);
}

Driver::Driver()
    : EPOCH(0)
{
    ct::init();
    thisProxy.start();
}

void Driver::execute_operation(int epoch, int size, char* cmd)
{
    // first delete arrays
    uint32_t num_deletions = extract<uint32_t>(cmd);
    for (int i = 0; i < num_deletions; i++)
    {
        ct_name_t name = extract<ct_name_t>(cmd);
        Server::remove(name);
    }
    astnode* head = decode(cmd);
    std::vector<uint64_t> metadata;
    calculate(head, metadata);
    delete_ast(head);
}

void Driver::execute_command(int epoch, uint8_t kind, int size, char* cmd)
{
    opkind kind_type = (opkind) kind;
    switch(kind_type)
    {
        case opkind::creation: {
            execute_creation(epoch, size, cmd);
            break;
        }
        case opkind::operation: {
            execute_operation(epoch, size, cmd);
            break;
        }
        case opkind::deletion: {
            execute_delete(epoch, size, cmd);
            break;
        }
        case opkind::fetch: {
            execute_fetch(epoch, size, cmd);
            break;
        }
        case opkind::disconnect: {
            execute_disconnect(epoch, size, cmd);
            break;
        }
        case opkind::sync: {
            execute_sync(epoch, size, cmd);
            break;
        }
    }
}

void Driver::execute_creation(int epoch, int size, char* cmd)
{
    /* First 32 bits are number of dimensions
     * Each of the next 64 bits is the size of the array
     * in each dimension
     */
    // FIXME need a field for dtype
    ct_name_t res_name = extract<ct_name_t>(cmd);
    uint32_t ndim = extract<uint32_t>(cmd);
    bool has_buf = extract<bool>(cmd);
    bool has_init = extract<bool>(cmd);
    switch(ndim)
    {
        case 0: {
            // create scalar
            CmiAbort("Not implemented");
        }
        case 1: {
            // create vector
            uint64_t size = extract<uint64_t>(cmd);
            ct_array_t res;
            if (has_buf)
            {
                //double* init_buf = (double*) cmd;
                //res = ct::vector(size, init_buf);
            }
            else if (has_init)
            {
                double init_value = extract<double>(cmd);
                res = ct::vector(size, init_value);
            }
            else
            {
                res = ct::vector(size);
            }
            Server::insert(res_name, res);
            break;
        }
        case 2: {
            // create matrix
            uint64_t size1 = extract<uint64_t>(cmd);
            uint64_t size2 = extract<uint64_t>(cmd);
            ct_array_t res;
            if (has_buf)
            {
                //double* init_buf = (double*) cmd;
                //res = ct::matrix(size1, size2, init_buf);
            }
            else if (has_init)
            {
                double init_value = extract<double>(cmd);
                res = ct::matrix(size1, size2, init_value);
            }
            else
            {
                res = ct::matrix(size1, size2);
            }
            Server::insert(res_name, res);
            break;
        }
        default: {
            // FIXME is this correctly caught?
            CmiAbort("Greater than 2 dimensions not supported");
        }
    }
}

void Driver::execute_fetch(int epoch, int size, char* cmd)
{
    ct_name_t name = extract<ct_name_t>(cmd);
    ct_array_t& arr = Server::lookup(name);
    char* reply = nullptr;
    int reply_size = 0;
    std::visit(
        [&](auto& x) {
            using T = std::decay_t<decltype(x)>;
            if constexpr(std::is_same_v<T, ct::scalar>)
            {
                double value = x.get();
                reply = (char*) &value;
                reply_size += 8;
                main_proxy.send_reply(epoch, reply_size, reply);
                //CcsSendReply(reply_size, reply);
            }    
            else
                CmiAbort("Operation not implemented1");
        }, arr);
}

void Driver::execute_delete(int epoch, int size, char* cmd)
{
    uint32_t num_deletions = extract<uint32_t>(cmd);
    for (int i = 0; i < num_deletions; i++)
    {
        ct_name_t name = extract<ct_name_t>(cmd);
        Server::remove(name);
    }
}

void Driver::execute_disconnect(int epoch, int size, char* cmd)
{
    uint8_t client_id = extract<uint8_t>(cmd);
    client_ids.push(client_id);
#ifndef NDEBUG
    CkPrintf("Disconnected %" PRIu8 " from server\n", client_id);
#endif
}

void Driver::execute_sync(int epoch, int size, char* cmd)
{
    CkPrintf("Execution time = %f\n", CkTimer() - start_time);
    bool r = true;
    main_proxy.send_reply(epoch, 1, (char*) &r);
}
