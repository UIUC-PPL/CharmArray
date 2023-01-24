#include <charmtyles/charmtyles.hpp>
#include <stack>
#include <cinttypes>
#include <variant>
#include <queue>
#include <unordered_map>
#include "ast.hpp"

#include "server.decl.h"


using ct_name_t = uint64_t;
using ct_array_t = std::variant<ct::scalar, ct::vector, ct::matrix, double>;
std::unordered_map<ct_name_t, ct_array_t> symbol_table;
std::stack<uint8_t> client_ids;


CProxy_Main main_proxy;
CProxy_Driver driver_proxy;


ct_array_t calculate(astnode* node, std::vector<uint64_t> &metadata);


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
    Server server;
    std::unordered_map<int, CcsDelayedReply> reply_buffer;

    Main(CkArgMsg* msg);

    void register_handlers();

    void send_reply(int epoch, int size, char* msg);

    CcsDelayedReply& get_reply(int epoch);
};

class Driver : public CBase_Driver
{
private:
    int EPOCH;

public:
    Driver_SDAG_CODE

    Driver();

    void execute_command(int epoch, uint8_t kind, int size, char* cmd);

    void execute_operation(int epoch, int size, char* cmd);

    void execute_creation(int epoch, int size, char* cmd);

    void execute_delete(int epoch, int size, char* cmd);

    void execute_fetch(int epoch, int size, char* cmd);

    void execute_disconnect(int epoch, int size, char* cmd);

    void execute_sync(int epoch, int size, char* cmd);
};

class Server
{
public:
    static void initialize()
    {
        for (int16_t i = 255; i >= 0; i--)
            client_ids.push((uint8_t) i);
    }

    inline static void insert(ct_name_t name, ct_array_t arr)
    {
#ifndef NDEBUG
        CkPrintf("Created array %" PRIu64 " on server\n", name);
#endif
        symbol_table[name] = arr;
    }

    inline static void remove(ct_name_t name)
    {
        symbol_table.erase(name);
#ifndef NDEBUG
        CkPrintf("Deleted array %" PRIu64 " on server\n", name);
#endif
    }

    inline static uint8_t get_client_id()
    {
        if (client_ids.empty())
            CmiAbort("Too many clients connected to the server");
        uint8_t client_id = client_ids.top();
        client_ids.pop();
        return client_id;
    }

    static ct_array_t& lookup(ct_name_t name)
    {
        auto find = symbol_table.find(name);
        if (find == std::end(symbol_table))
        {
#ifndef NDEBUG
            CkPrintf("Active symbols: ");
            for (auto it: symbol_table)
                CkPrintf("%" PRIu64 ", ", it.first);
            CkPrintf("\n");
#endif
            CmiAbort("Symbol %i not found", name);
        }
        return find->second;
    }

    static void operation_handler(char* msg)
    {
        CkPrintf("Received operation\n");
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        uint32_t size = extract<uint32_t>(cmd);
        driver_proxy.receive_command(epoch, (uint8_t) opkind::operation, size, cmd);
    }

    static void creation_handler(char* msg)
    {
        CkPrintf("Received creation\n");
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        uint32_t size = extract<uint32_t>(cmd);
        driver_proxy.receive_command(epoch, (uint8_t) opkind::creation, size, cmd);
    }

    static void delete_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        uint32_t size = extract<uint32_t>(cmd);
        driver_proxy.receive_command(epoch, (uint8_t) opkind::deletion, size, cmd);
    }

    static void fetch_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        uint32_t size = extract<uint32_t>(cmd);
        Main* main_obj = main_proxy.ckLocal();
        if (main_obj == NULL)
            CkAbort("Local branch of main not found!\n");
        main_obj->reply_buffer[epoch] = CcsDelayReply();
        driver_proxy.receive_command(epoch, (uint8_t) opkind::fetch, size, cmd);
    }

    static void connection_handler(char* msg)
    {
        uint8_t client_id = get_client_id();
        CcsSendReply(1, (void*) &client_id);
    }

    static void disconnection_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        uint32_t size = extract<uint32_t>(cmd);
        driver_proxy.receive_command(epoch, (uint8_t) opkind::disconnect, size, cmd);
    }

    static void sync_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        int epoch = extract<int>(cmd);
        uint32_t size = extract<uint32_t>(cmd);
        main_proxy.ckLocal()->reply_buffer[epoch] = CcsDelayReply();
        driver_proxy.receive_command(epoch, (uint8_t) opkind::sync, size, cmd);
    }

    inline static void exit_server(char* msg)
    {
        CkExit();
    }
};

ct_array_t calculate(astnode* node, std::vector<uint64_t> &metadata)
{
    switch (node->oper)
    {
        case operation::noop: {
            if (node->is_scalar)
                return *reinterpret_cast<double*>(&(node->name));
            else
                return Server::lookup(node->name);
        }
        case operation::add: {
            ct_array_t s1 = calculate(node->operands[0], metadata);
            ct_array_t s2 = calculate(node->operands[1], metadata);
            ct_array_t res;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr(std::is_same_v<T, V> && !std::is_same_v<T, double> &&
                            !std::is_same_v<T, ct::scalar>)
                        res = x + y;
                    else if constexpr(std::is_same_v<T, V> && std::is_same_v<T, ct::scalar>)
                        res = x.get() + y.get();
                    else
                        CmiAbort("Operation not permitted2");
                }, s1, s2);

            if (node->store)
                Server::insert(node->name, res);
            return res;
        }
        case operation::sub: {
            ct_array_t s1 = calculate(node->operands[0], metadata);
            ct_array_t s2 = calculate(node->operands[1], metadata);
            ct_array_t res;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    //if constexpr(std::is_same_v<T, V> && !std::is_same_v<T, double> && !std::is_same_v<T, ct::scalar>)
                    //    res = x - y;
                    //else if constexpr(std::is_same_v<T, V> && std::is_same_v<T, ct::scalar>)
                    //    res = x.get() - y.get();
                    if constexpr(std::is_same_v<T, V> && std::is_same_v<T, ct::vector>)
                        res = x - y;
                    else
                        CmiAbort("Operation not permitted3");
                }, s1, s2);

            if (node->store)
            {
                Server::insert(node->name, res);
            }
            return res;
        }
        // case operation::mul: {
        //     ct_array_t s1 = calculate(node->operands[0], metadata);
        //     ct_array_t s2 = calculate(node->operands[1], metadata);
        //     ct_array_t res;

        //     std::visit(
        //         [&](auto& x, auto& y) {
        //             using T = std::decay_t<decltype(x)>;
        //             using V = std::decay_t<decltype(y)>;
        //             if constexpr(std::is_same_v<T, ct::scalar> && std::is_same_v<V, ct::scalar>)
        //                 res = x.get() * y.get();
        //             else if constexpr(std::is_same_v<T, double> && !std::is_same_v<V, ct::scalar>)
        //                 res = x * y;
        //             else if constexpr(std::is_same_v<V, ct::scalar> && !std::is_same_v<T, ct::scalar>)
        //                 res = y.get() * x;
        //             else if constexpr(std::is_same_v<V, double> && !std::is_same_v<T, ct::scalar>)
        //                 res = y * x;
        //             else
        //                 CmiAbort("Operation not permitted");
        //         }, s1, s2);

        //     if (node->store)
        //     {
        //         Server::insert(node->name, res);
        //     }
        //     return res;
        // }
        case operation::div: {
            ct_array_t s1 = calculate(node->operands[0], metadata);
            ct_array_t s2 = calculate(node->operands[1], metadata);
            ct_array_t res;
            double op1, op2;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr(std::is_same_v<T, ct::scalar>)
                        op1 = x.get();
                    else if constexpr(std::is_same_v<V, ct::scalar>)
                        op2 = y.get();
                    else
                        CmiAbort("Operation not permitted4");
                    res = op1 / op2;
                }, s1, s2);

            if (node->store)
            {
                Server::insert(node->name, res);
            }
            return res;
        }
        case operation::matmul: {
            ct_array_t s1 = calculate(node->operands[0], metadata);
            ct_array_t s2 = calculate(node->operands[1], metadata);
            ct_array_t res;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr (std::is_same_v<T, ct::matrix> && 
                            std::is_same_v<V, ct::matrix>)
                        CmiAbort("Matrix multiplication not yet implemented");
                    else if constexpr (std::is_same_v<T, ct::vector> && 
                            std::is_same_v<V, ct::vector>)
                        res = ct::dot(x, y);
                    else if constexpr ((std::is_same_v<T, ct::matrix> || 
                                    std::is_same_v<T, ct::vector>) && 
                                std::is_same_v<V, ct::vector>)
                        res = ct::dot(x, y);
                    else
                        CmiAbort("Operation not permitted5");
                }, s1, s2);

            if (node->store)
            {
                Server::insert(node->name, res);
            }
            return res;
        }
        case operation::copy: {
            ct_array_t s1 = calculate(node->operands[0], metadata);
            ct_array_t res;

            std::visit(
                [&](auto& x) {
                    using T = std::decay_t<decltype(x)>;
                    if constexpr(std::is_same_v<T, ct::vector> || 
                            std::is_same_v<T, ct::scalar>)
                        res = x;
                    else
                        CmiAbort("Matrix copy not implemented");
                }, s1);

            if (node->store)
            {
                Server::insert(node->name, res);
            }
            return res;
        }
        case operation::axpy: {
            ct_array_t s1 = calculate(node->operands[0], metadata);
            ct_array_t s2 = calculate(node->operands[1], metadata);
            ct_array_t s3 = calculate(node->operands[2], metadata);
            ct_array_t res;

            std::visit(
                [&](auto& a, auto& x, auto& y) {
                    using S = std::decay_t<decltype(a)>;
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr(std::is_same_v<S, double> &&
                                std::is_same_v<T, ct::vector> &&
                                std::is_same_v<V, ct::vector>)
                        res = ct::axpy(a, x, y);
                    else
                        CmiAbort("Operation not permitted6");
                }, s1, s2, s3);

            if (node->store)
            {
                Server::insert(node->name, res);
            }
            return res;
        }
        case operation::axpy_multiplier: {
            ct_array_t s1 = calculate(node->operands[0], metadata);
            ct_array_t s2 = calculate(node->operands[1], metadata);
            ct_array_t s3 = calculate(node->operands[2], metadata);
            ct_array_t multiplier = calculate(node->operands[3], metadata);
            ct_array_t res;

            std::visit(
                [&](auto& multiplier, auto& a, auto& x, auto& y) {
                    using S = std::decay_t<decltype(a)>;
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    using M = std::decay_t<decltype(multiplier)>;
                    if constexpr(std::is_same_v<M, double> &&
                                std::is_same_v<S, double> &&
                                std::is_same_v<T, ct::vector> &&
                                std::is_same_v<V, ct::vector>)
                        res = ct::axpy(multiplier * a, x, y);
                    else
                        CmiAbort("Operation not permitted7");
                }, multiplier, s1, s2, s3);

            if (node->store)
                Server::insert(node->name, res);
            return res;
        }
        default: {
            CmiAbort("Operation not implemented8");
        }
    }
};

#include "server.def.h"
