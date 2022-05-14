#include <aum/aum.hpp>
#include <unordered_map>
#include <variant>
#include <queue>
#include <stack>
#include <cinttypes>
#include "server.h"
#include "converse.h"
#include "conv-ccs.h"

#include "server.decl.h"

using aum_name_t = uint64_t;
using aum_array_t = std::variant<aum::scalar, aum::vector, aum::matrix>;

std::unordered_map<aum_name_t, aum_array_t> symbol_table;
std::priority_queue<char*, std::vector<char*>, epoch_compare> message_buffer;
std::stack<uint8_t> client_ids;
uint64_t next_epoch[256];

class Main : public CBase_Main
{
public:
    Main(CkArgMsg* msg) 
    {
        for (int16_t i = 255; i >= 0; i--)
            client_ids.push((uint8_t) i);
        memset(next_epoch, 0, 256 * sizeof(uint64_t));
        register_handlers();
#ifndef NDEBUG
        CkPrintf("Initialization done\n");
#endif
    }

    inline static void insert(aum_name_t name, aum_array_t arr)
    {
#ifndef NDEBUG
        CkPrintf("Created array %" PRIu64 " on server\n", name);
#endif
        symbol_table.emplace(name, arr);
    }

    inline static void remove(aum_name_t name)
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

    static aum_array_t& lookup(aum_name_t name)
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

    static void flush_buffer()
    {
        while (true)
        {
            char* msg = message_buffer.top();
            char* cmd = msg + CmiMsgHeaderSizeBytes;
            uint8_t client_id = extract<uint8_t>(cmd);
            uint64_t epoch = extract<uint64_t>(cmd);
            if (epoch != next_epoch[client_id])
                break;
            message_buffer.pop();
            operation_handler(msg);
            free(msg);
            next_epoch[client_id]++;
        }
    }

    static void insert_buffer(uint32_t msg_size, char* msg)
    {
#ifndef NDEBUG
        CkPrintf("Inserting into buffer\n");
#endif
        char* msg_copy = (char*) malloc(msg_size);
        memcpy(msg_copy, msg, msg_size);
        message_buffer.push(msg_copy);
        flush_buffer();
    }

    static void operation_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint8_t client_id = extract<uint8_t>(cmd);
        uint64_t epoch = extract<uint64_t>(cmd);
        uint32_t msg_size = CmiMsgHeaderSizeBytes + extract<uint32_t>(cmd);
        if (epoch > next_epoch[client_id])
            insert_buffer(msg_size, msg);
        next_epoch[client_id]++;
        aum_name_t res_name = extract<aum_name_t>(cmd);
        uint32_t opcode = extract<uint32_t>(cmd);
#ifndef NDEBUG
        CkPrintf("Operation %" PRIu32 "\n", opcode);
#endif
        switch(opcode)
        {
            case 0: {
                // no op
                CmiAbort("No op not implemented");
            }
            case 1: {
                // addition
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_name_t name2 = extract<aum_name_t>(cmd);
                aum_array_t& c1 = lookup(name1);
                aum_array_t& c2 = lookup(name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, V>)
                            res = x + y;
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2);
                break;
            }
            case 2: {
                // subtraction
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_name_t name2 = extract<aum_name_t>(cmd);
                aum_array_t& c1 = lookup(name1);
                aum_array_t& c2 = lookup(name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, V>)
                            res = x - y;
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2);
                break;
            }
            case 3: {
                // multiply
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_name_t name2 = extract<aum_name_t>(cmd);
                aum_array_t& c1 = lookup(name1);
                aum_array_t& c2 = lookup(name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, aum::scalar>)
                            res = x * y;
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2);
                break;
            }
            case 4: {
                // division
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_name_t name2 = extract<aum_name_t>(cmd);
                aum_array_t& c1 = lookup(name1);
                aum_array_t& c2 = lookup(name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, aum::scalar> && 
                                std::is_same_v<V, aum::scalar>)
                            res = x / y;
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2);
                break;
            }
            case 5: {
                // mat/vec mul
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_name_t name2 = extract<aum_name_t>(cmd);
                aum_array_t& c1 = lookup(name1);
                aum_array_t& c2 = lookup(name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, aum::matrix> && 
                                std::is_same_v<V, aum::matrix>)
                            // Matmul here
                            CmiAbort("Matrix multiplication not yet implemented");
                        else if constexpr((std::is_same_v<T, aum::matrix> || 
                                    std::is_same_v<T, aum::vector>) && 
                                std::is_same_v<V, aum::vector>)
                            res = aum::dot(x, y);
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2);
                break;
            }
            // FIXME implement a matrix copy
            case 6: {
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_array_t& c1 = lookup(name1);
                std::visit(
                    [&](auto& x) {
                        using T = std::decay_t<decltype(x)>;
                        if constexpr(std::is_same_v<T, aum::scalar> || 
                                std::is_same_v<T, aum::vector>)
                        {
                            aum_array_t res = aum::copy(x); 
                            insert(res_name, res);
                        }
                        else
                        {
                            CmiAbort("Matrix copy not implemented!");
                        }
                    }, c1);
                break;
            }
            case 7: {
                // axpy
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_name_t name2 = extract<aum_name_t>(cmd);
                aum_name_t name3 = extract<aum_name_t>(cmd);
                aum_array_t& c1 = lookup(name1);
                aum_array_t& c2 = lookup(name2);
                aum_array_t& c3 = lookup(name3);
                std::visit(
                    [&](auto& a, auto& x, auto& y) {
                        using S = std::decay_t<decltype(a)>;
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<S, aum::scalar> && 
                                std::is_same_v<T, aum::vector> &&
                                std::is_same_v<V, aum::vector>)
                            res = aum::blas::axpy(a, x, y);
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2, c3);
                break;
            }
            case 8: {
                // axpy multiplier
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_name_t name2 = extract<aum_name_t>(cmd);
                aum_name_t name3 = extract<aum_name_t>(cmd);
                double multiplier = extract<double>(cmd);
                aum_array_t& c1 = lookup(name1);
                aum_array_t& c2 = lookup(name2);
                aum_array_t& c3 = lookup(name3);
                std::visit(
                    [&](auto& a, auto& x, auto& y) {
                        using S = std::decay_t<decltype(a)>;
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<S, aum::scalar> && 
                                std::is_same_v<T, aum::vector> &&
                                std::is_same_v<V, aum::vector>)
                            res = aum::blas::axpy(multiplier, a, x, y);
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2, c3);
                break;
            }
            case 9: {
                // multiply
                aum_name_t name1 = extract<aum_name_t>(cmd);
                double y = extract<double>(cmd);
                aum_array_t& c1 = lookup(name1);
                std::visit(
                    [&](auto& x) {
                        using T = std::decay_t<decltype(x)>;
                        aum_array_t res;
                        res = y * x;
                        insert(res_name, res);
                    }, c1);
                break;
            }
            case 10: {
                // division
                aum_name_t name1 = extract<aum_name_t>(cmd);
                aum_name_t name2 = extract<aum_name_t>(cmd);
                aum_array_t& c1 = lookup(name1);
                aum_array_t& c2 = lookup(name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, aum::scalar> && 
                                std::is_same_v<V, aum::scalar>)
                            res = x / y;
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2);
                break;
            }
            default: {
                CmiAbort("Operation not implemented");
            }
        }
    }
    
    static void creation_handler(char* msg)
    {
        /* First 32 bits are number of dimensions
         * Each of the next 64 bits is the size of the array
         * in each dimension
         */
        // FIXME need a field for dtype
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint8_t client_id = extract<uint8_t>(cmd);
        uint64_t epoch = extract<uint64_t>(cmd);
        uint32_t msg_size = CmiMsgHeaderSizeBytes + extract<uint32_t>(cmd);
        if (epoch > next_epoch[client_id])
            insert_buffer(msg_size, msg);
        next_epoch[client_id]++;
        aum_name_t res_name = extract<aum_name_t>(cmd);
        uint32_t ndim = extract<uint32_t>(cmd);
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
                aum_array_t res;
                if (has_init)
                {
                    double init_value = extract<double>(cmd);
                    res = aum::vector(size, init_value);
                }
                else
                {
                    res = aum::vector(size, aum::random{});
                }
                insert(res_name, res);
                break;
            }
            case 2: {
                // create matrix
                uint64_t size1 = extract<uint64_t>(cmd);
                uint64_t size2 = extract<uint64_t>(cmd);
                aum_array_t res;
                if (has_init)
                {
                    double init_value = extract<double>(cmd);
                    res = aum::matrix(size1, size2, init_value);
                }
                else
                {
                    res = aum::matrix(size1, size2, aum::random{});
                }
                insert(res_name, res);
                break;
            }
            default: {
                CmiAbort("Greater than 2 dimensions not supported");
            }
        }
    }

    static void fetch_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint8_t client_id = extract<uint8_t>(cmd);
        uint64_t epoch = extract<uint64_t>(cmd);
        uint32_t msg_size = CmiMsgHeaderSizeBytes + extract<uint32_t>(cmd);
        if (epoch > next_epoch[client_id])
            insert_buffer(msg_size, msg);
        next_epoch[client_id]++;
        aum_name_t name = extract<aum_name_t>(cmd);
        aum_array_t& arr = lookup(name);
        void* reply = nullptr;
        int reply_size = 0;
        std::visit(
            [&](auto& x) {
                using T = std::decay_t<decltype(x)>;
                if constexpr(std::is_same_v<T, aum::scalar>)
                {
                    double value = x.get();
                    reply = (void*) &value;
                    reply_size += 8;
                    CcsSendReply(reply_size, reply);
                }    
                else
                    CmiAbort("Operation not implemented");
            }, arr);
    }

    static void delete_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint8_t client_id = extract<uint8_t>(cmd);
        uint64_t epoch = extract<uint64_t>(cmd);
        uint32_t msg_size = CmiMsgHeaderSizeBytes + extract<uint32_t>(cmd);
        if (epoch > next_epoch[client_id])
            insert_buffer(msg_size, msg);
        next_epoch[client_id]++;
        aum_name_t name = extract<aum_name_t>(cmd);
        remove(name);
    }

    static void connection_handler(char* msg)
    {
        uint8_t client_id = get_client_id();
        CcsSendReply(1, (void*) &client_id);
    }

    static void disconnection_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint8_t client_id = extract<uint8_t>(cmd);
        client_ids.push(client_id);
#ifndef NDEBUG
        CkPrintf("Disconnected %" PRIu8 " from server\n", client_id);
#endif
    }

    inline static void exit_server(char* msg)
    {
        CkExit();
    }

    void register_handlers()
    {
        CcsRegisterHandler("aum_connect", (CmiHandler) connection_handler);
        CcsRegisterHandler("aum_disconnect", (CmiHandler) disconnection_handler);
        CcsRegisterHandler("aum_operation", (CmiHandler) operation_handler);
        CcsRegisterHandler("aum_creation", (CmiHandler) creation_handler);
        CcsRegisterHandler("aum_fetch", (CmiHandler) fetch_handler);
        CcsRegisterHandler("aum_delete", (CmiHandler) delete_handler);
        CcsRegisterHandler("aum_exit", (CmiHandler) exit_server);
    }
};

#include "server.def.h"
