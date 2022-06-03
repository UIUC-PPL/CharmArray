#include <aum/aum.hpp>
#include <stack>
#include <cinttypes>
#include <variant>
#include <queue>
#include <unordered_map>
#include "ast.hpp"


using aum_name_t = uint64_t;
using aum_array_t = std::variant<aum::scalar, aum::vector, aum::matrix, double>;
std::unordered_map<aum_name_t, aum_array_t> symbol_table;
std::stack<uint8_t> client_ids;


aum_array_t calculate(astnode* node, std::vector<uint64_t> &metadata);


class Server
{
public:
    static void initialize()
    {
        for (int16_t i = 255; i >= 0; i--)
            client_ids.push((uint8_t) i);
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

    static void operation_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        astnode* head = decode(cmd);
        std::vector<uint64_t> metadata;
        calculate(head, metadata);
        delete_ast(head);
        CcsSendReply(sizeof(uint64_t) * metadata.size(), (void*) &metadata[0]);
    }

    static void creation_handler(char* msg)
    {
        /* First 32 bits are number of dimensions
         * Each of the next 64 bits is the size of the array
         * in each dimension
         */
        // FIXME need a field for dtype
        try
        {
            char* cmd = msg + CmiMsgHeaderSizeBytes;
            aum_name_t res_name = extract<aum_name_t>(cmd);
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
                    aum_array_t res;
                    if (has_buf)
                    {
                        double* init_buf = (double*) cmd;
                        res = aum::vector(size, init_buf);
                    }
                    else if (has_init)
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
                    if (has_buf)
                    {
                        double* init_buf = (double*) cmd;
                        res = aum::matrix(size1, size2, init_buf);
                    }
                    else if (has_init)
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
                    // FIXME is this correctly caught?
                    CmiAbort("Greater than 2 dimensions not supported");
                }
            }
            
            bool status = true;
            CcsSendReply(1, (void*) &status);
        }
        catch(...)
        {
            bool status = false;
            CcsSendReply(1, (void*) &status);
        }
    }

    static void fetch_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
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
};

void extract_metadata(aum_name_t name, aum_array_t &res, std::vector<uint64_t> &metadata)
{
    metadata.push_back(name);
    std::visit(
        [&](auto& x) {
            using T = std::decay_t<decltype(x)>;
            if constexpr (std::is_same_v<T, aum::matrix>)
            {
                metadata.push_back(x.rows());
                metadata.push_back(x.cols());
            }
            else if constexpr (std::is_same_v<T, aum::vector>)
                metadata.push_back(x.size());
            else if constexpr (!std::is_same_v<T, aum::scalar>)
                CmiAbort("Array type not recognized");
        }, res);
}

aum_array_t calculate(astnode* node, std::vector<uint64_t> &metadata)
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
            aum_array_t s1 = calculate(node->operands[0], metadata);
            aum_array_t s2 = calculate(node->operands[1], metadata);
            aum_array_t res;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr(std::is_same_v<T, V> && !std::is_same_v<T, double>)
                    {
                        if (!node->operands[0]->store)
                            res = x.add_inplace(y);
                        else if (!node->operands[1]->store)
                            res = y.add_inplace(x);
                        else
                            res = x + y;
                    }
                    else
                        CmiAbort("Operation not permitted");
                }, s1, s2);

            if (node->store)
            {
                Server::insert(node->name, res);
                extract_metadata(node->name, res, metadata);
            }
            return res;
        }
        case operation::sub: {
            aum_array_t s1 = calculate(node->operands[0], metadata);
            aum_array_t s2 = calculate(node->operands[1], metadata);
            aum_array_t res;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr(std::is_same_v<T, V> && !std::is_same_v<T, double>)
                    {
                        if (!node->operands[0]->store)
                            res = x.sub_inplace_1(y);
                        else if (!node->operands[1]->store)
                            res = y.sub_inplace_2(x);
                        else
                            res = x - y;
                    }
                    else
                        CmiAbort("Operation not permitted");
                }, s1, s2);

            if (node->store)
            {
                Server::insert(node->name, res);
                extract_metadata(node->name, res, metadata);
            }
            return res;
        }
        case operation::mul: {
            aum_array_t s1 = calculate(node->operands[0], metadata);
            aum_array_t s2 = calculate(node->operands[1], metadata);
            aum_array_t res;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr(std::is_same_v<T, aum::scalar> || 
                            std::is_same_v<T, double>)
                    {
                        // FIXME implement inplace multiply
                        // if (!node->operands[1]->store)
                        //     res = y.mul_inplace(x);
                        // else
                        res = x * y;
                    }
                    else if constexpr(std::is_same_v<V, aum::scalar> || 
                            std::is_same_v<V, double>)
                    {
                        // if (!node->operands[0]->store)
                        //     res = x.mul_inplace(y);
                        // else
                        res = y * x;
                    }
                    else
                        CmiAbort("Operation not permitted");
                }, s1, s2);

            if (node->store)
            {
                Server::insert(node->name, res);
                extract_metadata(node->name, res, metadata);
            }
            return res;
        }
        case operation::div: {
            aum_array_t s1 = calculate(node->operands[0], metadata);
            aum_array_t s2 = calculate(node->operands[1], metadata);
            aum_array_t res;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr((std::is_same_v<T, aum::scalar> || 
                            std::is_same_v<T, double>) && 
                            (std::is_same_v<V, aum::scalar> || 
                            std::is_same_v<V, double>))
                    {
                        res = x / y;
                    }
                    else
                        CmiAbort("Operation not permitted");
                }, s1, s2);

            if (node->store)
            {
                Server::insert(node->name, res);
                extract_metadata(node->name, res, metadata);
            }
            return res;
        }
        case operation::matmul: {
            aum_array_t s1 = calculate(node->operands[0], metadata);
            aum_array_t s2 = calculate(node->operands[1], metadata);
            aum_array_t res;

            std::visit(
                [&](auto& x, auto& y) {
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr(std::is_same_v<T, aum::matrix> && 
                            std::is_same_v<V, aum::matrix>)
                        CmiAbort("Matrix multiplication not yet implemented");
                    else if constexpr((std::is_same_v<T, aum::matrix> || 
                                    std::is_same_v<T, aum::vector>) && 
                                std::is_same_v<V, aum::vector>)
                    {
                        // TODO implement inplace dot
                        res = aum::dot(x, y);
                    }
                    else
                        CmiAbort("Operation not permitted");
                }, s1, s2);

            if (node->store)
            {
                Server::insert(node->name, res);
                extract_metadata(node->name, res, metadata);
            }
            return res;
        }
        case operation::copy: {
            aum_array_t s1 = calculate(node->operands[0], metadata);
            aum_array_t res;

            std::visit(
                [&](auto& x) {
                    using T = std::decay_t<decltype(x)>;
                    if constexpr(std::is_same_v<T, aum::vector> || 
                            std::is_same_v<T, aum::scalar>)
                        res = aum::copy(x);
                    else
                        CmiAbort("Matrix copy not implemented");
                }, s1);

            if (node->store)
            {
                Server::insert(node->name, res);
                extract_metadata(node->name, res, metadata);
            }
            return res;
        }
        case operation::axpy: {
            aum_array_t s1 = calculate(node->operands[0], metadata);
            aum_array_t s2 = calculate(node->operands[1], metadata);
            aum_array_t s3 = calculate(node->operands[2], metadata);
            aum_array_t res;

            std::visit(
                [&](auto& a, auto& x, auto& y) {
                    using S = std::decay_t<decltype(a)>;
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    if constexpr(std::is_same_v<S, aum::scalar> &&
                                std::is_same_v<T, aum::vector> &&
                                std::is_same_v<V, aum::vector>)
                        res = aum::blas::axpy(a, x, y);
                    else
                        CmiAbort("Operation not permitted");
                }, s1, s2, s3);

            if (node->store)
            {
                Server::insert(node->name, res);
                extract_metadata(node->name, res, metadata);
            }
            return res;
        }
        case operation::axpy_multiplier: {
            aum_array_t s1 = calculate(node->operands[0], metadata);
            aum_array_t s2 = calculate(node->operands[1], metadata);
            aum_array_t s3 = calculate(node->operands[2], metadata);
            aum_array_t multiplier = calculate(node->operands[3], metadata);
            aum_array_t res;

            std::visit(
                [&](auto& multiplier, auto& a, auto& x, auto& y) {
                    using S = std::decay_t<decltype(a)>;
                    using T = std::decay_t<decltype(x)>;
                    using V = std::decay_t<decltype(y)>;
                    using M = std::decay_t<decltype(multiplier)>;
                    if constexpr(std::is_same_v<M, double> &&
                                std::is_same_v<S, aum::scalar> &&
                                std::is_same_v<T, aum::vector> &&
                                std::is_same_v<V, aum::vector>)
                        res = aum::blas::axpy(multiplier, a, x, y);
                    else
                        CmiAbort("Operation not permitted");
                }, multiplier, s1, s2, s3);

            if (node->store)
            {
                Server::insert(node->name, res);
                extract_metadata(node->name, res, metadata);
            }
            return res;
        }
        default: {
            CmiAbort("Operation not implemented");
        }
    }
}

