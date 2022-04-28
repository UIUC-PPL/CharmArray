#include <aum/aum.hpp>
#include <unordered_map>
#include <variant>
#include "converse.h"
#include "conv-ccs.h"

#include "server.decl.h"

#define NAME_SIZE 8
    
using aum_name_t = uint64_t;
using aum_array_t = std::variant<aum::scalar, aum::vector, aum::matrix>;

std::unordered_map<aum_name_t, aum_array_t> symbol_table;
aum_name_t next_name = 0;

class Main : public CBase_Main
{
public:
    Main(CkArgMsg* msg) 
    {
        register_handlers();
    }

    inline static void insert(aum_name_t name, aum_array_t arr)
    {
        CkPrintf("Created array %lld on server\n", name);
        symbol_table.emplace(name, arr);
    }

    inline static void remove(aum_name_t name)
    {
        symbol_table.erase(name);
    }

    inline static aum_name_t get_name()
    {
        return next_name++;
    }

    static aum_array_t& lookup(aum_name_t name)
    {
        auto find = symbol_table.find(name);
        if (find == std::end(symbol_table))
            CmiAbort("Symbol %i not found", name);
        return find->second;
    }

    static void operation_handler(char* msg)
    {
        auto res_name = get_name();
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint32_t* opcode = reinterpret_cast<uint32_t*>(cmd);
        switch(*opcode)
        {
            case 0: {
                // no op
                CmiAbort("No op not implemented");
            }
            case 1: {
                // addition
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(cmd + 12);
                aum_array_t& c1 = lookup(*name1);
                aum_array_t& c2 = lookup(*name2);
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
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(cmd + 12);
                aum_array_t& c1 = lookup(*name1);
                aum_array_t& c2 = lookup(*name2);
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
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(cmd + 12);
                aum_array_t& c1 = lookup(*name1);
                aum_array_t& c2 = lookup(*name2);
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
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(cmd + 12);
                aum_array_t& c1 = lookup(*name1);
                aum_array_t& c2 = lookup(*name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, aum::scalar> && std::is_same_v<V, aum::scalar>)
                            res = x / y;
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2);
                break;
            }
            case 5: {
                // mat/vec mul
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(cmd + 12);
                aum_array_t& c1 = lookup(*name1);
                aum_array_t& c2 = lookup(*name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, aum::matrix> && std::is_same_v<V, aum::matrix>)
                            // Matmul here
                            CmiAbort("Matrix multiplication not yet implemented");
                        else if constexpr((std::is_same_v<T, aum::matrix> || 
                                    std::is_same_v<T, aum::vector>) && std::is_same_v<V, aum::vector>)
                            res = aum::dot(x, y);
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2);
                break;
            }
            // FIXME implement a matrix copy
            case 6: {
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                aum_array_t& c1 = lookup(*name1);
                std::visit(
                    [&](auto& x) {
                        using T = std::decay_t<decltype(x)>;
                        if constexpr(std::is_same_v<T, aum::scalar> || std::is_same_v<T, aum::vector>)
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
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(cmd + 12);
                uint64_t* name3 = reinterpret_cast<uint64_t*>(cmd + 20);
                aum_array_t& c1 = lookup(*name1);
                aum_array_t& c2 = lookup(*name2);
                aum_array_t& c3 = lookup(*name3);
                std::visit(
                    [&](auto& a, auto& x, auto& y) {
                        using S = std::decay_t<decltype(a)>;
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<S, aum::scalar> && std::is_same_v<T, aum::vector> &&
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
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(cmd + 12);
                uint64_t* name3 = reinterpret_cast<uint64_t*>(cmd + 20);
                double* multiplier = reinterpret_cast<double*>(cmd + 28);
                CkPrintf("Multiplier = %f\n", *multiplier);
                aum_array_t& c1 = lookup(*name1);
                aum_array_t& c2 = lookup(*name2);
                aum_array_t& c3 = lookup(*name3);
                std::visit(
                    [&](auto& a, auto& x, auto& y) {
                        using S = std::decay_t<decltype(a)>;
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<S, aum::scalar> && std::is_same_v<T, aum::vector> &&
                                std::is_same_v<V, aum::vector>)
                            res = aum::blas::axpy(*multiplier, a, x, y);
                        else
                            CmiAbort("Operation not permitted");
                        insert(res_name, res);
                    }, c1, c2, c3);
                break;
            }
            case 9: {
                // multiply
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                double* y = reinterpret_cast<double*>(cmd + 12);
                aum_array_t& c1 = lookup(*name1);
                std::visit(
                    [&](auto& x) {
                        using T = std::decay_t<decltype(x)>;
                        aum_array_t res;
                        res = *y * x;
                        insert(res_name, res);
                    }, c1);
                break;
            }
            case 10: {
                // division
                uint64_t* name1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(cmd + 12);
                aum_array_t& c1 = lookup(*name1);
                aum_array_t& c2 = lookup(*name2);
                std::visit(
                    [&](auto& x, auto& y) {
                        using T = std::decay_t<decltype(x)>;
                        using V = std::decay_t<decltype(y)>;
                        aum_array_t res;
                        if constexpr(std::is_same_v<T, aum::scalar> && std::is_same_v<V, aum::scalar>)
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
        CcsSendReply(NAME_SIZE, (void*) &res_name);
    }
    
    static void creation_handler(char* msg)
    {
        /* First 32 bits are number of dimensions
         * Each of the next 64 bits is the size of the array
         * in each dimension
         */
        // FIXME need a field for dtype
        auto res_name = get_name();
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        uint32_t* ndim = reinterpret_cast<uint32_t*>(cmd);
        bool* has_init = reinterpret_cast<bool*>(cmd + 4);
        switch(*ndim)
        {
            case 0: {
                // create scalar
                CmiAbort("Not implemented");
            }
            case 1: {
                // create vector
                uint64_t* size = reinterpret_cast<uint64_t*>(cmd + 5);
                aum_array_t res;
                if (*has_init)
                {
                    double* init_value = reinterpret_cast<double*>(cmd + 13);
                    res = aum::vector(*size, *init_value);
                }
                else
                {
                    res = aum::vector(*size, aum::random{});
                }
                insert(res_name, res);
                break;
            }
            case 2: {
                // create matrix
                uint64_t* size1 = reinterpret_cast<uint64_t*>(cmd + 5);
                uint64_t* size2 = reinterpret_cast<uint64_t*>(cmd + 13);
                aum_array_t res;
                if (*has_init)
                {
                    double* init_value = reinterpret_cast<double*>(cmd + 21);
                    res = aum::matrix(*size1, *size2, *init_value);
                }
                else
                {
                    res = aum::matrix(*size1, *size2, aum::random{});
                }
                insert(res_name, res);
                break;
            }
            default: {
                CmiAbort("Greater than 2 dimensions not supported");
            }
        }
        CcsSendReply(NAME_SIZE, (void*) &res_name);
    }

    static void fetch_handler(char* msg)
    {
        char* cmd = msg + CmiMsgHeaderSizeBytes;
        aum_name_t* name = reinterpret_cast<aum_name_t*>(cmd);
        aum_array_t& arr = lookup(*name);
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
        uint64_t* name = reinterpret_cast<uint64_t*>(cmd);
        remove(*name);
        CkPrintf("Deleted array %lld on server\n", *name);
    }

    inline static void exit_server(char* msg)
    {
        CkExit();
    }

    void register_handlers()
    {
        CcsRegisterHandler("aum_operation", (CmiHandler) operation_handler);
        CcsRegisterHandler("aum_creation", (CmiHandler) creation_handler);
        CcsRegisterHandler("aum_fetch", (CmiHandler) fetch_handler);
        CcsRegisterHandler("aum_delete", (CmiHandler) delete_handler);
        CcsRegisterHandler("aum_exit", (CmiHandler) exit_server);
    }
};

#include "server.def.h"
