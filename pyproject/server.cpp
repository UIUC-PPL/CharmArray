#include <aum/aum.hpp>
#include <PyBind/operation.hpp>
#include <unordered_map>
#include "converse.h"
#include "conv-ccs.h"

#include "server.decl.h"

#define REPLY_SIZE 8
    
using aum_name_t = uint64_t;

std::unordered_map<aum_name_t, aum::matrix> symbol_table;
aum_name_t next_name;

class Main : public CBase_Main
{
private:
    // FIXME - how to handle different types?
public:
    Main(CkArgMsg* msg) 
    {
        next_name = 0;
        register_handlers();
    }

    inline static void insert(aum_name_t name, aum::matrix const& mat)
    {
        symbol_table.emplace(name, mat);
    }

    inline static aum_name_t get_name()
    {
        return next_name++;
    }

    static aum::matrix& lookup(aum_name_t name)
    {
        auto find = symbol_table.find(name);
        if (find == std::end(symbol_table))
            CmiAbort("Symbol %i not found", name);
        return find->second;
    }

    static void operation_handler(char* msg)
    {
        auto res_name = get_name();
        uint32_t* opcode = reinterpret_cast<uint32_t*>(msg);
        switch(*opcode)
        {
            case 0: {
                // no op
                CmiAbort("No op not implemented");
            }
            case 1: {
                // addition
                uint64_t* name1 = reinterpret_cast<uint64_t*>(msg + 4);
                uint64_t* name2 = reinterpret_cast<uint64_t*>(msg + 12);
                auto& c1 = lookup(*name1);
                auto& c2 = lookup(*name2);
                auto op = aum::bind::operation<aum::matrix, aum::matrix>(c1, c2);
                auto res = op.execute(aum::bind::oper::add);
                insert(res_name, res);
            }
            default: {
                CmiAbort("Operation not implemented");
            }
        }
        CcsSendReply(REPLY_SIZE, (void*) &res_name);
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
        switch(*ndim)
        {
            case 0: {
                // create scalar
                CmiAbort("Not implemented");
            }
            case 1: {
                // create vector
                CmiAbort("Not implemented");
            }
            case 2: {
                // create matrix
                uint64_t* size1 = reinterpret_cast<uint64_t*>(cmd + 4);
                uint64_t* size2 = reinterpret_cast<uint64_t*>(cmd + 12);
                auto res = aum::matrix(*size1, *size2);
                insert(res_name, res);
                break;
            }
            default: {
                CmiAbort("Greater than 2 dimensions not supported");
            }
        }
        CkPrintf("Created array %lld on server\n", res_name);
        CcsSendReply(REPLY_SIZE, (void*) &res_name);
    }

    static void fetch_handler(char* msg)
    {
        CmiAbort("Fetching not implemented");
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
        CcsRegisterHandler("aum_exit", (CmiHandler) exit_server);
    }
};

#include "server.def.h"
