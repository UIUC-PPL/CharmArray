#include <charmtyles/charmtyles.hpp>
#include "server.hpp"
#include "converse.h"
#include "conv-ccs.h"

#include "server.decl.h"


class Main : public CBase_Main
{
public:
    Main(CkArgMsg* msg) 
    {
        Server::initialize();
        register_handlers();
        ct::init();
#ifndef NDEBUG
        CkPrintf("Initialization done\n");
#endif
    }

    void register_handlers()
    {
        CcsRegisterHandler("aum_connect", (CmiHandler) Server::connection_handler);
        CcsRegisterHandler("aum_disconnect", (CmiHandler) Server::disconnection_handler);
        CcsRegisterHandler("aum_operation", (CmiHandler) Server::operation_handler);
        CcsRegisterHandler("aum_creation", (CmiHandler) Server::creation_handler);
        CcsRegisterHandler("aum_fetch", (CmiHandler) Server::fetch_handler);
        CcsRegisterHandler("aum_delete", (CmiHandler) Server::delete_handler);
        CcsRegisterHandler("aum_exit", (CmiHandler) Server::exit_server);
    }
};

#include "server.def.h"
