




/* ---------------- method closures -------------- */
#ifndef CK_TEMPLATES_ONLY
#endif /* CK_TEMPLATES_ONLY */

#ifndef CK_TEMPLATES_ONLY

    struct Closure_Main::handle_command_2_closure : public SDAG::Closure {
            int epoch;
            uint8_t kind;
            uint32_t size;
            char *cmd;

      CkMarshallMsg* _impl_marshall;
      char* _impl_buf_in;
      int _impl_buf_size;

      handle_command_2_closure() {
        init();
        _impl_marshall = 0;
        _impl_buf_in = 0;
        _impl_buf_size = 0;
      }
      handle_command_2_closure(CkMigrateMessage*) {
        init();
        _impl_marshall = 0;
        _impl_buf_in = 0;
        _impl_buf_size = 0;
      }
            int & getP0() { return epoch;}
            uint8_t & getP1() { return kind;}
            uint32_t & getP2() { return size;}
            char *& getP3() { return cmd;}
      void pup(PUP::er& __p) {
        __p | epoch;
        __p | kind;
        __p | size;
        packClosure(__p);
        __p | _impl_buf_size;
        bool hasMsg = (_impl_marshall != 0); __p | hasMsg;
        if (hasMsg) CkPupMessage(__p, (void**)&_impl_marshall);
        else PUParray(__p, _impl_buf_in, _impl_buf_size);
        if (__p.isUnpacking()) {
          char *impl_buf = _impl_marshall ? _impl_marshall->msgBuf : _impl_buf_in;
          PUP::fromMem implP(impl_buf);
  PUP::detail::TemporaryObjectHolder<int> epoch;
  implP|epoch;
  PUP::detail::TemporaryObjectHolder<uint8_t> kind;
  implP|kind;
  PUP::detail::TemporaryObjectHolder<uint32_t> size;
  implP|size;
  int impl_off_cmd, impl_cnt_cmd;
  implP|impl_off_cmd;
  implP|impl_cnt_cmd;
          impl_buf+=CK_ALIGN(implP.size(),16);
          cmd = (char *)(impl_buf+impl_off_cmd);
        }
      }
      virtual ~handle_command_2_closure() {
        if (_impl_marshall) CmiFree(UsrToEnv(_impl_marshall));
      }
      PUPable_decl(SINGLE_ARG(handle_command_2_closure));
    };
#endif /* CK_TEMPLATES_ONLY */



#ifndef CK_TEMPLATES_ONLY
  PUPable_def(pow_t)
#endif /* CK_TEMPLATES_ONLY */

#ifndef CK_TEMPLATES_ONLY
  PUPable_def(log_t)
#endif /* CK_TEMPLATES_ONLY */

#ifndef CK_TEMPLATES_ONLY
  PUPable_def(exp_t)
#endif /* CK_TEMPLATES_ONLY */

#ifndef CK_TEMPLATES_ONLY
  PUPable_def(abs_t)
#endif /* CK_TEMPLATES_ONLY */

/* DEFS: mainchare Main: Chare{
Main(CkArgMsg* impl_msg);
void handle_command(int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd);
};
 */
#ifndef CK_TEMPLATES_ONLY
 int CkIndex_Main::__idx=0;
#endif /* CK_TEMPLATES_ONLY */
#ifndef CK_TEMPLATES_ONLY
#endif /* CK_TEMPLATES_ONLY */
#ifndef CK_TEMPLATES_ONLY
/* DEFS: Main(CkArgMsg* impl_msg);
 */
CkChareID CProxy_Main::ckNew(CkArgMsg* impl_msg, int impl_onPE)
{
  CkChareID impl_ret;
  CkCreateChare(CkIndex_Main::__idx, CkIndex_Main::idx_Main_CkArgMsg(), impl_msg, &impl_ret, impl_onPE);
  return impl_ret;
}
void CProxy_Main::ckNew(CkArgMsg* impl_msg, CkChareID* pcid, int impl_onPE)
{
  CkCreateChare(CkIndex_Main::__idx, CkIndex_Main::idx_Main_CkArgMsg(), impl_msg, pcid, impl_onPE);
}

// Entry point registration function
int CkIndex_Main::reg_Main_CkArgMsg() {
  int epidx = CkRegisterEp("Main(CkArgMsg* impl_msg)",
      reinterpret_cast<CkCallFnPtr>(_call_Main_CkArgMsg), CMessage_CkArgMsg::__idx, __idx, 0);
  CkRegisterMessagePupFn(epidx, (CkMessagePupFn)CkArgMsg::ckDebugPup);
  return epidx;
}

void CkIndex_Main::_call_Main_CkArgMsg(void* impl_msg, void* impl_obj_void)
{
  Main* impl_obj = static_cast<Main*>(impl_obj_void);
  new (impl_obj_void) Main((CkArgMsg*)impl_msg);
}
#endif /* CK_TEMPLATES_ONLY */

#ifndef CK_TEMPLATES_ONLY
/* DEFS: void handle_command(int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd);
 */
void CProxy_Main::handle_command(int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd, const CkEntryOptions *impl_e_opts)
{
  ckCheck();
  //Marshall: int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd
  int impl_off=0;
  int impl_arrstart=0;
  int impl_off_cmd, impl_cnt_cmd;
  impl_off_cmd=impl_off=CK_ALIGN(impl_off,sizeof(char));
  impl_off+=(impl_cnt_cmd=sizeof(char)*(size));
  { //Find the size of the PUP'd data
    PUP::sizer implP;
    implP|epoch;
    //Have to cast away const-ness to get pup routine
    implP|(typename std::remove_cv<typename std::remove_reference<uint8_t>::type>::type &)kind;
    //Have to cast away const-ness to get pup routine
    implP|(typename std::remove_cv<typename std::remove_reference<uint32_t>::type>::type &)size;
    implP|impl_off_cmd;
    implP|impl_cnt_cmd;
    impl_arrstart=CK_ALIGN(implP.size(),16);
    impl_off+=impl_arrstart;
  }
  CkMarshallMsg *impl_msg=CkAllocateMarshallMsg(impl_off,impl_e_opts);
  { //Copy over the PUP'd data
    PUP::toMem implP((void *)impl_msg->msgBuf);
    implP|epoch;
    //Have to cast away const-ness to get pup routine
    implP|(typename std::remove_cv<typename std::remove_reference<uint8_t>::type>::type &)kind;
    //Have to cast away const-ness to get pup routine
    implP|(typename std::remove_cv<typename std::remove_reference<uint32_t>::type>::type &)size;
    implP|impl_off_cmd;
    implP|impl_cnt_cmd;
  }
  char *impl_buf=impl_msg->msgBuf+impl_arrstart;
  memcpy(impl_buf+impl_off_cmd,cmd,impl_cnt_cmd);
  if (ckIsDelegated()) {
    int destPE=CkChareMsgPrep(CkIndex_Main::idx_handle_command_marshall2(), impl_msg, &ckGetChareID());
    if (destPE!=-1) ckDelegatedTo()->ChareSend(ckDelegatedPtr(),CkIndex_Main::idx_handle_command_marshall2(), impl_msg, &ckGetChareID(),destPE);
  } else {
    CkSendMsg(CkIndex_Main::idx_handle_command_marshall2(), impl_msg, &ckGetChareID(),0);
  }
}

// Entry point registration function
int CkIndex_Main::reg_handle_command_marshall2() {
  int epidx = CkRegisterEp("handle_command(int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd)",
      reinterpret_cast<CkCallFnPtr>(_call_handle_command_marshall2), CkMarshallMsg::__idx, __idx, 0+CK_EP_NOKEEP);
  CkRegisterMarshallUnpackFn(epidx, _callmarshall_handle_command_marshall2);
  CkRegisterMessagePupFn(epidx, _marshallmessagepup_handle_command_marshall2);

  return epidx;
}

void CkIndex_Main::_call_handle_command_marshall2(void* impl_msg, void* impl_obj_void)
{
  Main* impl_obj = static_cast<Main*>(impl_obj_void);
  CkMarshallMsg *impl_msg_typed=(CkMarshallMsg *)impl_msg;
  char *impl_buf=impl_msg_typed->msgBuf;
  envelope *env = UsrToEnv(impl_msg_typed);
  /*Unmarshall pup'd fields: int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd*/
  PUP::fromMem implP(impl_buf);
  PUP::detail::TemporaryObjectHolder<int> epoch;
  implP|epoch;
  PUP::detail::TemporaryObjectHolder<uint8_t> kind;
  implP|kind;
  PUP::detail::TemporaryObjectHolder<uint32_t> size;
  implP|size;
  int impl_off_cmd, impl_cnt_cmd;
  implP|impl_off_cmd;
  implP|impl_cnt_cmd;
  impl_buf+=CK_ALIGN(implP.size(),16);
  /*Unmarshall arrays:*/
  char *cmd=(char *)(impl_buf+impl_off_cmd);
  impl_obj->handle_command(std::move(epoch.t), std::move(kind.t), std::move(size.t), cmd);
}
int CkIndex_Main::_callmarshall_handle_command_marshall2(char* impl_buf, void* impl_obj_void) {
  Main* impl_obj = static_cast<Main*>(impl_obj_void);
  envelope *env = UsrToEnv(impl_buf);
  /*Unmarshall pup'd fields: int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd*/
  PUP::fromMem implP(impl_buf);
  PUP::detail::TemporaryObjectHolder<int> epoch;
  implP|epoch;
  PUP::detail::TemporaryObjectHolder<uint8_t> kind;
  implP|kind;
  PUP::detail::TemporaryObjectHolder<uint32_t> size;
  implP|size;
  int impl_off_cmd, impl_cnt_cmd;
  implP|impl_off_cmd;
  implP|impl_cnt_cmd;
  impl_buf+=CK_ALIGN(implP.size(),16);
  /*Unmarshall arrays:*/
  char *cmd=(char *)(impl_buf+impl_off_cmd);
  impl_obj->handle_command(std::move(epoch.t), std::move(kind.t), std::move(size.t), cmd);
  return implP.size();
}
void CkIndex_Main::_marshallmessagepup_handle_command_marshall2(PUP::er &implDestP,void *impl_msg) {
  CkMarshallMsg *impl_msg_typed=(CkMarshallMsg *)impl_msg;
  char *impl_buf=impl_msg_typed->msgBuf;
  envelope *env = UsrToEnv(impl_msg_typed);
  /*Unmarshall pup'd fields: int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd*/
  PUP::fromMem implP(impl_buf);
  PUP::detail::TemporaryObjectHolder<int> epoch;
  implP|epoch;
  PUP::detail::TemporaryObjectHolder<uint8_t> kind;
  implP|kind;
  PUP::detail::TemporaryObjectHolder<uint32_t> size;
  implP|size;
  int impl_off_cmd, impl_cnt_cmd;
  implP|impl_off_cmd;
  implP|impl_cnt_cmd;
  impl_buf+=CK_ALIGN(implP.size(),16);
  /*Unmarshall arrays:*/
  char *cmd=(char *)(impl_buf+impl_off_cmd);
  if (implDestP.hasComments()) implDestP.comment("epoch");
  implDestP|epoch;
  if (implDestP.hasComments()) implDestP.comment("kind");
  implDestP|kind;
  if (implDestP.hasComments()) implDestP.comment("size");
  implDestP|size;
  if (implDestP.hasComments()) implDestP.comment("cmd");
  implDestP.synchronize(PUP::sync_begin_array);
  for (int impl_i=0;impl_i*(sizeof(*cmd))<impl_cnt_cmd;impl_i++) {
    implDestP.synchronize(PUP::sync_item);
    implDestP|cmd[impl_i];
  }
  implDestP.synchronize(PUP::sync_end_array);
}
PUPable_def(SINGLE_ARG(Closure_Main::handle_command_2_closure))
#endif /* CK_TEMPLATES_ONLY */

#ifndef CK_TEMPLATES_ONLY
#endif /* CK_TEMPLATES_ONLY */
#ifndef CK_TEMPLATES_ONLY
void CkIndex_Main::__register(const char *s, size_t size) {
  __idx = CkRegisterChare(s, size, TypeMainChare);
  CkRegisterBase(__idx, CkIndex_Chare::__idx);
  // REG: Main(CkArgMsg* impl_msg);
  idx_Main_CkArgMsg();
  CkRegisterMainChare(__idx, idx_Main_CkArgMsg());

  // REG: void handle_command(int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd);
  idx_handle_command_marshall2();

}
#endif /* CK_TEMPLATES_ONLY */

#ifndef CK_TEMPLATES_ONLY
void _registerserver(void)
{
  static int _done = 0; if(_done) return; _done = 1;
  _registerlibcharmtyles();

      PUPable_reg(pow_t);

      PUPable_reg(log_t);

      PUPable_reg(exp_t);

      PUPable_reg(abs_t);

/* REG: mainchare Main: Chare{
Main(CkArgMsg* impl_msg);
void handle_command(int epoch, const uint8_t &kind, const uint32_t &size, const char *cmd);
};
*/
  CkIndex_Main::__register("Main", sizeof(Main));

}
extern "C" void CkRegisterMainModule(void) {
  _registerserver();
}
#endif /* CK_TEMPLATES_ONLY */
#ifndef CK_TEMPLATES_ONLY
template <>
void CBase_Main::virtual_pup(PUP::er &p) {
    recursive_pup<Main>(dynamic_cast<Main*>(this), p);
}
#endif /* CK_TEMPLATES_ONLY */
