#include <aum/aum.hpp>

template<class T>
inline T extract(char* &msg, bool increment=true)
{
    T arg = *(reinterpret_cast<T*>(msg));
    if (increment)
        msg += sizeof(T);
    return arg;
}

inline uint64_t extract_epoch(char* msg)
{
    char* cmd = msg + CmiMsgHeaderSizeBytes;
    return extract<uint64_t>(cmd);
}

class epoch_compare
{
public:
    bool operator()(char* msg1, char* msg2)
    {
        uint64_t epoch1 = extract_epoch(msg1);
        uint64_t epoch2 = extract_epoch(msg2);
        return epoch1 < epoch2;
    }
};

