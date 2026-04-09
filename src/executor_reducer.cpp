#include "backend_internal.hpp"

#include <cstring>

#ifdef USE_KOKKOS
#include <KokkosBlas1_dot.hpp>
#endif

CkReduction::reducerType reduce_dot_sum_type;

CkReductionMsg* reduce_dot_sum(int nMsg, CkReductionMsg** msgs) {
    // All messages have the same layout: ReduceContrib (24 bytes).
    // Sum the value field based on dtype, preserve metadata from first msg.
    ReduceContrib result;
    memcpy(&result, msgs[0]->getData(), sizeof(ReduceContrib));

    DType dt = static_cast<DType>(result.dtype_int);
    for (int i = 1; i < nMsg; ++i) {
        ReduceContrib other;
        memcpy(&other, msgs[i]->getData(), sizeof(ReduceContrib));
        switch (dt) {
        case DType::FLOAT32: {
            float a, b;
            memcpy(&a, result.value, sizeof(float));
            memcpy(&b, other.value, sizeof(float));
            a += b;
            memcpy(result.value, &a, sizeof(float));
            break;
        }
        case DType::FLOAT64: {
            double a, b;
            memcpy(&a, result.value, sizeof(double));
            memcpy(&b, other.value, sizeof(double));
            a += b;
            memcpy(result.value, &a, sizeof(double));
            break;
        }
        case DType::INT32: {
            int32_t a, b;
            memcpy(&a, result.value, sizeof(int32_t));
            memcpy(&b, other.value, sizeof(int32_t));
            a += b;
            memcpy(result.value, &a, sizeof(int32_t));
            break;
        }
        case DType::INT64: {
            int64_t a, b;
            memcpy(&a, result.value, sizeof(int64_t));
            memcpy(&b, other.value, sizeof(int64_t));
            a += b;
            memcpy(result.value, &a, sizeof(int64_t));
            break;
        }
        }
    }
    return CkReductionMsg::buildNew(sizeof(ReduceContrib), &result);
}

void register_reduce_dot_sum() {
    reduce_dot_sum_type = CkReduction::addReducer(reduce_dot_sum);
}

template <int N>
DType determine_dtype(DAGNode* node, std::unordered_map<int, CTArrayBase<N>*>& arrays) {
    // Check AST roots for dtype info
    for (ASTNode* root : node->ast->roots)
        if (root->dtype != DType::FLOAT32)
            return root->dtype;
    // Fall back to first referenced array's dtype
    for (ASTNode* root : node->ast->roots)
        for (ASTNode* operand : root->operands)
            if (!operand->is_scalar && !operand->is_broadcast) {
                auto it = arrays.find(operand->result_name);
                if (it != arrays.end())
                    return it->second->dtype;
            }
    return DType::FLOAT32;
}

template DType determine_dtype<1>(DAGNode*, std::unordered_map<int, CTArrayBase<1>*>&);
template DType determine_dtype<2>(DAGNode*, std::unordered_map<int, CTArrayBase<2>*>&);
template DType determine_dtype<3>(DAGNode*, std::unordered_map<int, CTArrayBase<3>*>&);
