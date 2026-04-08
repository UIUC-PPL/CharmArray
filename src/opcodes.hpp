#pragma once

enum class Opcode {
    COPY = -5,
    SET_REGION = -4,
    GET_REGION = -3,
    CREATE = -2,
    NOOP = -1,
    ADD = 0,
    SUB = 1,
    MUL = 2,
    DIV = 3,
    MATMUL = 4,
    TANH = 5,
    EXP = 6,
    TILE = 7,
    REDUCE = 8,
    MATMATMUL = 9,
    DIAG = 10
};

/// Returns true for binary elementwise ops.
inline bool is_binary_elementwise(Opcode op) {
    switch (op) {
    case Opcode::ADD:
    case Opcode::SUB:
    case Opcode::MUL:
    case Opcode::DIV:
        return true;
    default:
        return false;
    }
}

/// Returns true for unary elementwise ops.
inline bool is_unary_elementwise(Opcode op) {
    switch (op) {
    case Opcode::TANH:
    case Opcode::EXP:
        return true;
    default:
        return false;
    }
}

/// Returns true for any elementwise op (binary or unary).
inline bool is_elementwise(Opcode op) {
    return is_binary_elementwise(op) || is_unary_elementwise(op);
}
