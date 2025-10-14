#include <charmtyles/charmtyles.hpp>
#include <stack>
#include <cinttypes>
#include <variant>
#include <queue>
#include <unordered_map>
#include <cmath>
#include <vector>
#include <cstring>

using ctop = ct::util::Operation;
using ct_name_t = uint64_t;
using ct_array_t = std::variant<ct::scalar, ct::vector, ct::matrix>;
std::unordered_map<ct_name_t, ct_array_t> symbol_table;

inline static void insert(ct_name_t name, ct_array_t arr) noexcept {
  CkPrintf("Created array %" PRIu64 " on server\n", name);
  symbol_table[name] = std::move(arr);
}

inline static void remove(ct_name_t name) noexcept {
  symbol_table.erase(name);
}

static ct_array_t &lookup(ct_name_t name) {
  auto find = symbol_table.find(name);
  CkPrintf("Looking up array %" PRIu64 " on server\n", name);
  if (find == std::end(symbol_table))
    CmiAbort("Symbol%" PRIu64 "not found", name);
  return find->second;
}

template <typename T>
inline T extract(char *&msg) noexcept {
  T arg = *(reinterpret_cast<T *>(msg));
  msg += sizeof(T);
  return arg;
}

template <typename T>
inline T peek(char* &msg) noexcept {
  return *(reinterpret_cast<T*>(msg));
}

template<typename tensorType, typename tensorAstNodeType>
std::vector<tensorAstNodeType> faster_tortoise(char *cmd, bool flush = false);

template<typename tensorType, typename tensorAstNodeType>
std::pair<uint8_t, uint64_t> getFlushedOperand(char* cmd) {
  char* recurse_cmd = cmd;

  uint8_t marker = extract<uint8_t>(cmd);
  if (marker != 2) CmiAbort("Matmuls only supported with Tensor Types");

  uint8_t dim = extract<uint8_t>(cmd);
  if (dim < 1 || dim > 2) CmiAbort("Matmuls not supported with dimension%" PRIu8 "", dim);

  cmd += dim * sizeof(uint64_t);

  uint32_t opcode = extract<uint32_t>(cmd);
  if (opcode) faster_tortoise<tensorType, tensorAstNodeType>(recurse_cmd, true);

  cmd += sizeof(bool);

  uint64_t tensorID = extract<uint64_t>(cmd);
  return {dim, tensorID};
}

ctop inline to_ctop(uint64_t opcode) noexcept {
  if(opcode>=41 and opcode<=52) return ctop::unary_expr;
  if(opcode>=71 and opcode<=84) return ctop::binary_expr;
  switch (opcode) {
    case 0:  return ctop::noop;
    case 1:  return ctop::add;
    case 2:  return ctop::sub;
    case 3:  return ctop::multiply;
    case 4:  return ctop::divide;
    case 5:  return ctop::matmul;
    case 6:  return ctop::copy;
    case 9: return ctop::greater;
    case 10: return ctop::lesser;
    case 11: return ctop::geq;
    case 12: return ctop::leq;
    case 13: return ctop::eq;
    case 14: return ctop::neq;
    case 15: return ctop::logical_and;
    case 16: return ctop::logical_or;
    case 17: return ctop::logical_not;
    case 18: return ctop::where;
    default: return ctop::noop;
  }
}

std::shared_ptr<ct::unary_operator> to_ct_unary(uint64_t opcode, const std::vector<double>& args) noexcept {
  switch(opcode) {
    case 41: return ct::unary_ops::exp(args);
    case 42: return ct::unary_ops::log(args);
    case 43: return ct::unary_ops::abs(args);
    case 44: return ct::unary_ops::negate(args);
    case 45: return ct::unary_ops::square(args);
    case 46: return ct::unary_ops::sqrt(args);
    case 47: return ct::unary_ops::reciprocal(args);
    case 48: return ct::unary_ops::sin(args);
    case 49: return ct::unary_ops::cos(args);
    case 50: return ct::unary_ops::relu(args);
    case 51: return ct::unary_ops::scale(args);
    case 52: return ct::unary_ops::add_constant(args);
    default: return nullptr;
  }
}

std::shared_ptr<ct::binary_operator> to_ct_binary(uint64_t opcode, const std::vector<double>& args) noexcept {
  switch(opcode) {
    case 71: return ct::binary_ops::add(args);
    case 72: return ct::binary_ops::subtract(args);
    case 73: return ct::binary_ops::multiply(args);
    case 74: return ct::binary_ops::divide(args);
    case 75: return ct::binary_ops::power(args);
    case 76: return ct::binary_ops::modulo(args);
    case 77: return ct::binary_ops::max(args);
    case 78: return ct::binary_ops::min(args);
    case 79: return ct::binary_ops::greater_than(args);
    case 80: return ct::binary_ops::less_than(args);
    case 81: return ct::binary_ops::equal(args);
    case 82: return ct::binary_ops::atan2(args);
    case 83: return ct::binary_ops::weighted_average(args);
    case 84: return ct::binary_ops::axpy(args);
    default: return nullptr;
  }
}

template<typename tensorType, typename tensorAstNodeType>
std::vector<tensorAstNodeType> faster_tortoise(char *cmd, bool flush)
{
  uint8_t marker = extract<uint8_t>(cmd);

  std::vector<uint64_t> shape; shape.reserve(2);

  if (marker == 0) {
    if constexpr (std::is_same_v<tensorType, ct::vector>) {
      shape.push_back(extract<uint64_t>(cmd));
    } else if constexpr (std::is_same_v<tensorType, ct::matrix>) {
      shape.push_back(extract<uint64_t>(cmd));
      shape.push_back(extract<uint64_t>(cmd));
    }
    double value = extract<double>(cmd);
    tensorAstNodeType temp_node(0, ctop::broadcast, value, shape);
    return {temp_node};
  }

  uint8_t dims = extract<uint8_t>(cmd);

  for(uint8_t i = 0; i < dims; i++)
    shape.push_back(extract<uint64_t>(cmd));

  uint32_t opcode = extract<uint32_t>(cmd);
  bool store  = extract<bool>(cmd);
  uint64_t tensorID = extract<uint64_t>(cmd);
  ckout<<"for tensorid "<<tensorID<<" -> "<<store<<endl;

  if (opcode == 0) {
    if (marker == 1) {
      auto& tmp = std::get<ct::scalar>(lookup(tensorID));
      double result = tmp.get();
      tensorAstNodeType temp_node(0, ctop::broadcast, result, shape);
      return {temp_node};
    } else {
      const auto& tmp = std::get<tensorType>(lookup(tensorID));
      return tmp();
    }
  }

  // Args for custom unops/binops
  uint32_t numArgs  = extract<uint32_t>(cmd);
  std::vector<double> args;
  for(uint32_t i = 0; i < numArgs; i++)
    args.push_back(extract<double>(cmd));

  tensorAstNodeType rootNode;
  ctop ctopcode = to_ctop(opcode);
  if (ctopcode == ctop::unary_expr) {
    rootNode = tensorAstNodeType(-1, ctopcode, to_ct_unary(opcode, args), shape);
  } else if(ctopcode==ctop::binary_expr){
    rootNode = tensorAstNodeType(-1, ctopcode, to_ct_binary(opcode, args), shape);
  } else {
    rootNode = tensorAstNodeType(ctopcode, shape);
  }
  std::vector<tensorAstNodeType> ast;

  uint8_t  numOperands = extract<uint8_t>(cmd);

  // when we encounter a matmul, we treat it as a :
  // 1. a dot product returning a scalar if both the operands are vectors
  // 2. a dot product returning a vector if one operand is a matrix and the other a vector
  // 3. a gemm returning a matrix if both the operands are matrices
  if (ctopcode == ctop::matmul) {
    uint32_t operand_size = extract<uint32_t>(cmd);
    std::pair<uint8_t, uint64_t> xOperandInfo = getFlushedOperand<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;
    operand_size = extract<uint32_t>(cmd);
    std::pair<uint8_t, uint64_t> yOperandInfo = getFlushedOperand<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;

    const uint8_t& xDim = xOperandInfo.first;
    const uint8_t& yDim = yOperandInfo.first;
    const uint64_t& xID = xOperandInfo.second;
    const uint64_t& yID = yOperandInfo.second;

    if (xDim == 1 and yDim == 1) {
      const auto& x = std::get<ct::vector>(lookup(xID));
      const auto& y = std::get<ct::vector>(lookup(yID));

      ct::scalar tensor0D = ct::dot(x, y);
      double result = tensor0D.get();

      insert(tensorID, std::move(tensor0D));
      tensorAstNodeType temp_node(0, ctop::broadcast, result, shape);
      return {temp_node};
    } else if constexpr (std::is_same_v<tensorType, ct::vector>) {
      if (xDim == 1 and yDim == 2) {
        const auto& x = std::get<ct::vector>(lookup(xID));
        const auto& y = std::get<ct::matrix>(lookup(yID));

        ct::vector tensor = ct::dot(x, y);
        const auto& tensorNode = tensor();
        insert(tensorID, std::move(tensor));

        return tensorNode;
      } else if (xDim == 2 and yDim == 1) {
        const auto& x = std::get<ct::matrix>(lookup(xID));
        const auto& y = std::get<ct::vector>(lookup(yID));

        ct::vector tensor = ct::dot(x, y);
        const auto& tensorNode = tensor();
        insert(tensorID, std::move(tensor));

        return tensorNode;
      }
    } else if constexpr (std::is_same_v<tensorType, ct::matrix>) {
      if (xDim == 2 and yDim == 2) {
        const auto& x = std::get<ct::matrix>(lookup(xID));
        const auto& y = std::get<ct::matrix>(lookup(yID));

        ct::matrix tensor = ct::matmul(x, y);
        const auto& tensorNode = tensor();
        insert(tensorID, std::move(tensor));

        return tensorNode;
      }
    }
  } else if (ctopcode == ctop::copy) {
    uint32_t operand_size = extract<uint32_t>(cmd);
    std::pair<uint8_t, uint64_t> copyOperandInfo = getFlushedOperand<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;

    const uint64_t& copyID = copyOperandInfo.second;
    const auto& copy = std::get<tensorType>(lookup(copyID));
    tensorType tensor(copy);

    const auto& tensorNode = tensor();
    insert(tensorID, std::move(tensor));
    return tensorNode;
  }

  if(numOperands <= 2) {
    uint32_t operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> left = faster_tortoise<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;
    std::vector<tensorAstNodeType> right;
    if(numOperands==2){
      operand_size = extract<uint32_t>(cmd);
      right = faster_tortoise<tensorType, tensorAstNodeType>(cmd);
      cmd += operand_size;
    }

    rootNode.left_ = 1;
    size_t right_size;
    size_t left_size;
    if (ctopcode == ctop::unary_expr  ||
        ctopcode == ctop::logical_not ||
        ctopcode == ctop::custom_expr) {
      rootNode.right_ = -1;
      right_size = 0;
      left_size = left.size();
    } else if(ctopcode == ctop::copy){
      //assuming copy is done on non temps only
      rootNode.right_ = -1;
      left_size = 0;
      right_size = 0;
      rootNode.copy_id_ = left[0].name_;
    } else {
      rootNode.right_ = left.size() + 1;
      right_size = right.size();
      left_size = left.size();
    }

    ast.reserve(left.size() + right_size + 1);
    ast.emplace_back(rootNode);
    std::copy(left.begin(), left.end(), std::back_inserter(ast));

    if (right_size)
        std::copy(right.begin(), right.end(), std::back_inserter(ast));

    for (int i = 1; i != left.size(); ++i) {
        if (ast[i].left_ != -1) {
            ast[i].left_ += 1;
        }

        if (ast[i].right_ != -1) {
            ast[i].right_ += 1;
        }

        if (ast[i].ter_ != -1) {
            ast[i].ter_ += 1;
        }
    }

    for (int i = 1 + left.size(); i != ast.size(); ++i) {
        if (ast[i].left_ != -1)
        {
            ast[i].left_ += 1 + left.size();
        }

        if (ast[i].right_ != -1)
        {
            ast[i].right_ += 1 + left.size();
        }

        if (ast[i].ter_ != -1)
        {
            ast[i].ter_ += 1 + left.size();
        }
    }
    ckout<<"HERE "<<endl;
  } else {
    uint32_t operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> left = faster_tortoise<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;
    operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> right = faster_tortoise<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;
    operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> ter = faster_tortoise<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;

    rootNode.left_ = 1;
    rootNode.right_ = left.size() + 1;
    rootNode.ter_ = left.size() + right.size() + 1;

    ast.reserve(left.size() + right.size() + ter.size() + 1);

    ast.emplace_back(rootNode);
    std::copy(left.begin(), left.end(), std::back_inserter(ast));
    std::copy(right.begin(), right.end(), std::back_inserter(ast));
    std::copy(ter.begin(), ter.end(), std::back_inserter(ast));

    for (int i = 1; i != left.size(); ++i) {
      if (ast[i].left_ != -1)
        ast[i].left_ += 1;

      if (ast[i].right_ != -1)
        ast[i].right_ += 1;

      if (ast[i].ter_ != -1)
        ast[i].ter_ += 1;
    }

    for (int i = 1 + left.size(); i != left.size() + right.size(); ++i) {
      if (ast[i].left_ != -1)
        ast[i].left_ += 1 + left.size();

      if (ast[i].right_ != -1)
        ast[i].right_ += 1 + left.size();

      if (ast[i].ter_ != -1)
        ast[i].ter_ += 1 + left.size();
    }

    for (int i = 1 + left.size() + right.size(); i != ast.size(); ++i) {
      if (ast[i].left_ != -1)
        ast[i].left_ += 1 + left.size() + right.size();

      if (ast[i].right_ != -1)
        ast[i].right_ += 1 + left.size() + right.size();

      if (ast[i].ter_ != -1)
        ast[i].ter_ += 1 + left.size() + right.size();
    }
  }
  
  if (store or flush) {
    ckout<<"store through AST break "<<tensorID<<endl;
    tensorType tensor(ast);
    const auto& tensorNode = tensor();
    insert(tensorID, std::move(tensor));
    return tensorNode;
  }
  return ast;
}
