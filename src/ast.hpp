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
using ct_array_t = std::variant<ct::scalar, ct::vector, ct::matrix, double>;
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

ctop inline to_ctop(uint64_t opcode) noexcept {
  switch (opcode) {
    case 0:  return ctop::noop;
    case 1:  return ctop::add;
    case 2:  return ctop::sub;
    case 3:  return ctop::multiply;
    case 4:  return ctop::divide;
    case 11: return ctop::greater;
    case 12: return ctop::lesser;
    case 13: return ctop::geq;
    case 14: return ctop::leq;
    case 15: return ctop::eq;
    case 16: return ctop::neq;
    case 17: return ctop::logical_and;
    case 18: return ctop::logical_or;
    case 19: return ctop::logical_not;
    case 20: return ctop::where;
    case 23: return ctop::unary_expr;
    default: return ctop::noop;
  }
}

std::shared_ptr<ct::unary_operator> to_ct_unary(uint64_t opcode, const std::vector<double>& args) noexcept {
  switch(opcode) {
    case 23: return ct::unary_ops::abs(args);
    default: return nullptr;
  }
}

template<typename tensorType, typename tensorAstNodeType>
std::vector<tensorAstNodeType> faster_tortoise(char *cmd)
{
  uint8_t dims = extract<uint8_t>(cmd);
  ckout << "DIMS> " << dims << endl;

  std::vector<uint64_t> shape; shape.reserve(2);

  if (dims == 0) {
    if constexpr (std::is_same_v<tensorType, ct::vector>) {
      shape.push_back(extract<uint64_t>(cmd));
    } else if constexpr (std::is_same_v<tensorType, ct::matrix>) {
      shape.push_back(extract<uint64_t>(cmd));
      shape.push_back(extract<uint64_t>(cmd));
    }
    double value = extract<double>(cmd);
    ckout << "VAL> " << value << endl;
    tensorAstNodeType temp_node(0, ctop::broadcast, value, shape);
    return {temp_node};
  }

  for(uint8_t i = 0; i < dims; i++)
    shape.push_back(extract<uint64_t>(cmd));
  ckout << "SHAPE> " << shape[0] << endl;

  uint32_t opcode = extract<uint32_t>(cmd);
  bool store  = extract<bool>(cmd);
  uint64_t tensorID = extract<uint64_t>(cmd);
  ckout << "TENSORID> " << tensorID << endl;

  if (opcode == 0) {
    ckout << "NO-OP" << endl;
    const auto& tmp = std::get<tensorType>(lookup(tensorID));
    return tmp();
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
  } else {
    rootNode = tensorAstNodeType(ctopcode, shape);
  }
  std::vector<tensorAstNodeType> ast;

  uint8_t  numOperands = extract<uint8_t>(cmd);
  ckout << "NUM OPERANDS> " << numOperands << endl;

  if(numOperands <= 2) {
    uint32_t operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> left = faster_tortoise<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;
    operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> right = faster_tortoise<tensorType, tensorAstNodeType>(cmd);
    cmd += operand_size;

    rootNode.left_ = 1;
    size_t right_size;
    if (ctopcode == ctop::unary_expr  ||
        ctopcode == ctop::logical_not ||
        ctopcode == ctop::custom_expr) {
      rootNode.right_ = -1;
      right_size = 0;
    } else {
      rootNode.right_ = left.size() + 1;
      right_size = right.size();
    }
    ckout << "HEREH" << endl;
    ast.reserve(left.size() + right_size + 1);
    ast.emplace_back(rootNode);
    std::copy(left.begin(), left.end(), std::back_inserter(ast));
    ckout << "THERE" << endl;

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

  if (store) {
    tensorType tensor(ast);
    const auto& tensorNode = tensor();
    insert(tensorID, std::move(tensor));
    return tensorNode;
  }
  return ast;
}
