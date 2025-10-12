#include <charmtyles/charmtyles.hpp>
#include <stack>
#include <cinttypes>
#include <variant>
#include <queue>
#include <unordered_map>
#include <cmath>
#include <vector>
#include <cstring>

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

ct::util::Operation inline to_ctop(uint64_t opcode) noexcept {
  using ctop = ct::util::Operation;
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
    default: return ctop::noop;
  }
}

template<typename tensorType, typename tensorAstNodeType>
std::vector<tensorAstNodeType> faster_tortoise(char *cmd)
{
  uint8_t dims = extract<uint8_t>(cmd);

  if (dims == 0) {
    double value = extract<double>(cmd);
    tensorAstNodeType temp_node{0, ct::util::Operation::broadcast, value, shape};
    return {temp_node};
  }

  std::vector<uint64_t> shape; shape.reserve(2);
  for(uint8_t i = 0; i < dims; i++)
    shape.push_back(extract<uint64_t>(cmd));

  ct::util::Operation opcode = to_ctop(extract<uint32_t>(cmd));
  if (opcode == ct::util::Operation::noop) {
    const auto& tmp = std::get<1>(Server::lookup(extract<uint64_t>(cmd)));
    return tmp();
  }

  bool store  = extract<bool>(cmd);
  uint32_t tensorID = extract<uint32_t>(cmd);

  // Args for custom unops/binops
  uint32_t numArgs  = extract<uint32_t>(cmd);
  std::vector<double> args;
  for(uint32_t i = 0; i < numArgs; i++)
    args.push_back(extract<double>(cmd));

  tensorAstNodeType rootNode(opcode, shape);
  std::vector<tensorAstNodeType> ast;

  uint8_t  numOperands = extract<uint8_t>(cmd);

  if(numOperands <= 2) {
    uint32_t operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> left = faster_tortoise<tensorAstNodeType>(cmd);
    cmd += operand_size;
    operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> right = faster_tortoise<tensorAstNodeType>(cmd);
    cmd += operand_size;

    rootNode.left_ = 1;
    size_t right_size;
    if (op == ct::util::Operation::unary_expr  ||
        op == ct::util::Operation::logical_not ||
        op == ct::util::Operation::custom_expr) {
      rootNode.right_ = -1;
      right_size = 0;
    } else {
      rootNode.right_ = left.size() + 1;
      right_size = right.size();
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
  } else {
    uint32_t operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> left = faster_tortoise<tensorAstNodeType>(cmd);
    cmd += operand_size;
    operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> right = faster_tortoise<tensorAstNodeType>(cmd);
    cmd += operand_size;
    operand_size = extract<uint32_t>(cmd);
    std::vector<tensorAstNodeType> ter = faster_tortoise<tensorAstNodeType>(cmd);
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
    Server::insert(tensorID, std::move(tensor));
  }
  return ast;
}
