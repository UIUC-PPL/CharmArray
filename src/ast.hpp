#include <vector>
#include <cstring>

template <class T>
inline T extract(char *&msg, bool increment = true)
{
  T arg = *(reinterpret_cast<T *>(msg));
  if (increment)
    msg += sizeof(T);
  return arg;
}

enum class operation : uint64_t
{
  noop = 0,
  add = 1,
  sub = 2,
  mul = 3,
  div = 4,
  matmul = 5,
  copy = 6,
  axpy = 7,
  axpy_multiplier = 8,
  sqrt = 10
};

class astnode
{
public:
  bool store;
  bool is_scalar;
  // FIXME double scalars fit into name, but should probably
  // handle this better
  uint64_t name;
  operation oper;
  std::vector<astnode *> operands;
};

operation lookup_operation(uint64_t opcode)
{
  return static_cast<operation>(opcode);
}

astnode *decode(char *cmd)
{
  uint64_t opcode = extract<uint64_t>(cmd);
  astnode *node = new astnode;
  node->oper = lookup_operation(opcode);
  if (opcode == 0)
  {
    node->is_scalar = extract<bool>(cmd);
    // if leaf is a scalar
    if (node->is_scalar)
    {
      double value = extract<double>(cmd);
      memcpy(&(node->name), &value, sizeof(double));
    }
    else
      node->name = extract<uint64_t>(cmd);
    return node;
  }

  node->is_scalar = false;
  node->name = extract<uint64_t>(cmd);
  node->store = extract<bool>(cmd);
  uint8_t num_operands = extract<uint8_t>(cmd);
  for (uint8_t i = 0; i < num_operands; i++)
  {
    uint32_t operand_size = extract<uint32_t>(cmd);
    astnode *opnode = decode(cmd);
    node->operands.push_back(opnode);
    cmd += operand_size;
  }

  return node;
}

void delete_ast(astnode *node)
{
  if (node->oper == operation::noop)
  {
    delete node;
    return;
  }

  for (astnode *n : node->operands)
    delete_ast(n);

  delete node;
}
