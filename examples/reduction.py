import charmnumeric as cnp

N = 64
x = cnp.ones((N,), dtype=cnp.float32)
res = x @ x
x = x / res

interface = cnp.CharmNumericInterface()
interface.connect('192.168.1.115', 1234, 4)
print(x.get(interface))
