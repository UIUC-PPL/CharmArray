import charmnumeric as cnp
from charmtyles.core import plot_execution_state
import numpy as np


batch_size = 32
hidden_size = 10
sentence_length = 4
word_size = 10

X = cnp.random.randn(sentence_length, batch_size, hidden_size)
h0 = cnp.random.randn(1, hidden_size)
WLSTM = cnp.random.randn(
    word_size + hidden_size, 4 * hidden_size
) / np.sqrt(word_size + hidden_size)

xphpb = WLSTM.shape[0]
d = hidden_size
n = sentence_length
b = batch_size

Hin = cnp.zeros((n, b, xphpb), dtype=cnp.float32)
Hout = cnp.zeros((n, b, d), dtype=cnp.float32)
IFOG = cnp.zeros((n, b, d * 4), dtype=cnp.float32)
IFOGf = cnp.zeros((n, b, d * 4), dtype=cnp.float32)
C = cnp.zeros((n, b, d), dtype=cnp.float32)
Ct = cnp.zeros((n, b, d), dtype=cnp.float32)

for t in range(n):
    if t == 0:
        prev = cnp.tile(h0, (b, 1))
    else:
        prev = Hout[t - 1]

    Hin[t, :, :word_size] = X[t]
    Hin[t, :, word_size:] = prev
    # compute all gate activations. dots:
    IFOG[t] = Hin[t].dot(WLSTM)
    # non-linearities
    IFOGf[t, :, : 3 * d] = 1.0 / (
        1.0 + cnp.exp(-IFOG[t, :, : 3 * d])
    )  # sigmoids these are the gates
    IFOGf[t, :, 3 * d :] = cnp.tanh(IFOG[t, :, 3 * d :])  # tanh
    # compute the cell activation
    C[t] = IFOGf[t, :, :d] * IFOGf[t, :, 3 * d :]
    if t > 0:
        C[t] += IFOGf[t, :, d : 2 * d] * C[t - 1]
    Ct[t] = cnp.tanh(C[t])
    Hout[t] = IFOGf[t, :, 2 * d : 3 * d] * Ct[t]

plot_execution_state()
