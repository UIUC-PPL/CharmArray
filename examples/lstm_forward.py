from charmnumeric.random import randn
from charmnumeric.operations import zeros, tile, tanh, exp
from charmtyles.core import plot_execution_state
import numpy as np


batch_size = 32
hidden_size = 10
sentence_length = 4
word_size = 10

X = randn(sentence_length, batch_size, hidden_size)
h0 = randn(1, hidden_size)
WLSTM = randn(
   word_size + hidden_size, 4 * hidden_size
) / np.sqrt(word_size + hidden_size)

xphpb = WLSTM.shape[0]
d = hidden_size
n = sentence_length
b = batch_size

Hin = zeros((n, b, xphpb))
Hout = zeros((n, b, d))
IFOG = zeros((n, b, d * 4))
IFOGf = zeros((n, b, d * 4))
C = zeros((n, b, d))
Ct = zeros((n, b, d))

for t in range(0, n):
    if t == 0:
       prev = tile(h0, (b, 1))
    else:
       prev = Hout[t - 1]

    Hin[t, :, :word_size] = X[t]
    Hin[t, :, word_size:] = prev
    # compute all gate activations. dots:
    IFOG[t] = Hin[t].dot(WLSTM)
    # non-linearities
    IFOGf[t, :, : 3 * d] = 1.0 / (
        1.0 + exp(-IFOG[t, :, : 3 * d])
    )  # sigmoids these are the gates
    IFOGf[t, :, 3 * d :] = tanh(IFOG[t, :, 3 * d :])  # tanh
    # compute the cell activation
    C[t] = IFOGf[t, :, :d] * IFOGf[t, :, 3 * d :]
    if t > 0:
        C[t] += IFOGf[t, :, d : 2 * d] * C[t - 1]
    Ct[t] = tanh(C[t])
    Hout[t] = IFOGf[t, :, 2 * d : 3 * d] * Ct[t]

plot_execution_state()
