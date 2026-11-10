# -*- coding: utf-8 -*-

import numpy as np

# budget = 0.01

sens_long = block_size_long
sens_lati = block_size_lati

def lap(sensitivity, budget):
    scale = sensitivity /budget
    noise = np.random.laplace(0, scale)
    return noise


# noise_long = lap(sens_long, budget)
# noise_lati = lap(sens_lati, budget)

# print(noise_long, noise_lati)