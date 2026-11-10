# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:35:13 2022

@author: he322
"""
import math
import numpy as np

counter = 0
pri_hit = []
tar_hit = []
overlap_hit = []
for row in data_expr.values:
    # print(row)
    counter = counter + 1
    print(counter)
    dot = [row[1], row[2]]
    if in_block_list(dot, pri_block_list):
        pri_hit.append(counter)
    if in_block_list(dot, tar_block_list):
        tar_hit.append(counter)
        if in_block_list(dot, pri_block_list):
            overlap_hit.append(counter)
    
print(in_block_list(dot, pri_block_list))
print(in_block_list(dot, tar_block_list))

p = 0.2
def budget(p):
    return np.log((1-p)/p)
def pr(bud):
    return 1/(np.exp(bud) + 1)
print(budget(p))
print(pr(budget(p)))

budgets = [0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5]
print(pr(budgets))