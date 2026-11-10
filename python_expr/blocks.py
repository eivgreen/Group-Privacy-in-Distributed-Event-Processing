# -*- coding: utf-8 -*-

import numpy as np
from random import sample
#from load import long_min, long_max, lati_min, lati_max

x1 = long_min
x2 = long_max
y1 = lati_min
y2 = lati_max
#print(40.2-39.6)
#print(116.7-116.1)

# block basic parameters
b = 40
block_num = b**2
block_size_long = (x2 - x1) / b
block_size_lati = (y2 - y1) / b

def in_block(dot, block):
    if block[1] <= dot[1] < (block[1] + block_size_lati) and block[0] <= dot[0] < (block[0] + block_size_long):
        return True
    else:
        return False    

def in_block_long(dot, block):
    if block[0] <= dot[0] < (block[0] + block_size_long):
        return True
    else:
        return False
    
def in_block_lati(dot, block):
    if block[1] <= dot[1] < (block[1] + block_size_lati):
        return True
    else:
        return False    
    
def in_block_list(dot, block_list):
    for block in block_list:
        if in_block(dot, block):
            return True
    return False

def in_block_list_long(dot, block_list):
    for block in block_list:
        if in_block_long(dot, block):
            return True
    return False

def in_block_list_lati(dot, block_list):
    for block in block_list:
        if in_block_lati(dot, block):
            return True
    return False






            