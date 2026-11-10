# -*- coding: utf-8 -*-

import pandas as pd
data_list = []
empty_list = [295, 1089, 1497, 1947, 2929, 2945, 3050, 3160, 3194, 3950, 5972, 6030, 6236, 6322, 6717,
              7583, 8209, 8424, 9874, 10115, 10352]

data_all = pd.read_csv('taxi\\1.txt', sep = ",", header = None)

for i in range(0, 1400):
    print(i)
    if i not in empty_list:
        data_temp = pd.read_csv('taxi\\{}.txt'.format(i), sep = ",", header = None)
        data_all = pd.concat([data_all, data_temp], ignore_index=True)
        
data_expr = data_all.iloc[:, 1:]
data_expr = data_expr[data_expr.iloc[:, 1] > 115]
data_expr = data_expr[data_expr.iloc[:, 1] < 118]
data_expr = data_expr[data_expr.iloc[:, 2] > 38]
data_expr = data_expr[data_expr.iloc[:, 2] < 41]

long_max = data_expr.iloc[:, 1].max()
long_min = data_expr.iloc[:, 1].min()
lati_max = data_expr.iloc[:, 2].max()
lati_min = data_expr.iloc[:, 2].min()