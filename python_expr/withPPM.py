# -*- coding: utf-8 -*-

# 1: Into Pri, 0: Stay Pri, -1: Out Pri, -2: irre
# 1: Into Tar, 0: Stay Tar, -1: Out Tar, -2: irre
from sklearn.metrics import f1_score
from sklearn.metrics import recall_score, precision_score, accuracy_score
import matplotlib.pyplot as plt

x = []
y = []
y_pri = []
y_pri_accu = []
y_pri_recall = []
same_count_pri = []
recall_list = []
precision_list = []
accuracy_list = []
#e_list = [0.01, 0.1, 0.2, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5]
e_list = [1]
for e in e_list:
    print(e)
    n = 2
    sum_budget = e
    budget= sum_budget / n
    
    pri_patterns_list_both_PPM = []
    pri_patterns_list_PPM = []
    tar_patterns_list_PPM = []
    for i in range(len(data_expr)-1):
        #print(i)
        row = data_expr.values[i]
        
        if pri_patterns_list[i] == 1 or pri_patterns_list[i] == 2 or pri_patterns_list[i] == 3:
            noise_long = lap(sens_long, budget)
            noise_lati = lap(sens_lati, budget)
            dot = [row[1] + noise_long, row[2] + noise_lati]
        else:
            dot = [row[1], row[2]]
            
        # noise_long = lap(sens_long, budget)
        # noise_lati = lap(sens_lati, budget)
        # dot = [row[1] + noise_long, row[2] + noise_lati]
            
        if in_block_list(dot, pri_block_list):
            pri_patterns_list_PPM.append(1)
        else:
            pri_patterns_list_PPM.append(0)
                
        if in_block_list(dot, tar_block_list):
            tar_patterns_list_PPM.append(1)
        else:
            tar_patterns_list_PPM.append(0)
            
        if in_block_list(dot, pri_block_list):
            pri_patterns_list_both_PPM.append(1)
        else:
            pri_patterns_list_both_PPM.append(0)

    score = f1_score(tar_patterns_list, tar_patterns_list_PPM)
    score_pri = f1_score(pri_patterns_list_both, pri_patterns_list_both_PPM, pos_label=1)
    #score = f1_score(tar_patterns_list, tar_patterns_list_PPM, average = "macro")
    
    recall_list = recall_score(pri_patterns_list_both, pri_patterns_list_both_PPM, pos_label = 1)
    precision_list = precision_score(pri_patterns_list_both, pri_patterns_list_both_PPM, pos_label=1)
    accuracy_list = accuracy_score(pri_patterns_list_both, pri_patterns_list_both_PPM)
    
    x.append(sum_budget)
    y.append(1 - score)
    y_pri.append(score_pri)
    y_pri_accu.append(accuracy_list)
    #y_pri_recall.append(1 - recall_list)
    #y_pri_accu.append(accuracy_list)
y_overlapping_rate_uncoop.append(y)
# for e in range(1, 10):
#     print(e)
    
#     dim = 2
#     n = 2
#     sum_budget = e/100
#     budget= sum_budget
    
#     counter = 0
#     pri_patterns_list_PPM = []
#     tar_patterns_list_PPM = []
#     for i in range(len(data_expr)-1):
#         #print(i)
#         row1 = data_expr.values[i]
#         row2 = data_expr.values[i + 1]
#         if pri_patterns_list[i] == 1:
#             dot1 = [row1[1], row1[2]]
#             noise_long, noise_lati = lap(sens_long, sens_lati, budget, budget)
#             dot2 = [row2[1] + noise_long, row2[2] + noise_lati]
#         elif pri_patterns_list[i] == 0:
#             noise_long, noise_lati = lap(sens_long, sens_lati, budget, budget)
#             dot1 = [row1[1] + noise_long, row1[2] + noise_lati]
#             noise_long, noise_lati = lap(sens_long, sens_lati, budget, budget)
#             dot2 = [row2[1] + noise_long, row2[2] + noise_lati]
#         elif pri_patterns_list[i] == -1: 
#             noise_long, noise_lati = lap(sens_long, sens_lati, budget, budget)
#             dot1 = [row1[1] + noise_long, row1[2] + noise_lati]
#             dot2 = [row2[1], row2[2]]
#         else:
#             dot1 = [row1[1], row1[2]]
#             dot2 = [row2[1], row2[2]]
#         if in_block_list(dot1, pri_block_list):
#             if in_block_list(dot2, pri_block_list):
#                 pri_patterns_list_PPM.append(0)
#             else:
#                 pri_patterns_list_PPM.append(-1)
#         else:
#             if in_block_list(dot2, pri_block_list):
#                 pri_patterns_list_PPM.append(1)
#             else:
#                 pri_patterns_list_PPM.append(-2)
                
#         if in_block_list(dot1, tar_block_list):
#             if in_block_list(dot2, tar_block_list):
#                 tar_patterns_list_PPM.append(0)
#             else:
#                 tar_patterns_list_PPM.append(-1)
#         else:
#             if in_block_list(dot2, tar_block_list):
#                 tar_patterns_list_PPM.append(1)
#             else:
#                 tar_patterns_list_PPM.append(-2)
#     #score = f1_score(tar_patterns_list, tar_patterns_list_PPM, average = "macro")
#     score = f1_score(tar_patterns_list, tar_patterns_list_PPM, average = "macro")
#     x.append(sum_budget)
#     y.append(1 - score)