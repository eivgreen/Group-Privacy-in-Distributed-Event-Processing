# -*- coding: utf-8 -*-

# 1: Into Pri, 0: Stay Pri, -1: Out Pri, -2: irre
# 1: Into Tar, 0: Stay Tar, -1: Out Tar, -2: irre

counter = 0
pri_patterns_list_both = []
pri_patterns_list = []
tar_patterns_list = []
for i in range(len(data_expr)-1):
    print(i)
    row = data_expr.values[i]
    dot = [row[1], row[2]]
    if in_block_list(dot, pri_block_list):
        pri_patterns_list.append(3)
    elif in_block_list_long(dot, pri_block_list):
        pri_patterns_list.append(1)
    elif in_block_list_lati(dot, pri_block_list):
        pri_patterns_list.append(2)
    else:
        pri_patterns_list.append(0)
            
    if in_block_list(dot, tar_block_list):
        tar_patterns_list.append(1)
    else:
        tar_patterns_list.append(0)
        
    if in_block_list(dot, pri_block_list):
        pri_patterns_list_both.append(1)
    else:
        pri_patterns_list_both.append(0)
            
# for i in range(len(data_expr)-1):
#     print(i)
#     row1 = data_expr.values[i]
#     row2 = data_expr.values[i + 1]
#     dot1 = [row1[1], row1[2]]
#     dot2 = [row2[1], row2[2]]
#     if in_block_list(dot1, pri_block_list):
#         if in_block_list(dot2, pri_block_list):
#             pri_patterns_list.append(0)
#         else:
#             pri_patterns_list.append(-1)
#     else:
#         if in_block_list(dot2, pri_block_list):
#             pri_patterns_list.append(1)
#         else:
#             pri_patterns_list.append(-2)
            
#     if in_block_list(dot1, tar_block_list):
#         if in_block_list(dot2, tar_block_list):
#             tar_patterns_list.append(0)
#         else:
#             tar_patterns_list.append(-1)
#     else:
#         if in_block_list(dot2, tar_block_list):
#             tar_patterns_list.append(1)
#         else:
#             tar_patterns_list.append(-2)