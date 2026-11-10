# -*- coding: utf-8 -*-

from sklearn.metrics import f1_score


# score = f1_score(tar_patterns_list, tar_patterns_list_PPM, average = "macro")
# print(sum_budget)
# print(score)

# same_count = sum(a == b for a, b in zip(tar_patterns_list_PPM, tar_patterns_list))

# print(same_count/len(tar_patterns_list))

plt.plot(x, y, label="uncooperative group", color = "green", linestyle="--")
plt.ylim(0, 1)

plt.plot(x_coop, y_coop, label="cooperative group", color = "blue")
plt.ylim(0, 1)


# plt.plot(x, y_pri, label="pri_uncoop", linestyle=":", color = "green")
# plt.ylim(0, 1)

# plt.plot(x_coop, y_pri_coop, label="pri_coop", linestyle=":", color = "blue")
# plt.ylim(0, 1)

# plt.plot(x_random, y_random, label="random", color = "red")
# plt.ylim(0, 1)

# plt.plot(x_random, y_pri_random, label="pri_random_guess", color = "red")
# plt.ylim(0, 1)
# plt.plot(x, y_pri_accu, label="pri_accu_uncoop", linestyle="--", color = "green")
# plt.ylim(0, 1)

# plt.plot(x_coop, y_pri_accu_coop, label="pri_accu_coop", linestyle="--", color = "blue")
# plt.ylim(0, 1)

plt.xlabel("Privacy Budgets")
plt.ylabel("MRE")
plt.legend()


# plt.plot(x, y_pri)
# plt.ylim(0, 1)

# plt.plot(x, recall_list)
# plt.ylim(0, 1)

# plt.plot(x, precision_list)
# plt.ylim(0, 1)

# same_count = sum(a == b for a, b in zip(pri_patterns_list_both_PPM, pri_patterns_list_both))

# print(same_count/len(pri_patterns_list_both))