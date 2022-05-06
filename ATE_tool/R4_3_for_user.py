import sys

import R4_3
import math
import pandas as pd
import string
import numpy as np
import matplotlib.pyplot as plt
from R4_3 import Requirement4_3

"""
使用备注：
1.  有的图x轴的精确度不够， 可以取消下面的注释提高精确度
                # plt.xticks(xticks)
                
2. linux系统使用脚本前需要终端执行命令：   sudo apt-get install python3-tk

3. 运行代码之后弹出图之后， 作图工具可以实现自定义选取观察范围 - 点击放大镜，选取范围即可

### 实验数据:
    # 根据需求输入想得到的test_lot相关数据
    test_lot = "LXT2128N008-D001.002"
    
    # 实验测试项名称：
    GPIO25_data_0_DAC_Test_GPIO25_data
"""

# -------------------
# 参数设置区：
test_item_list = ["GPIO25_data_0_DAC_Test_GPIO25_data", "OS_test_N_SD_DATA_0_data"]


DEFAULT_RANGE = "CLOSER"
ALL_RANGE = "ALL"
TOGETHER = True
SEPERATE = False
# —-------------------


class Notebook_enclose(object):

    def __init__(self, test_lots: list):
        self.__test_lots = test_lots
        """用户要选择的test_lot"""
        self.__agents = {}

        # 测试项原值数据
        self.__test_item_dfs = {}

        # 测试项统计数据
        self.__raw_statics_data_dict = {}

        for i in test_lots:
            agent = Requirement4_3(i)
            # key: value ==> test_lot(str): instance of Requirement4_3 (object)
            self.__agents[i] = agent

            # 测试项原值数据
            test_item_df = pd.DataFrame(agent.get_data_for_plot())
            self.__test_item_dfs[i] = test_item_df

            # 测试项统计数据
            # example:
            # {'_id': 'Cal_crc_Result_', 'test_num': '16002', 'mean': 0.9991935997283704, 'count': 47123, 'min': -1.0,
            # 'max': 1.0, 'standard_deviation': 0.04015159102527631, 'high_limit': 1.0, 'low_limit': 1.0, 'CPK': 0.0}
            # 裝在 DataFrame 中
            raw_statics_data_df = self.__get_raw_data(agent)
            self.__raw_statics_data_dict[i] = raw_statics_data_df

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 2000)
        """通过运行R4_3的代码得到为作图准备的生数据"""

    def __get_raw_data(self, agent):
        """
        提取生数据 (统计数据)
        字段有： test_name test_num  mean  count  min  max  standard_deviation  high_limit  low_limit  CPK
        返回 df
        """
        data = agent.main_function()
        df = pd.DataFrame(data)
        df = df.rename(columns={'_id': 'test_name'})
        return df

    def __prepare_plot(self, one_test_item_name, raw_statics_data_df, test_item_df):
        """
        作图前的数据准备
        one_test_item_name： 用户输入的测试项名称 （此处为单个）
        """
        clean_df = raw_statics_data_df[(raw_statics_data_df.standard_deviation != 0)]
        # 指定测试项的统计数据
        one_test_item_statistic_data = clean_df[(clean_df.test_name == one_test_item_name)]
        one_test_item_statistic_data = one_test_item_statistic_data.reset_index(drop=True)

        # 取 指定测试项的high/low limit
        high_limit = round(one_test_item_statistic_data.loc[0, 'high_limit'], 4)
        low_limit = round(one_test_item_statistic_data.loc[0, 'low_limit'], 4)

        # 指定测试项的纯数据
        one_test_item_df = test_item_df[(test_item_df.test_name == one_test_item_name)]
        return one_test_item_df, high_limit, low_limit

    def column_chart(self, one_test_item_name, range_chosed, out_as=TOGETHER):
        """
        作柱状图 / 可看做分布图（颗粒度小）
        默认范围是 (low_limit - 0.05)---(high_limit + 0.05)

        """
        if out_as:
            plt.figure(figsize=(17, 9))
            plt.title("column chart for lots:[" + str(self.__raw_statics_data_dict.keys()) + "]")
            plt.xlabel(one_test_item_name + "  - result_value", fontsize=15)
            plt.ylabel("count_number", fontsize=15)
            high_limit, low_limit = 0, 0

            for key in self.__raw_statics_data_dict.keys():
                raw_statics_data_df = self.__raw_statics_data_dict[key]
                test_item_df = self.__test_item_dfs[key]

                one_test_item_df, high_limit, low_limit = self.__prepare_plot(one_test_item_name, raw_statics_data_df, test_item_df)

                x_values = one_test_item_df.value
                y_values = one_test_item_df.Counter
                plt.bar(x_values, y_values, width=0.001, label=key, alpha=0.8)
                # plt.plot(x_values, y_values)


                # for a, b in zip(x_values, y_values):
                #     plt.text(a, b+0.05, '%.0f' % b, ha='center', va='bottom', fontsize=11)

            # -------------------------------------
            if range_chosed == ALL_RANGE:
                pass
            else:
                plt.xlim((low_limit - 0.05), (high_limit + 0.05), 0.00015)

            # --------------------------------------
            plt.grid(True, linestyle='--', alpha=0.3)
            plt.axvline(x=low_limit, c='r', ls="--")
            plt.axvline(x=high_limit, c='r', ls="--")
            plt.legend()

            plt.show()
        else:
            plt.ion()
            high_limit, low_limit = 0, 0

            for key in self.__raw_statics_data_dict.keys():
                plt.figure(figsize=(17, 9))
                plt.title("column chart for lot: "+key)
                plt.xlabel(one_test_item_name + "  - result_value", fontsize=15)
                plt.ylabel("count_number", fontsize=15)

                raw_statics_data_df = self.__raw_statics_data_dict[key]
                test_item_df = self.__test_item_dfs[key]

                one_test_item_df, high_limit, low_limit = self.__prepare_plot(one_test_item_name, raw_statics_data_df,
                                                                              test_item_df)

                x_values = one_test_item_df.value
                y_values = one_test_item_df.Counter
                plt.bar(x_values, y_values, width=0.001, label=key)
                # plt.plot(x_values, y_values)

                # for a, b in zip(x_values, y_values):
                #     plt.text(a, b+0.05, '%.0f' % b, ha='center', va='bottom', fontsize=11)

            # -------------------------------------
            if range_chosed == ALL_RANGE:
                pass
            else:
                plt.xlim((low_limit - 0.05), (high_limit + 0.05), 0.00015)

            # --------------------------------------
            plt.grid(True, linestyle='--', alpha=0.3)
            plt.axvline(x=low_limit, c='r', ls="--")
            plt.axvline(x=high_limit, c='r', ls="--")
            plt.ioff()


            plt.show()

    def histogram(self, one_test_item_name, out_as=TOGETHER):
        """
        作频布直方图
        """

        if out_as:
            plt.figure(figsize=(20, 18), dpi=100)
            high_limit, low_limit = 0, 0
            for key in self.__raw_statics_data_dict.keys():

                raw_statics_data_df = self.__raw_statics_data_dict[key]
                test_item_df = self.__test_item_dfs[key]

                one_test_item_df, high_limit, low_limit = self.__prepare_plot(one_test_item_name, raw_statics_data_df, test_item_df)
                x_values = one_test_item_df.value
                y_values = one_test_item_df.Counter
                minmum = math.floor(min(x_values))
                maxmum = math.ceil(max(x_values))

                # 在x_value中储存着测试项的值， 在y_value中储存则测试项这个值的数量，
                # 在后面的频布直方图中，我们不用统计每个值的数量，需要一个拥有所有值的list

                x_value_list = list(x_values)
                y_value_list = list(y_values)
                hist_x_value = []

                for i in range(len(x_value_list)):
                    count_number = y_value_list[i]
                    value = x_value_list[i]
                    for j in range(count_number):
                        hist_x_value.append(value)

                xticks = list(range(10 * minmum, 10 * maxmum, 1))
                for i in range(len(xticks)):
                    xticks[i] = xticks[i] / 10


                interval = 0.02
                bins = 50
                plt.hist(hist_x_value, bins, alpha=0.3, label=key)
                #  有的图x轴的精确度不够， 可以取消下面的注释提高精确度
                # plt.xticks(xticks)
                print(key+" 此测试项" + one_test_item_name + "的统计数据:")
                print(pd.Series(hist_x_value).describe())

            plt.xlabel(one_test_item_name + "  - result_value", fontsize=15)
            plt.ylabel("count_number", fontsize=15)
            plt.title("Test-Item-Histogram for lots:[" + str(self.__raw_statics_data_dict.keys()) + "]", fontsize=20)
            plt.axvline(x=low_limit, c='r', ls="--")
            plt.axvline(x=high_limit, c='r', ls="--")
            plt.grid(True, linestyle='--', alpha=0.3)
            plt.legend()

            plt.show()
        else:
            plt.ion()
            for key in self.__raw_statics_data_dict.keys():

                raw_statics_data_df = self.__raw_statics_data_dict[key]
                test_item_df = self.__test_item_dfs[key]

                one_test_item_df, high_limit, low_limit = self.__prepare_plot(one_test_item_name, raw_statics_data_df, test_item_df)
                x_values = one_test_item_df.value
                y_values = one_test_item_df.Counter
                minmum = math.floor(min(x_values))
                maxmum = math.ceil(max(x_values))

                # 在x_value中储存着测试项的值， 在y_value中储存则测试项这个值的数量，
                # 在后面的频布直方图中，我们不用统计每个值的数量，需要一个拥有所有值的list

                x_value_list = list(x_values)
                y_value_list = list(y_values)
                hist_x_value = []

                for i in range(len(x_value_list)):
                    count_number = y_value_list[i]
                    value = x_value_list[i]
                    for j in range(count_number):
                        hist_x_value.append(value)

                xticks = list(range(10 * minmum, 10 * maxmum, 1))
                for i in range(len(xticks)):
                    xticks[i] = xticks[i] / 10

                plt.figure(figsize=(20, 18), dpi=100)
                interval = 0.02
                bins = 50
                plt.hist(hist_x_value, bins)
                #  有的图x轴的精确度不够， 可以取消下面的注释提高精确度
                # plt.xticks(xticks)
                plt.xlabel(one_test_item_name + "  - result_value", fontsize=15)
                plt.ylabel("count_number", fontsize=15)
                plt.title("Test-Item-Histogram for " + key, fontsize=20)
                plt.axvline(x=low_limit, c='r', ls="--")
                plt.axvline(x=high_limit, c='r', ls="--")
                plt.grid(True, linestyle='--', alpha=0.3)
                print(key+" 此测试项" + one_test_item_name + "的统计数据:")
                print(pd.Series(hist_x_value).describe())

            plt.ioff()
            plt.show()



    def box_figure(self, one_test_item_name, show_outlier=False, out_as=TOGETHER):

        if out_as:
            plt.figure(figsize=(9, 15))
            plt.grid(True, linestyle='--', alpha=0.3)
            plt.title("Boxplot for: " + one_test_item_name)
            data = []
            labels = []

            for key in self.__raw_statics_data_dict.keys():

                raw_statics_data_df = self.__raw_statics_data_dict[key]
                test_item_df = self.__test_item_dfs[key]

                one_test_item_df, high_limit, low_limit = self.__prepare_plot(one_test_item_name, raw_statics_data_df,
                                                                              test_item_df)
                x_values = one_test_item_df.value
                x_value_list = list(x_values)
                y_values = one_test_item_df.Counter
                y_value_list = list(y_values)

                hist_x_value = []

                for i in range(len(x_value_list)):
                    count_number = y_value_list[i]
                    value = x_value_list[i]
                    for j in range(count_number):
                        hist_x_value.append(value)
                labels.append(key)
                print(key + " 此测试项" + one_test_item_name + "的统计数据:")
                print(pd.Series(hist_x_value).describe())
                data.append(hist_x_value)

                # 如果需要显示异常值则设置  showfliers = Ture

            plt.boxplot(data, showmeans=True, sym='*', showfliers=show_outlier, labels=labels)
            # plt.xlabel("Lots")
            plt.show()
        else:
            plt.ion()
            for key in self.__raw_statics_data_dict.keys():

                raw_statics_data_df = self.__raw_statics_data_dict[key]
                test_item_df = self.__test_item_dfs[key]

                one_test_item_df, high_limit, low_limit = self.__prepare_plot(one_test_item_name, raw_statics_data_df, test_item_df)
                x_values = one_test_item_df.value
                x_value_list = list(x_values)
                y_values = one_test_item_df.Counter
                y_value_list = list(y_values)

                hist_x_value = []

                for i in range(len(x_value_list)):
                    count_number = y_value_list[i]
                    value = x_value_list[i]
                    for j in range(count_number):
                        hist_x_value.append(value)

                plt.figure(figsize=(9, 15))
                plt.title("Boxplot for: " + one_test_item_name)
                plt.xlabel("For the lot: "+key)

                # 如果需要显示异常值则设置  showfliers = Ture
                box1 = pd.Series({one_test_item_name: hist_x_value})

                labels = [one_test_item_name]
                plt.boxplot(box1, showmeans=True, sym='*', showfliers=show_outlier)

                plt.grid(True, linestyle='--', alpha=0.3)

                print(key+" 此测试项" + one_test_item_name + "的统计数据:")
                print(pd.Series(hist_x_value).describe())
            plt.ioff()
            plt.show()
    def get_name_by_no(self, test_item_df):
        """
        通过输入测试项号，查找对应的测试项名称
        以防记不住或错误输入
        """
        test_num = input("Please input 'test_num' to get the 'test_name': ")
        name = test_item_df[test_item_df.test_num == str(test_num)]['test_name'].unique()
        print("test_num: " + test_num + " ==> test_name:" + name)

    def check_noisy_data(self, raw_statics_data_df):
        """
        查看标准差为零的数据
        --不确定是代码逻辑的原因还是有其他原因
        """
        print("-------------------cpk 为 N/A ， 标准差为0的数据：")
        df = raw_statics_data_df[(raw_statics_data_df.standard_deviation == 0)]
        print(df)

# class Multi_lot(object):
#     """
#     为某一功能准备数据
#     围绕功能： 多个lot的同一测试项 在同一张画布作图对比
#     """
#     def __init__(self, test_lots: list):
#         self.__multi_lot = {}
#         for i in test_lots:
#             single_lot = Notebook_enclose(i)
#             # key: value ==> test_lot(str): instance of Requirement4_3 (object)
#             self.__multi_lot[i] = single_lot
#
#     def multi_lot(self, multi_lot):
#         """
#         此函数围绕 - 功能：多个lot的同一测试项一起作图
#         """
#

def menu(test_lot):
    # test_lot = "LXT2128N008-D001.002"
    ne = Notebook_enclose(test_lot)
    # ne.box_figure("GPIO25_data_0_DAC_Test_GPIO25_data", False)
    head = "\n\n==============================================================================\n" \
           "==============================================================================\n" \
           "                            R4_3 Dashboard                                    \n" \
           "==============================================================================\n" \
           "==============================================================================\n" \
           "(1) enter 1 --> make the column_chart\n" \
           "(2) enter 2 --> make the histogram\n" \
           "(3) enter 3 --> make the box_figure\n" \
           "(4) enter 4 --> quit the program\n" \
           "tips: you can stop the program at anywhere you input 'quit'(ignore cases)"

    user_input = ""
    while user_input.upper() != "QUIT":
        print(head)
        user_input = input("your next step: ")
        if user_input == '1':
            for test_item in test_item_list:
                print("---------------------FOR--"+test_item+"-----------------------")
                f_choice = input("show all range of data?(y/n) ")
                if f_choice == 'y':
                    s_choice = input("show multi-lot together or seperately?(t/s)")
                    if s_choice == 't':
                        ne.column_chart(test_item, range_chosed=ALL_RANGE, out_as=TOGETHER)
                    else:
                        ne.column_chart(test_item, range_chosed=ALL_RANGE, out_as=SEPERATE)
                else:
                    s_choice = input("show multi-lot together or seperately?(t/s)")
                    if s_choice == 't':
                        ne.column_chart(test_item, range_chosed=DEFAULT_RANGE, out_as=TOGETHER)
                    else:
                        ne.column_chart(test_item, range_chosed=DEFAULT_RANGE, out_as=SEPERATE)
        elif user_input == '2':
            for test_item in test_item_list:
                print("---------------------FOR--" + test_item + "-----------------------")
                f_choice = input("show multi-lot together or seperately?(t/s)")
                if f_choice == 't':
                    ne.histogram(test_item, out_as=TOGETHER)
                else:
                    ne.histogram(test_item, out_as=SEPERATE)

        elif user_input == '3':
            for test_item in test_item_list:
                print("---------------------FOR--" + test_item + "-----------------------")
                choice = input("Do you want to show the outliers? (y/n)")
                if choice == 'y':
                    choice2 = input("show multi-lot together or seperately?(t/s)")
                    if choice2 == 't':
                        ne.box_figure(test_item, show_outlier=True, out_as=TOGETHER)
                    else:
                        ne.box_figure(test_item, show_outlier=True, out_as=SEPERATE)

                else:
                    choice2 = input("show multi-lot together or seperately?(t/s)")
                    if choice2 == 't':
                        ne.box_figure(test_item, show_outlier=False, out_as=TOGETHER)
                    else:
                        ne.box_figure(test_item, show_outlier=False, out_as=SEPERATE)

        elif user_input == '4':
            sys.exit()
        else:
            print("not a correct option")


if __name__ == "__main__":
    # test_lot = ["LXT2128N008-D001.002", "LXT2128N008-D001.003"]
    # ne = Notebook_enclose(test_lot)
    # ne.box_figure("GPIO25_data_0_DAC_Test_GPIO25_data")
    # ne.histogram("GPIO25_data_0_DAC_Test_GPIO25_data")
    # ne.column_chart("GPIO25_data_0_DAC_Test_GPIO25_data", ALL_RANGE, False)

    menu(["LXT2128N008-D001.002", "LXT2128N008-D001.003"])

    # ne = notebook_enclose(test_lot)
    # ne.box_figure("GPIO25_data_0_DAC_Test_GPIO25_data", False)
