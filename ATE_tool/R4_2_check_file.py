
import utilities
import mongoConnect
import pandas as pd

# 使用说明：
# 在数据库中如果有（和想象情况一致） 新添加的字段名（比如new_hbr）（自己统计后写入的字段）
# 则
# 将SBR_FIELD 的值改为 new_sbr(新字段名);  将HBR_FIELD 的值改为 new_hbr（新字段名） 即可
# 更换if __name__ == "__main__": 中的test_lot 得到对应的不同的批次的统计结果

# 此需求运行结果建议将结果导入到本地数据库中后用compass查看, 或导出为Json结构的文件 - 因为相比其他需求，此需求结构比较复杂，嵌套层次多

# 备 注：
# 如果输出为空则表示mongodb的aggregation中代码有问题， 没有返回任何值
TOP_FIVE_RECOVER_RATE = "Top_five_recover_rate"
SITE_NUM = "SITE_NUM"
FAIL_RATE = "fail_rate"

CHIPS_FOR_THIS_SITE = 'total_chips_for_this_site'
CHIPS_FOR_THIS_BIN = 'total_chips_for_this_bin'
BIN_SUMMARY = 'BIN_SUMMARY'
SITE_SUMMARY = 'SITE_SUMMARY'

CHIPS_NUM = 'chips_number'
FINAL_YIELD_RATE_SUMMARY = 'Final_yield_rate_summary'
FIRST_YIELD_RATE_SUMMARY = 'First_yield_rate_summary'
SBR_FIELD = "new_sbr"
HBR_FIELD = "new_hbr"
BIN_NUM = ""
TEST_IN_FIELD = "Test_in"


def bubble_sort(arr):
    """
    从小到大排列， 前五个最小的-->差值最大的
    """
    for i in range(1, len(arr)):
        for j in range(0, len(arr) - i):
            if arr[j][FAIL_RATE] > arr[j + 1][FAIL_RATE]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    return arr


class Requirement4_2(object):

    def __init__(self, test_lot: str):
        self.agent = mongoConnect.AteDbAgent()
        self.__writer = mongoConnect.CalDb()
        self.__test_lot = test_lot

    def __get_query(self, choice: str):
        global BIN_NUM
        # 因为sbin 和 hbin的代码几乎一样，所以下面的代码讲整合为一个query并只改变必须改变的字段名和
        #
        if choice == SBR_FIELD:
            BIN = "SBIN"

        elif choice == HBR_FIELD:
            BIN = "HBIN"

        # 原始mongodb的aggregation代码可以取一个test_lot作为例子， 自行组合下面的代码来复原原始的mongodb aggregation 查询语句，
        # 然后到Mongodb compass中预览结果

        # 下面三个变量为三个字典， sbr 和hbr有不同的字段名
        # agg_project 中的$filter 将过滤choice字段的（array）所有元素， 只返回符合条件的HEAD_NUM， 我们不用=255的数据，因为后面可以自己统计出来
        agg_project = {
            '$project': {
                choice: {
                    '$filter': {
                        'input': '$' + choice,
                        'as': 'item',
                        'cond': {
                            '$and': [
                                {
                                    '$not': {
                                        '$eq': [
                                            '$$item.HEAD_NUM', 255
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                },
                'program': '$mir.JOB_NAM',
                'filename': 1
            }
        }
        # 展开(解包)字段choice(new_sbr/new_hbr)
        agg_unwind = {
            '$unwind': {
                'path': '$' + choice
            }
        }

        agg_group = {
            # 按（S/HBIN_NUM + 芯片数量 + 提供前面数据的测试轮次组成的array） 分组
            # 后面会用到测试轮次_id，在PartData中获取Test_in
            '$group': {
                '_id': {
                    (BIN + '_NUM'): '$' + choice + '.' + BIN + '_NUM',
                    SITE_NUM: '$' + choice + '.' + SITE_NUM,
                    'if_FT': '$if_FT'
                },
                'chips_number': {
                    '$sum': '$' + choice + '.' + BIN + '_CNT'
                },
                mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                    '$addToSet': '$_id'
                }
            }
        }

        # 下面的代码query是处理不同字段（sbr或hbr）时 命令完全相同的部分
        query = [
            #  匹配搜索所有符合用户输入的test_lot
            {
                '$match': {
                    'dirname': {"$regex": ".*" + self.__test_lot + '$', "$options": "i"}
                }
            },
            # sbr 或 hbr 有不同的命令， agg_project变量将会根据情况自动对应到正确的命令
            agg_project
            , {
                # 增加 if_FT 来判断此测试轮次是否为 初测， if_FT_FR 来判断此测试轮次是否为 初测+复测（即不是抽测即可）
                # 观察规律得到（适用于所有工厂）
                '$addFields': {
                    'if_FT': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    {
                                        '$and': [
                                            utilities.regexMatch('$filename', ".*FT.*")
                                            , {
                                                '$not': utilities.regexMatch('$filename', ".*R0.*")
                                            }, {
                                                '$not': utilities.regexMatch('$filename', ".*FT1_R.*")
                                            }
                                        ]
                                    },
                                    utilities.regexMatch('$filename', ".*FT1_R0.*")
                                ]
                            },
                            'then': True,
                            'else': False
                        }
                    },
                    'if_FT_FR': {
                        '$cond': {
                            'if': {
                                '$or': [
                                    utilities.regexMatch('$filename', ".*_QA.*"),
                                    utilities.regexMatch('$filename', ".*QC.*")
                                ]
                            },
                            'then': False,
                            'else': True
                        }
                    }
                }
            }, {
                '$match': {
                    'if_FT_FR': True
                }
            }, agg_unwind
            , agg_group
            ,
            # ---------
            {
                '$group': {
                    '_id': {
                        BIN + '_NUM': '$_id.' + BIN + '_NUM',
                        'if_FT': '$_id.if_FT'
                    },
                    SITE_SUMMARY: {
                        '$addToSet': {
                            SITE_NUM: '$_id.' + SITE_NUM,
                            'chips_number': '$chips_number'
                        }
                    },
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                    }
                }
            }
            # -----------
            , {
                '$unwind': {
                    'path': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                }
            }, {
                '$unwind': {
                    'path': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                }
            },
            # ==============
            {
                '$group': {
                    '_id': '$_id',
                    SITE_SUMMARY: {
                        '$first': '$' + SITE_SUMMARY
                    },
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                    }
                }
            }
            # =============
            # -------------
            , {
                '$addFields': {
                    CHIPS_FOR_THIS_BIN: {
                        '$sum': '$' + SITE_SUMMARY + '.chips_number'
                    }
                }
            },
            # ------------
            # ------------
            {
                '$group': {
                    '_id': self.__test_lot,
                    'First_yield_rate_summary': {
                        '$addToSet': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$_id.if_FT', True
                                    ]
                                },
                                'then': {
                                    BIN + '_NUM': '$_id.' + BIN + '_NUM',
                                    SITE_SUMMARY: '$' + SITE_SUMMARY,
                                    CHIPS_FOR_THIS_BIN: '$' + CHIPS_FOR_THIS_BIN
                                },
                                'else': None
                            }
                        }
                    },
                    'Final_yield_summary': {
                        '$addToSet': {
                            BIN + '_NUM': '$_id.' + BIN + '_NUM',
                            SITE_SUMMARY: '$' + SITE_SUMMARY
                        }
                    },
            # ------------
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$_id.if_FT', True
                                    ]
                                },
                                'then': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD,
                                'else': None
                            }
                        }
                    }
                }
            }, {
                '$unwind': {
                    'path': '$Final_yield_summary'
                }
            }, {
                '$unwind': {
                    'path': '$Final_yield_summary.' + SITE_SUMMARY
                }
            },
            # ------
            {
                '$group': {
                    '_id': {
                        SITE_NUM: '$Final_yield_summary.' + SITE_NUM,
                        BIN + '_NUM': '$Final_yield_summary.' + BIN_SUMMARY + '.' + BIN + '_NUM'
                    },
                    'chips_num': {
                        '$sum': '$Final_yield_summary.' + BIN_SUMMARY + '.chips_number'
                    },
                    'First_yield_rate_summary': {
                        '$first': '$First_yield_rate_summary'
                    },
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$first': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                    },
                    'test_lot': {
                        '$first': '$_id'
                    }
                }
            }, {
                '$group': {
                    '_id': '$_id.' + SITE_NUM,
                    'First_yield_rate_summary': {
                        '$first': '$First_yield_rate_summary'
                    },
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$first': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                    },
                    BIN_SUMMARY: {
                        '$addToSet': {
                            BIN + '_NUM': '$_id.' + BIN + '_NUM',
                            CHIPS_NUM: '$chips_num'
                        }
                    },
                    'test_lot': {
                        '$first': '$test_lot'
                    }
                }
            }, {
                '$group': {
                    '_id': '$test_lot',
                    'First_yield_rate_summary': {
                        '$first': '$First_yield_rate_summary'
                    },
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$first': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                    },
                    'Final_yield_rate_summary': {
                        '$addToSet': {
                            SITE_NUM: '$_id',
                            BIN_SUMMARY: '$' + BIN_SUMMARY,
                            CHIPS_FOR_THIS_SITE: {
                                '$sum': '$' + BIN_SUMMARY + '.' + CHIPS_NUM
                            }
                        }
                    }
                }
            }, {
                '$unwind': {
                    'path': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                }
            }, {
                '$unwind': {
                    'path': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                }
            }, {
                '$group': {
                    '_id': '$_id',
                    FIRST_YIELD_RATE_SUMMARY: {
                        '$first': '$First_yield_rate_summary'
                    },
                    FINAL_YIELD_RATE_SUMMARY: {
                        '$first': '$Final_yield_rate_summary'
                    },
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': '$' + mongoConnect.TEST_ROUNDS_LIST_FIELD
                    }
                }
            }
        ]

        return query

    @staticmethod
    def get_query_for_test_in(basic_data_id):
        """
        获取query(query作用：从PartData中取在单个测试轮次, 最大的part_id, 即为Test_in)
        basic_data_id (list): basicData中document 的 _id
        返回： query 字符串
        """
        query2_for_test_in = [
            {
                '$match': {
                    'basicDataId': basic_data_id
                }
            }, {
                '$group': {
                    '_id': '$basicDataId',
                    'partId': {
                        '$max': {"$toInt": '$partId'}
                    }
                }
            }
        ]

        return query2_for_test_in

    def __get_test_in(self, temp_result: dict):
        """
        temp_result： 经过main_function() 中被mongodb aggregate query初步处理过的 结果 （以一个个字典的形式装在list中）
        向结果中添加字段 TEST_IN_FIELD
        """
        test_in = 0
        for j in temp_result[mongoConnect.TEST_ROUNDS_LIST_FIELD]:
            basic_data_id = j
            query2_for_test_in = Requirement4_2.get_query_for_test_in(j)
            part_cursor = self.agent.part.aggregate(query2_for_test_in)
            for item in part_cursor:
                test_in += int(item["partId"])

        temp_result[TEST_IN_FIELD] = test_in

    def main_function(self):
        """
        输入参数： 测试批次号 - test_lot
        返回： 需求结果
        """
        a_list = [SBR_FIELD, HBR_FIELD]
        result_list = []
        for i in a_list:
            query = self.__get_query(i)
            temp_result = self.agent.basic.aggregate(query)
            # self.agent.logger.info_list(temp_result)
            # temp_result 为只有一个元素的cursor
            for j in temp_result:
                print("-------")
                self.__get_test_in(j)
                # 计算fail_rate
                self.__get_fail_recover_rate(j)
                # 计算recover rate  j[FINAL_YIELD_RATE_SUMMARY][-1]["Yield"] = Final Yield
                # top_five_recover_rate = self.__get_top5_recover(temp_list_for_recover, j[FINAL_YIELD_RATE_SUMMARY][-1]["Yield"])
                # j[TOP_FIVE_RECOVER_RATE] = top_five_recover_rate
                j["test_lot"] = j["_id"]
                j.pop("_id")
                j["For_sbr_or_hbr"] = i

                # 整理结构格式
                for item in [FIRST_YIELD_RATE_SUMMARY, FINAL_YIELD_RATE_SUMMARY]:
                    sites_info = j[item][:-1]
                    Yield = j[item][-1]["Yield"]
                    j.pop(item)
                    j[item] = {}
                    j[item]["sites_info"] = sites_info
                    j[item]["Yield"] = Yield
                print(j)
                result_list.append(j)


        self.agent.close()
        return result_list

    def __get_fail_recover_rate(self, a_dictionary):
        """
        a_dictionary:单个字典形式的结果 - 经过main_function() 中被mongodb aggregate query初步处理过的 结果 （以一个个字典的形式装在list中）; e.g.[{},{},...]
        Test_in: Test_in 字段中的值
        本函数往数据中添加fail_rate 和 first_yield, final yield, 并返回装有每个site,bin，fail rate的list,
        后面可以把fail rate 通过get_top5_recover（）， 得到前五个recover rate
        """

        Test_in = a_dictionary[TEST_IN_FIELD]
        # Yield = 0
        a_list = [FIRST_YIELD_RATE_SUMMARY, FINAL_YIELD_RATE_SUMMARY]
        # 历遍 FIRST_YIELD_RATE_SUMMARY, FINAL_YIELD_RATE_SUMMARY
        for item in a_list:
            Yield = 0
            # 顺便先清理一下 列表中的None
            a_dictionary[item] = list(filter(None, a_dictionary[item]))
            for i in a_dictionary[item]:
                # 历遍每个site中的BIN_SUMMARY
                for j in i[BIN_SUMMARY]:
                    # 获取sbin_num = 1的 chips_number; “和”作为产出
                    if j[BIN_NUM + "_NUM"] == 1:
                        Yield += j[CHIPS_NUM]
                    # 其他sbin_num 计算fail_rate
                    else:
                        j[FAIL_RATE] = j[CHIPS_NUM] / Test_in
            # 添加产出（first_yield 和 final_yield 的字段名都用Yield表示）
            print(item + " - yield chips number: " , end='')
            print(Yield)
            a_dictionary[item].append({"Yield": Yield / Test_in})

        final_yield = a_dictionary[FINAL_YIELD_RATE_SUMMARY][-1]["Yield"]
        for item in a_list:
            # 添加recover rate
            a_dictionary[item] = list(filter(None, a_dictionary[item]))
            # a_dictionary[item]结构：{'_id': 'XX', 'First_yield_rate_summary': [{'SITE_NUM':XX, 'BIN_SUMMARY':[{},{}...]}, 'Final_yield_rate_summary':[]]
            for i in a_dictionary[item][:-1]:
                site_num = i[SITE_NUM]
                temp_list_for_recover = []
                for j in i[BIN_SUMMARY]:
                    if j[BIN_NUM + "_NUM"] == 1:
                        pass
                    else:
                        # 下面准备为后续计算recover rate的数据
                        bin_num = j[BIN_NUM + "_NUM"]
                        fail_rate = j[FAIL_RATE]
                        temp_list_for_recover.append({SITE_NUM: site_num, BIN_NUM + "_NUM": bin_num, FAIL_RATE: fail_rate})
                top_five_recover_rate = self.__get_top5_recover(temp_list_for_recover, final_yield)
                i[TOP_FIVE_RECOVER_RATE] = top_five_recover_rate




    def __get_top5_recover(self, a_list, final_yield):
        """
        a_list: list(装有多个结构为{'SBIN_NUM': , 'chips_number': , 'fail_rate': }的字典 - BIN_SUMMARY)
        final_yield: final_yield字段中的值
        处理来自get_fail_rate中的list， 得到前五个recover rate
        返回： 前五个 recover rate
        """
        result = bubble_sort(a_list)[:5]
        for i in result:
            i["recover_rate"] = final_yield - i[FAIL_RATE]
            i.pop(FAIL_RATE)
        return result

    def input_data_to_db(self, result_data: list):
        """
        写入到mongodb数据库中
        """
        # 写入前清理
        self.__writer.R4_2.drop()
        self.__writer.R4_2.insert_many(result_data)

    def export_to_csv(self, result):
        """
        将数据导出为csv - 通过pandas
        """
        df = pd.DataFrame(result)
        df.to_csv('R4_2_result_example_' + self.__test_lot + '.csv')


if __name__ == "__main__":

    # 实验数据
    # 模糊查询-不区分大小写，但必须写全
    test_lot = "LXT2128N008-D001.002"
    R4_2 = Requirement4_2(test_lot)
    data = R4_2.main_function()
    R4_2.input_data_to_db(data)

    # 若要写入数据到Cal数据库
    # 则只需取消mian_function（）中的指定注释
