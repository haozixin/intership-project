from datetime import datetime
import mongoConnect
import utilities
import pandas as pd
FACTORY_FIELD = "OSAT(factory)"
TEST_ROUNDS_LIST_FIELD = 'test_rounds_list'
TEST_ROUND_START_TIME_FIELD = "start_time"
TEST_ROUND_FINISH_TIME_FIELD = "finish_time"
TEST_OUT_FIELD = 'Test_out'
SBIN_CNT_FOR_CLASS_2_FIELD = 'class_2_sbin_cnt'
FT_BIN1_FIELD = 'ft_bin1'
AVERAGE_BIN1_TEST_TIME_FIELD = 'Test_T(s)'
TEST_IN_FIELD = "Test_in"


class Requirement4_1(object):
    """
    此类负责完成需求4.1
    """

    def __init__(self, start_datetime: str, end_datetime: str, filter_factory=utilities.DEFAULT_FILTER,
                 filter_mpn=utilities.DEFAULT_FILTER, filter_tester_no=utilities.DEFAULT_FILTER):
        """
        初始化Requirement1
        """
        self.__agent = mongoConnect.AteDbAgent()
        """提取数据通过已经设置好的__agent"""
        self.__writer = mongoConnect.CalDb()
        """写入有写入权限的数据库通过writer"""
        self.__start_datetime = start_datetime
        self.__end_datetime = end_datetime
        # 默认输入字符串"ALL" - 全部工厂(不过滤)
        self.filter_factory = filter_factory
        # 默认输入字符串“ALL” - 全部mpn（不过滤）
        self.filter_mpn = filter_mpn
        self.filter_tester_no = filter_tester_no

    def main_function(self):
        query1 = self.__get_query(self.__start_datetime, self.__end_datetime)

        cursor = self.add_other_infor(query1)

        self.__close_db()
        return cursor

    def add_other_infor(self, query1):
        result_list = list(self.__agent.basic.aggregate(query1))
        for i in result_list:
            self.__agent.get_idle_time(i)
            # print(i)
            # 初测bin1芯片平均测试时间
            tr_test_t = 0
            ft_num = 0
            sample_test_round_id = i[TEST_ROUNDS_LIST_FIELD][0]["test_round_id"]
            sample_test_round = mongoConnect.AteLotInfo(self.__agent.basic.find_one({"_id": sample_test_round_id}))
            i['_id']["Device"] = sample_test_round.filenameInfo.mpn
            test_in = 0
            # 测试伦次(内有重复轮次，因为把sbr数组展开过/解包过, 对 取bin1 和 二级品 的数量不影响，但对 根据轮次id 去partData中取数据的情况会出现重复取的错误)
            already_used = []
            for j in i[TEST_ROUNDS_LIST_FIELD]:

                basic_data_id = j["test_round_id"]
                test_round = mongoConnect.AteLotInfo(self.__agent.basic.find_one({"_id": basic_data_id}))
                # Test_in
                # 如果是初测
                if test_round.filenameInfo.is_ft:

                    ft_num += 1
                    if basic_data_id not in already_used:
                        # 如果是初测， 取partId, 并把这个测试轮次的id放入list， 确保不重复取partId
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
                        part_cursor = self.__agent.part.aggregate(query2_for_test_in)
                        # print(part_cursor)
                        for item in part_cursor:
                            # print(item)
                            test_in += int(item["partId"])
                        already_used.append(j["test_round_id"])
                    else:
                        pass

                    # ------------------------------------------------------
                    # 初测中，bin1芯片的平均测试时间
                    query2_for_test_t = [
                        {
                            '$match': {
                                'softBin': 1,
                                'basicDataId': basic_data_id
                            }
                        }, {
                            '$group': {
                                '_id': '$basicDataId',
                                AVERAGE_BIN1_TEST_TIME_FIELD: {
                                    '$avg': '$testT'
                                }
                            }
                        }
                    ]
                    test_t_cursor = self.__agent.part.aggregate(query2_for_test_t)

                    for item in test_t_cursor:
                        if basic_data_id == item["_id"]:
                            tr_test_t += item[AVERAGE_BIN1_TEST_TIME_FIELD]

                    # ------------------------------------------------------------------

            # Test_in 如果是0， 可能是因为时间上没包括全初测轮次
            # 可以忽略（视为不在指定范围内）
            i[TEST_IN_FIELD] = test_in

            try:
                i[AVERAGE_BIN1_TEST_TIME_FIELD] = tr_test_t / ft_num / 1000
            except ZeroDivisionError:
                i[AVERAGE_BIN1_TEST_TIME_FIELD] = 0
                self.__agent.logger.error("检测到初测数量等于0 (可能因为filename不符合命名规则导致判断不出 或 大概率初测轮次不在指定时间内) - 可忽略此数据;  lot  ==> " + i["_id"]["lot"])
            try:
                i["First_yield(%)"] = (i[FT_BIN1_FIELD] + i[SBIN_CNT_FOR_CLASS_2_FIELD]) / i[TEST_IN_FIELD]
            except ZeroDivisionError:
                i["First_yield(%)"] = "N/A"
                self.__agent.logger.error("Test_in字段为0 (可能因为前一步骤检测不出初测轮次);  lot  ==> " + i["_id"]["lot"])
            try:
                i["Final_yield(%)"] = i[TEST_OUT_FIELD] / i[TEST_IN_FIELD]
            except ZeroDivisionError:
                i["Final_yield(%)"] = "N/A"

            # i.pop(TEST_ROUNDS_LIST_FIELD)
            # print(i)
        return result_list

    def __get_query(self, start_time, end_time):
        time_format = "%Y-%m-%d %H:%M:%S"
        tmin = datetime.strptime(start_time, time_format)
        utc_start = int(tmin.timestamp())
        tmax = datetime.strptime(end_time, time_format)
        utc_end = int(tmax.timestamp())

        print(utc_start, utc_end)

        query1 = [
            # # 为不存在mrr的数据添加自己计算的 FINISH_T 的逻辑
            utilities.set_finish_time()
            ,
            {
                '$match': {
                    '$and': [
                        {
                            'mir.START_T': {
                                '$gte': utc_start
                            }
                        }, {
                            'mrr.FINISH_T': {
                                '$lte': utc_end
                            }
                        }
                    ]
                }
            },
            utilities.customized_filter(self.filter_factory, self.filter_mpn, self.filter_tester_no),
            {
                '$project': {
                    'filename': 1,
                    'sbr': '$new_sbr',
                    'dirname': 1,
                    'mir': 1,
                    'mrr': 1
                }
            },
            {
                '$unwind': {
                    'path': '$sbr'
                }
            },
            utilities.add_if_class_2()
            ,
            utilities.add_bin1_chips()
            ,
            utilities.add_chips_num_for_class2()
            ,
            utilities.add_if_ft_rt(),
            utilities.add_if_ft()
            ,
            {
                '$group': {
                    '_id': {
                        'lot': '$dirname',
                        FACTORY_FIELD: {
                            '$toUpper': {
                                '$arrayElemAt': [
                                    {
                                        '$split': [
                                            '$dirname', '/'
                                        ]
                                    }, 3
                                ]
                            }
                        },
                        'Device': '待定'
                    },
                    TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': {
                            'test_round_id': '$_id',
                            'filename': '$filename',
                            TEST_ROUND_START_TIME_FIELD: "$mir.START_T",
                            TEST_ROUND_FINISH_TIME_FIELD: "$mrr.FINISH_T"
                        }
                    },
                    TEST_OUT_FIELD: {
                        '$sum': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$if_FT_FR', True
                                    ]
                                },
                                'then': '$bin1',
                                'else': 0
                            }
                        }
                    },
                    SBIN_CNT_FOR_CLASS_2_FIELD: {
                        '$sum': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$if_level_2', True
                                    ]
                                },
                                'then': '$sbin_cnt_for_class_2',
                                'else': 0
                            }
                        }
                    },
                    FT_BIN1_FIELD: {
                        '$sum': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$if_FT', True
                                    ]
                                },
                                'then': '$bin1',
                                'else': 0
                            }
                        }
                    }
                }
            }
        ]
        return query1

    def __close_db(self):
        self.__agent.close()

    def input_data_to_db(self, result_data: list):
        """
        写入到mongodb数据库中
        """
        # 写入前清理
        self.__writer.R4_1.drop()
        self.__writer.R4_1.insert_many(result_data)

    def export_to_csv(self, result):
        """
        将数据导出为csv - 通过pandas
        """
        df = pd.DataFrame(result)
        df.to_csv('R4_1_result_example_' + str(self.__start_datetime.replace(":", "_")) + '_' + str(self.__end_datetime.replace(":", "_")) + '.csv')


if __name__ == "__main__":
    R5 = Requirement4_1("2021-08-12 00:00:00", "2021-8-15 23:59:59", filter_factory=["HT", "UM"], filter_mpn=["ESP8266EX"])
    data = R5.main_function()
    for i in data:
        print(i)

