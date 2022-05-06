from datetime import datetime
import mongoConnect
from R4_3 import Requirement4_3
import utilities
import R4_1
import pandas as pd


TEST_LOT_FIELD = "test_lot"


class Requirement5(object):
    """
    本类的目标：
    完成以下任务的准备工作（数据提取），配合ipython 作图
    1. 由于可能数据库中还没有FF/SS的数据，先随意找两个lot 作为假定的实验数据，把图作到指定lot的图中，用来对比
    2. 选定时间， 用大量lot的数据， 做出每个测试项的mean, cpk, first/final loss的值的变化曲线
    3. 忽略 ppt中的第三条
    """

    def __init__(self, start_datetime: str, end_datetime: str, filter_factory=utilities.DEFAULT_FILTER,
                 filter_mpn=utilities.DEFAULT_FILTER, filter_tester_no=utilities.DEFAULT_FILTER):
        self.__helper = None
        """
        helper 是 R4_3中的实例对象， （可以获得一个lot中的各个测试项的统计信息, 结构如下），复用代码，帮助完成需求5
        {'_id': 'Func_Dig_Test_', 'test_num': '17006', 'mean': 0.012336488354780389, 'count': 47015, 'min': 0.0, 'max': 15.0, 'standard_deviation': 0.4113885730677409, 'high_limit': 15.0, 'low_limit': 0.0, 'CPK': 6.046530232562536}
        """
        self.__agent = mongoConnect.AteDbAgent()
        """与数据库的操作全部交给agent"""
        self.__writer = mongoConnect.CalDb()
        """写入数据到某个数据库中的操作全部交给writer"""

        self.__R4_1 = R4_1.Requirement4_1(start_datetime, end_datetime, filter_factory, filter_mpn, filter_tester_no)
        self.__start_datetime = start_datetime
        self.__end_datetime = end_datetime
        # 默认输入字符串"ALL" - 全部工厂(不过滤)
        self.filter_factory = filter_factory
        # 默认输入字符串“ALL” - 全部mpn（不过滤）
        self.filter_mpn = filter_mpn
        self.filter_tester_no = filter_tester_no

    def __get_helper(self, a_test_lot: str):
        self.__helper = Requirement4_3(a_test_lot)

    def main_function(self):
        """
        此函数负责协助完成 目标2中的mean/cpk部分  -- 准备作图相关数据
        """
        data = self.__get_test_lot(self.__start_datetime, self.__end_datetime)
        # 已得到指定时间内的测试批次名
        # test_lot_list = []
        raw_data = []
        for i in data:
            # test_lot_list.append(i[mongoConnect.TEST_LOT_FIELD])
            self.__get_helper(i[TEST_LOT_FIELD])
            # 处理一个测试批次 平均需要8秒左右， 这里批量处理需要的时间很长
            temp = list(self.__helper.main_function(False))
            raw_data += temp
            print(datetime.now(), "-------finish one-----")

        return raw_data

    def __get_test_lot(self, start_time_min, start_time_max):
        """
        返回： 会得到指定时间内的lot/测试批次 --（dirname中最后面的测试批次可以作为lot的唯一标识）
        逻辑： 根据时间过滤 --> 再根据dirname 分组得到 lot --> 从路径名中的到test_lot(测试批号) --> 根据测试批号分组
        """
        dir_list = utilities.get_lot(start_time_min, start_time_max)
        query1 = [
            {
                '$match': {
                    "dirname": {"$in": dir_list}
                }
            },
            utilities.customized_filter(self.filter_factory, self.filter_mpn, self.filter_tester_no),
            {
                '$group': {
                    '_id': '$dirname',
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': '$_id'
                    }
                }
            },
            utilities.get_test_lot_from_dirname("_id")
        ]

        group_test_lot = [
            {
                "$group": {
                    "_id": "$" + TEST_LOT_FIELD
                }
            },
            {
                "$project": {
                    TEST_LOT_FIELD: "$_id",
                    "_id": 0
                }
            }

        ]

        data = self.__agent.basic.aggregate(query1 + group_test_lot)
        print("----------- __get_test_lot() finished-----------")
        return data

    def __get_query(self):
        """
        此函数和R4_1中的同名函数逻辑相同， 只有开头的"过滤"逻辑不同
        根据__get_test_lot_name中得到的lot 过滤BasicData中的测试轮次， 即只处理这些lot
        query1中所有常数变量 引用自 R4_1 包
        """
        # target_field = "mir.START_T"
        # time_format = "%Y-%m-%d %H:%M:%S"
        # tmin = datetime.strptime(self.start_time, time_format)
        # utc_min = int(tmin.timestamp())
        #
        # tmax = datetime.strptime(self.end_time, time_format)
        # utc_max = int(tmax.timestamp())
        #
        # query = [
        #     {'$match': {
        #         "$and": [{target_field: {"$gte": utc_min}},
        #                  {target_field: {"$lte": utc_max}}]
        #     }},
        #     {
        #         '$group': {
        #             '_id': 'dirname_list',
        #             "dir_list": {
        #                 '$addToSet': '$dirname'
        #             }
        #         }
        #     }
        # ]
        #
        # temp_data = self.__agent.basic.aggregate(query)
        # dir_list = []
        # for i in temp_data:
        #     dir_list = i["dir_list"]

        dir_list = utilities.get_lot(self.__start_datetime, self.__end_datetime)

        query1 = [
            {
                '$match': {
                    "dirname": {"$in": dir_list}
                }
            },
            utilities.customized_filter(self.filter_factory, self.filter_mpn, self.filter_tester_no),
            # # 为不存在mrr的数据添加自己计算的 FINISH_T 的逻辑
            utilities.set_finish_time()
            ,
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
            utilities.add_bin1_chips()
            ,
            utilities.add_if_class_2()
            ,
            utilities.add_chips_num_for_class2()
            ,
            utilities.add_if_ft_rt()
            ,
            utilities.add_if_ft()
            , {
                '$group': {
                    '_id': {
                        'lot': '$dirname',
                        R4_1.FACTORY_FIELD: {
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
                    R4_1.TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': {
                            'test_round_id': '$_id',
                            'filename': '$filename',
                            R4_1.TEST_ROUND_START_TIME_FIELD: "$mir.START_T",
                            R4_1.TEST_ROUND_FINISH_TIME_FIELD: "$mrr.FINISH_T"
                        }
                    },
                    R4_1.TEST_OUT_FIELD: {
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
                    R4_1.SBIN_CNT_FOR_CLASS_2_FIELD: {
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
                    R4_1.FT_BIN1_FIELD: {
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

    def get_yield_data(self):
        """
        为R5.ipynb 提供生数据
        注： 消耗时间的步骤为__get_query() & __R4_1.add_other_infor(query)
        """
        query = self.__get_query()
        temp_result = self.__R4_1.add_other_infor(query)
        result = []

        print("result number: ", len(list(temp_result)))
        for i in temp_result:

            # 从 dirname 中 取test_lot
            test_lot = i["_id"]["lot"].split("/")[-1].upper()

            result.append(
                {"test_lot": test_lot, 'First_yield(%)': i['First_yield(%)'], "Final_yield(%)": i["Final_yield(%)"]})

        # 'First_yield(%)': 0.8687642690319364, 'Final_yield(%)': 0.9521610202849871
        # '_id': {'lot': '/home/data/unisem/FT_data/ESP32-D0WDQ6-V3-ATUE00_PAH471.00-MTK#E_ESP2126019DQ000'

        return result

    def close_db(self):
        self.__agent.close()

    def input_data_to_db(self, result_data: list):
        """
        写入到mongodb数据库中
        """
        # 写入前清理
        self.__writer.R5.drop()
        self.__writer.R5.insert_many(result_data)

    def export_to_csv(self, result):
        """
        将数据导出为csv - 通过pandas
        """
        df = pd.DataFrame(result)
        df.to_csv('R5_result_example_' + str(self.__start_datetime.replace(":", "_")) + '_' + str(self.__end_datetime.replace(":", "_")) + '.csv')


if __name__ == "__main__":
    # R5 = Requirement5("2021-08-12 00:00:00", "2021-8-15 23:59:59")
    # # 1628697600
    # # 1629043199
    # data = R5.get_yield_data()
    # for i in data:
    #     print(i)
    # R5.close_db()

    R5 = Requirement5("2021-08-12 00:00:00", "2021-8-15 23:59:59", filter_factory=["HT", "UM"])
    # 下面两个函数选择一个运行：----------
    data = R5.main_function()
    # data = R5.get_yield_data()
    # -------------------------------

    for i in data:
        print(i)
    R5.close_db()


