import re
import utilities
from pymongo import MongoClient
from bson.objectid import ObjectId
import mongoConnect
from mongoConnect import Logger

COUNTER = 'Counter'

VALUE = "value"

STANDARD_DEVIATION = 'standard_deviation'

MEAN = 'mean'

TEST_NUM = 'test_num'

LOW_LIMIT_FIELD = "low_limit"

HIGH_LIMIT_FIELD = "high_limit"

TEST_NAME = "test_name"

"""
存在问题： res 中的测试项可能少（原始数据就少）
"""


class Requirement4_3(object):
    """
    此类完成需求4.3

    对外接口函数:  main_function() 和 get_data_for_plot()
    使用方法:
     只需更换 创建对象时所需要的test_lot参数， 则可对不同lot进行需求4.3的处理，
    (tips: 单个lot的所有芯片的所有测试项，数量高达百万个，不可以简单的全部提取 )
    """

    def __init__(self, a_test_lot: str):
        self.__agent = mongoConnect.AteDbAgent()
        self.__writer = mongoConnect.CalDb()
        self.__if_only_bin1 = False
        """
        if_only_bin1 = False 则 处理所有bin的芯片 的相关测试项信息
        if_only_bin1 = True 则 只处理bin1 芯片的相关测试项信息
        """
        self.__test_lot = a_test_lot
        """通过参数输入不同的测试批号锁定不同lot"""
        self.__test_rounds_id = self.__get_test_rounds_id()
        """这个lot的初测+复测在数据库中对应的_id"""

    def __set_if_only_bin1(self, if_bin1):
        """
        False 则 处理所有bin的芯片 的相关测试项信息
        True 则 只处理bin1 芯片的相关测试项信息
        必须先用此函数设置（默认不设置 则为False），再运行main_function()的到数据
        """
        self.__if_only_bin1 = if_bin1

    def __get_test_rounds_id(self):
        """
        此函数将返回 已指定lot的所有初测  + 复测轮侧的id
        为私有属性__test_rounds_id 赋值
        """
        query = [
            {
                '$match': {
                    'dirname': {"$regex": ".*" + self.__test_lot + '$', "$options": "i"}
                }
            }, {
                '$addFields': {
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
            }, {
                '$group': {
                    '_id': self.__test_lot,
                    mongoConnect.TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': '$_id'
                    }
                }
            }
        ]
        result = self.__agent.basic.aggregate(query)
        test_rounds = []
        for i in result:
            test_rounds = i[mongoConnect.TEST_ROUNDS_LIST_FIELD]

        # 取消下面注释可查看打印
        # self.agent.logger.info_list(result)
        return test_rounds

    def __get_statistic_data(self):
        """
        注： 测试项 000 的被过滤掉了
        """
        if self.__if_only_bin1:
            match_query = {
                '$match': {
                    'basicDataId': {
                        '$in': self.__test_rounds_id
                    },
                    'hardBin': 1
                }
            }
        else:
            match_query = {
                '$match': {
                    'basicDataId': {
                        '$in': self.__test_rounds_id
                    }
                }
            }

        query = [
            match_query
            , {
                '$project': {
                    'basicDataId': 1,
                    'hardBin': 1,
                    'res': 1
                }
            }, {
                '$unwind': {
                    'path': '$res'
                }
            },
            {
                '$match': {
                    'res.num': {
                        '$ne': '000'
                    }
                }
            },
            {
                '$group': {
                    '_id': '$res.txt'
                    ,
                    TEST_NUM: {
                        '$max': '$res.num'
                    },
                    MEAN: {
                        '$avg': '$res.res'
                    },
                    'count': {
                        '$sum': 1
                    },
                    'min': {
                        '$min': '$res.res'
                    },
                    'max': {
                        '$max': '$res.res'
                    },
                    STANDARD_DEVIATION: {
                        '$stdDevPop': '$res.res'
                    }
                }
            }
        ]

        data = self.__agent.part.aggregate(query)
        # self.agent.logger.info_list(list(data)[:10])
        # 得到信息结构如下
        # 16: 39:38 - [INFO]
        # {'_id': {'test_name': 'Func_Dig_Test_'}, 'test_num': '17006', 'mean': 0.012336488354780389, 'count': 47015,
        #  'min': 0.0, 'max': 15.0, 'sd': 0.4113885730677409}
        # 16: 39:38 - [INFO]
        # {'_id': {'test_name': 'OS_test_N_MTDI_data'}, 'test_num': '13007', 'mean': -0.33926207324678476,
        # 'count': 49707, 'min': -1.7104339599609375, 'max': -0.0002288818359375, 'sd': 0.015511161193844923}
        # 16: 39:38 - [INFO]
        # {'_id': {'test_name': 'OS_test_N_U0RXD_data'}, 'test_num': '13012', 'mean': -0.34797872902149957,
        #  'count': 49707, 'min': -1.7328643798828125, 'max': -0.2739715576171875, 'sd': 0.06805192082326102}

        return list(data)

    def main_function(self, only_bin1=False):
        """
        输入参数：
            测试批次号 - test_lot;
            only_bin1: 是否只取bin1数据--参考 —— __set_if_only_bin1()
        返回： 需求中，除cpk以外的其他数据（已统计好）
        """
        self.__set_if_only_bin1(only_bin1)
        raw_data = self.__get_statistic_data()
        high_limit_list, low_limit_list = self.__get_limit()

        for r in raw_data:
            raw_data_test_name = r["_id"]
            # 为单个测试项添加 high_limit
            for h in high_limit_list:
                test_name = h["txt"]
                high_limit = h["res"]
                if raw_data_test_name == test_name:
                    r[HIGH_LIMIT_FIELD] = high_limit

            for l in low_limit_list:
                # 为单个测试项添加 low_limit
                test_name = l["txt"]
                low_limit = l["res"]
                if raw_data_test_name == test_name:
                    r[LOW_LIMIT_FIELD] = low_limit
        self.__calculate_cpk(raw_data)
        # self.agent.logger.info_list(raw_data)
        return raw_data





    def get_data_for_plot(self):
        """
        为iphython 中作图(plt.bar)准备 raw data
        """
        if self.__if_only_bin1:
            match_query = {
                '$match': {
                    'basicDataId': {
                        '$in': self.__test_rounds_id
                    },
                    'hardBin': 1
                }
            }
        else:
            match_query = {
                '$match': {
                    'basicDataId': {
                        '$in': self.__test_rounds_id
                    }
                }
            }

        query = [
            match_query
            , {
                '$project': {
                    'basicDataId': 1,
                    'hardBin': 1,
                    'res': 1
                }
            }, {
                '$unwind': {
                    'path': '$res'
                }
            },
            {
                '$match': {
                    'res.num': {
                        '$ne': '000'
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        TEST_NAME: '$res.txt',
                        VALUE: '$res.res'
                    },
                    COUNTER: {'$sum': 1},
                    TEST_NUM: {'$max': '$res.num'}
                }
            },
            {
                '$project': {
                    TEST_NAME: '$_id.' + TEST_NAME,
                    TEST_NUM: '$' + TEST_NUM,
                    VALUE: '$_id.' + VALUE,
                    COUNTER: 1,
                    "_id": 0
                }
            }
        ]
        data = self.__agent.part.aggregate(query)
        # self.agent.logger.info_list(data)
        return list(data)

    def get_test_names(self):
        """
        取 测试项的 名字， 目前没有用
        """
        if self.__if_only_bin1:
            match_query = {
                '$match': {
                    'basicDataId': {
                        '$in': self.__test_rounds_id
                    },
                    'hardBin': 1
                }
            }
        else:
            match_query = {
                '$match': {
                    'basicDataId': {
                        '$in': self.__test_rounds_id
                    }
                }
            }

        query = [
            match_query
            , {
                '$project': {
                    'basicDataId': 1,
                    'hardBin': 1,
                    'res': 1
                }
            }, {
                '$unwind': {
                    'path': '$res'
                }
            },
            {
                '$match': {
                    'res.num': {
                        '$ne': '000'
                    }
                }
            },
            {
                '$group': {
                    '_id': '$res.txt'
                }
            }
        ]
        data = self.__agent.part.aggregate(query)
        # self.agent.logger.info_list(data)
        return list(data)

    def __calculate_cpk(self, statistic_data_list):
        """
        参考算法网址： http://down.gztaiyou.com/images/23d234.png

        CPK = Cp(1-|Ca|)
        Cp = (High_limit - Low_limit)/6 sigma（标准差）
        Ca = (平均数 - Low_limit)/ 3 sigma(标准差)
        """
        for i in statistic_data_list:
            sigma = round(float(i[STANDARD_DEVIATION]), 4)

            try:
                high = i[HIGH_LIMIT_FIELD]
            except KeyError:
                high = "N/A"
                i[HIGH_LIMIT_FIELD] = high
            try:
                low = i[LOW_LIMIT_FIELD]
            except KeyError:
                low = "N/A"
                i[LOW_LIMIT_FIELD] = low

            if high == "N/A" or low == "N/A":
                cpk = "N/A"
            else:
                try:
                    cp = (high - low) / (6 * sigma)
                    ca = (round(float(i[MEAN]), 4) - round(float(i[LOW_LIMIT_FIELD]), 4)) / (6 * sigma)
                    cpk = cp * (1 - abs(ca))
                except ZeroDivisionError:
                    cpk = "N/A"

            i["CPK"] = cpk

    def get_test_res_number(self):
        """
        查看测试项数量， 以检查得到的数据是否正确
        已查过，完成此需求的代码没有遗漏数据 除非partData数据库中的数据缺失。 （可再次使用进行检查）
        """
        query = [
            {
                '$match': {
                    'basicDataId': {
                        '$in': self.__test_rounds_id
                    }
                }
            },
            {
                '$project': {
                    'basicDataId': 1,
                    'hardBin': 1,
                    'res': 1
                }
            }, {
                '$unwind': {
                    'path': '$res'
                }
            },
            {
                '$group': {
                    '_id': {
                        TEST_NAME: '$res.txt',
                        TEST_NUM: '$res.num'
                    }
                }
            },
            {
                '$group': {
                    '_id': "jjjj",
                    "count": {"$sum": 1}
                }

            }
        ]
        data = self.__agent.part.aggregate(query)
        for i in data:
            print(i)

    def __get_limit(self):
        query = [
            {
                '$match': {
                    '_id': self.__test_rounds_id[0]
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "limit.HI_LIMIT": 1,
                    "limit.LO_LIMIT": 1
                }
            }
        ]
        data = self.__agent.basic.aggregate(query)

        high_limit_list = []
        low_limit_list = []
        for i in data:
            high_limit_list = i["limit"]["HI_LIMIT"]
            low_limit_list = i["limit"]["LO_LIMIT"]

        return high_limit_list, low_limit_list

        # self.agent.logger.info_list(high_limit_list)
        # 得到信息结构如下
        # 16: 34:41 - [INFO]
        # {'_t': 'TestRes', 'num': '000', 'txt': 'HEAD_NUM', 'res': 0}
        # 16: 34:41 - [INFO]
        # {'_t': 'TestRes', 'num': '000', 'txt': 'NUM_TEST', 'res': 0}
        # 16: 34:41 - [INFO]
        # {'_t': 'TestRes', 'num': '000', 'txt': 'PART_FIX', 'res': 0}

    def close_db(self):
        self.__agent.close()


if __name__ == "__main__":
    # 实验数据
    test_lot = "LXT2128N008-D001.002"

    R4_3 = Requirement4_3(test_lot)

    # # # 当需要只看bin1数据时， main_function()参数输入True
    # -----------------------------------
    data = R4_3.main_function()
    for i in data:
        print(i)
    # ---------------------------------

    R4_3.close_db()
