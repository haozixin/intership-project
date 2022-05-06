import re
from re import compile

import mongoConnect
import utilities
import pandas as pd

TEST_ROUND_ID = 'test_round_id'

AVERAGE_OUTPUT_FIELD = "average_output"

PROGRAM_VERSION_FILED = 'program_version'
MPN_FIELD = 'mpn'
TESTER_NO_FIELD = 'tester_no'  # 机台编号
SITE_NUMBER_FILED = 'site_number'
ALL_BIN1_FIELD = 'all_bin1'
TEST_ROUNDS_LIST_FIELD = 'test_rounds_list'
TEST_ROUND_START_TIME_FIELD = "start_time"
TEST_ROUND_FINISH_TIME_FIELD = "finish_time"
TEST_BATCH_NUMBER = 'test_batch_num'
WAFER_LOT_FIELD = 'wafer_lot'
FACTORY_FIELD = "OSAT(factory)"


class Requirement1(object):
    """
    此类负责完成需求1
    """

    def __init__(self, start_datetime: str, end_datetime: str, filter_factory=utilities.DEFAULT_FILTER,
                 filter_mpn=utilities.DEFAULT_FILTER, filter_tester_no=utilities.DEFAULT_FILTER):
        """
        初始化Requirement1
        """
        self.__agent = mongoConnect.AteDbAgent()
        """提取数据通过已经设置好的__agent"""
        self.__writer = mongoConnect.CalDb()
        """通过writer写入有写入权限的数据库"""
        self.__start_datetime = start_datetime
        self.__end_datetime = end_datetime
        # 默认输入字符串"ALL" - 全部工厂(不过滤)
        self.filter_factory = filter_factory
        # 默认输入字符串“ALL” - 全部mpn（不过滤）
        self.filter_mpn = filter_mpn
        self.filter_tester_no = filter_tester_no

    def __close_db(self):
        """关闭数据库连接"""
        self.__agent.close()

    def __get_lot(self, start_time_min, start_time_max):
        """
        得到指定时间内的lot
        start_time_min: 起始时间
        start_time_max: 截止时间
        返回 结构为 [{'_id':XXXX, 'test_rounds_list':[XX, XX, XX, XX]}, {XXX}, ...]的list
        """
        dir_list = utilities.get_lot(start_time_min, start_time_max)

        query = [
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
            }
        ]
        '''
        一段 mongodb query 字符串 
        (mongodb只运行此query得到的结果：{'_id':XXXX, 'test_rounds_list':[XX, XX, XX, XX]})
        ('_id' 为路径名， 'test_rounds_list'为测试轮次在mongodb中的id列表)
        '''

        lots = self.__agent.basic.aggregate(query)

        print("has got all lots data")

        return list(lots)

    def __get_query(self, test_round, dir):
        """
        同一文件夹内（dir）的测试轮次 肯定来自一个工厂，符合同一个命名规则
        下面的 mongodb 的 aggregate 命令中, 会变化的参数是（会受命名规则的影响的参数）：
            工厂名 -- test_round_info.get_factory() （全大写）
            mpn --- 取文件名按“_”或“-”分成数组后的第x个
            新加的轮次字段（'if_FT'）- 判断是不是 初测+复测， 用于计算总产出Test_in
        lot ： 任意取一个test_round，判断这个lot是哪个工厂的;
        dir ： 是 lot的文件夹地址 - 用以按文件夹路径（dirname）将测试轮次（documents in mongodb）分类 以得到 (大)lot 然后以lot 为单位继续以后的步骤
        函数功能：根据各工厂不同的文件命名规则 满足需求1 - ppt12的结果
        返回 mongodb的aggregate pipline
        """
        test_round_info = mongoConnect.AteLotInfo(test_round)
        split_with, query_for_mpn = utilities.get_mpn(test_round_info)

        # query_for_mpn为空（基本不可能）， 则返回空list
        if not query_for_mpn:
            return []

        # 下面为代码相同的部分， 也是返回的部分
        query_for_agg = [
            {
                '$match': {
                    'dirname': dir
                }
            },
            utilities.set_finish_time()
            ,
            {
                '$project': {
                    '_id': '$_id',
                    'filename2': {
                        '$split': [
                            '$filename', split_with
                        ]
                    },
                    'mir': 1,
                    'sbr': "$new_sbr",
                    'sdr': 1,
                    'mrr': 1,
                    'filename': 1
                }
            }, {
                '$addFields': {
                    'mpn': {"$toUpper": query_for_mpn},
                    'site_n': {
                        '$max': '$sdr.SITE_CNT'
                    },
                    'lot_start_time': '$mir.START_T',
                    'lot_finish_time': '$mrr.FINISH_T'
                }
            }, {
                '$unwind': {
                    'path': '$sbr'
                }
            }, {
                '$addFields': {
                    'mir_part_type': '$mir.PART_TYP'
                }
            },
            utilities.add_if_ft_rt(),
            utilities.add_bin1_chips(),
            {
                '$addFields': {
                    'all_bin1': {
                        '$cond': {
                            'if': '$if_FT_FR',
                            'then': '$bin1',
                            'else': 0
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        MPN_FIELD: {"$toUpper": '$mpn'},
                        TESTER_NO_FIELD: '$mir.NODE_NAM'
                    },
                    PROGRAM_VERSION_FILED: {
                        '$addToSet': '$mir.JOB_NAM'
                    },
                    SITE_NUMBER_FILED: {
                        '$max': '$site_n'
                    },
                    ALL_BIN1_FIELD: {
                        '$sum': '$all_bin1'
                    },
                    TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': {
                            TEST_ROUND_ID: '$_id',
                            TEST_ROUND_START_TIME_FIELD: '$lot_start_time',
                            TEST_ROUND_FINISH_TIME_FIELD: '$lot_finish_time'
                        }
                    },
                    'mir_part_type': {
                        '$addToSet': '$mir_part_type'
                    }
                }
            }
        ]

        return query_for_agg

    def main_function(self):
        """
        start_time_min: 起始时间
        start_time_max: 截止时间
        格式 --> "2021-08-21 1:00:00"
        返回： 此需求最终结果
        """

        # lot_list 结构： 【{"_id": dirname, "test_rounds_list":[{ObjectId('65413115...')},{}...]}】
        # 得到要处理的lot
        lot_list = self.__get_lot(self.__start_datetime, self.__end_datetime)

        print("-------------processing those lot -------------")
        # final_data用来裝处理好的数据
        final_data = []
        # 下面历遍每一个lot
        for lot_item in lot_list:
            # dir = dirname; 用于分类
            dir = lot_item['_id']
            # sample_test_round = lot中的随便一个test_round(这里取第一个)，用于判断是哪个工厂
            sample_lot = self.__agent.basic.find_one({'_id': lot_item[TEST_ROUNDS_LIST_FIELD][0]})

            print("===================================================================================")
            print("------dirname(lot id): " + dir)

            query = self.__get_query(sample_lot, dir)

            if len(query) != 0:
                result = self.__agent.basic.aggregate(query)
                # 历遍 经过mongodb aggregate query 处理后的数据（lot）
                for i in result:
                    i["_id"][FACTORY_FIELD] = mongoConnect.AteLotInfo(sample_lot).get_factory()
                    # i["_id"][PROGRAM_VERSION_FILED] = i[PROGRAM_VERSION_FILED]
                    i["_id"][TESTER_NO_FIELD] = i["_id"][TESTER_NO_FIELD].upper()
                    i[TEST_BATCH_NUMBER] = [mongoConnect.AteLotInfo(sample_lot).filenameInfo.test_batch_num]
                    i[WAFER_LOT_FIELD] = [mongoConnect.AteLotInfo(sample_lot).filenameInfo.wafer_lot]
                    # i.pop(PROGRAM_VERSION_FILED)
                    i_has_added = False
                    # 用i_state 记录是否在final_data里有这条记录或者已经合并了这条记录
                    # 数据经过处理之后 需要把得到的初步的统计数据进行合并 - 即 合并mpn, 测试机台， 工厂 相同的数据（lot），
                    # 以应对一种芯片分批分配在同一个工厂的同一个机台用同一个测试程序测试的情况
                    if len(final_data) != 0:
                        for iterator in final_data:
                            # 如果还没导入的数据跟以及导入的数据有“_id”上的重复，则合并, 合并规则规则如下
                            if i['_id'] == iterator['_id'] and not i_has_added:
                                if iterator[SITE_NUMBER_FILED] < i[SITE_NUMBER_FILED]:
                                    iterator[SITE_NUMBER_FILED] = i[SITE_NUMBER_FILED]
                                iterator[ALL_BIN1_FIELD] += i[ALL_BIN1_FIELD]
                                iterator[TEST_BATCH_NUMBER] += i[TEST_BATCH_NUMBER]
                                iterator[WAFER_LOT_FIELD] += i[WAFER_LOT_FIELD]
                                iterator[TEST_ROUNDS_LIST_FIELD] += i[TEST_ROUNDS_LIST_FIELD]
                                i_has_added = True
                            else:
                                pass
                        # 历遍完成，如果没有匹配到相同的_id, 则可以直接添加进final_data
                        if not i_has_added:
                            final_data.append(i)
                    else:
                        final_data.append(i)

            else:
                pass
        for i in final_data:
            # 添加idle时间, 并添加机台测试时长和平均产出字段
            self.__agent.get_idle_time(i)
            # 添加平均产出字段
            i[AVERAGE_OUTPUT_FIELD] = i[ALL_BIN1_FIELD] / i[mongoConnect.TEST_DURATION_FIELD]
            self.__agent.get_test_time(i)
            # 计算并添加UPH字段
            i["UPH"] = i[ALL_BIN1_FIELD] / i[mongoConnect.TEST_DURATION_FIELD]
        for item in final_data:
            print(item)

        self.__close_db()
        return list(final_data)

    def input_data_to_db(self, result_data: list):
        """
        写入到mongodb数据库中
        """
        # 写入前清理
        self.__writer.R1.drop()
        self.__writer.R1.insert_many(result_data)

    def export_to_csv(self, result):
        """
        将数据导出为csv - 通过pandas
        """
        df = pd.DataFrame(result)
        df.to_csv('R1_result_example_' + str(self.__start_datetime).replace(":", "_") + '_' + str(self.__end_datetime.replace(":", "_")) + '.csv')


if __name__ == "__main__":
    # 输入要过滤的工厂和mpn时，格式必须为list [] （至少一个元素， 或不输入-删掉整个filter_factory=["XX", "XX"], filter_mpn=["XXXXXXX"]）
    # mpn 可以不输入全, mpn和工厂参数输入 大小写均可 （模糊查询）
    R1 = Requirement1("2021-08-15 00:00:00", "2021-08-20 23:59:59", filter_factory=["HT", "UM"], filter_mpn=["ESP8266EX"], filter_tester_no=["U-DMDX2"])
    # U-DMDX2
    data = R1.main_function()
    R1.export_to_csv(data)
    # R1.input_data_to_db(data)
