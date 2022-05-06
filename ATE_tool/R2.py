from datetime import datetime
import mongoConnect
import utilities
import pandas as pd

BASIC_LOT_INFORMATION_FIELD = "basic_lot_information"

TESTER_NO_FIELD = 'tester_No'  # 机台编号
TESTER_TYPE_FIELD = 'tester_type'
TEST_ROUNDS_LIST_FIELD = 'test_rounds_list'
FACTORY_FIELD = "OSAT(factory)"
MPN_FIELD = 'mpn'
WAFER_LOT_FIELD = 'wafer_lot'
TEST_LOT_FIELD = "test_lot"
TEST_ROUND_START_TIME_FIELD = "start_time"
SINGLE_TEST_ROUND_WORK_DURATION = "work_duration(h)"
USER_CHOOSED_TIME_DURATION_FIELD = 'chosen_time_duration(h)'
USAGE_RATE_FIELD = 'usage_rate(%)'
BIN1_OUTPUT_FIELD = 'output'
ONE_HOUR_IN_TIMESTAMP = 3600


class Requirement2_1(object):
    """
    此类负责完成需求2.1
    """

    def __init__(self, start_datetime: str, end_datetime: str, filter_factory=utilities.DEFAULT_FILTER,
                 filter_mpn=utilities.DEFAULT_FILTER, filter_tester_no=utilities.DEFAULT_FILTER):
        """
        初始化Requirement2.1
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

    def close_db(self):
        """关闭数据库连接"""
        self.__agent.close()

    def main_function(self):
        """
        list information about factory, test lot, etc for a single lot.
        start time between start_time_min and start_time_max will be taken into account.
        e.g. test_start_time_min = "2021-08-21 1:00:00"
             test_start_time_max = "2021-08-22 23:00:00"
        """
        # 过滤lot
        dir_list = utilities.get_lot(self.__start_datetime, self.__end_datetime)

        query = [
            {
                '$match': {
                    "dirname": {"$in": dir_list}
                }
            },
            utilities.customized_filter(self.filter_factory, self.filter_mpn, self.filter_tester_no),
            {
                '$group': {
                    '_id': {
                        'dirname': '$dirname',
                        TESTER_NO_FIELD: {
                            '$toUpper': '$mir.NODE_NAM'
                        },
                        TESTER_TYPE_FIELD: {
                            '$toUpper': '$mir.TSTR_TYP'
                        }
                    },
                    TEST_ROUNDS_LIST_FIELD: {
                        '$addToSet': '$_id'
                    }
                }
            }
        ]

        cursor = self.__agent.basic.aggregate(query)
        print("--------get all data from mongodb query----------")

        list_from_cursor = []
        for i in cursor:
            list_from_cursor.append(i)
        # store them into a list

        for item in list_from_cursor:
            sample_test_round = mongoConnect.AteLotInfo(
                self.__agent.basic.find_one({"_id": item[TEST_ROUNDS_LIST_FIELD][0]}))
            fileName_info = sample_test_round.filenameInfo
            item[BASIC_LOT_INFORMATION_FIELD] = {}
            basic_lot_information = item[BASIC_LOT_INFORMATION_FIELD]

            basic_lot_information[FACTORY_FIELD] = fileName_info.factory
            basic_lot_information[MPN_FIELD] = fileName_info.mpn
            basic_lot_information[WAFER_LOT_FIELD] = fileName_info.wafer_lot
            basic_lot_information[TEST_LOT_FIELD] = fileName_info.test_batch_num
            basic_lot_information[TESTER_TYPE_FIELD] = item["_id"][TESTER_TYPE_FIELD]
            basic_lot_information[TESTER_NO_FIELD] = item["_id"][TESTER_NO_FIELD]
        print("-----finish first step-------")

        for i in list_from_cursor:
            for j in range(len(i[TEST_ROUNDS_LIST_FIELD])):
                # ---------------------------------
                query = [
                    {
                        "$match": {
                            "_id": i[TEST_ROUNDS_LIST_FIELD][j]
                        }
                    },
                    {
                        "$project": {
                            "mir": 1,
                            "filename": 1,
                            "dirname": 1,
                            "hbr": "$new_hbr",
                            "mrr": 1,
                            "chips_test_duration_ms": 1
                        }
                    },
                    # 为不存在mrr的数据添加自己计算的 FINISH_T
                    utilities.set_finish_time()
                ]

                single_test_round_cursor = self.__agent.basic.aggregate(query)
                single_test_round = {}
                for item in single_test_round_cursor:
                    single_test_round = item

                # -----------------------------------

                # # 把 id 换成AteLotInfo对象 - list_from_cursor中TEST_ROUNDS_LIST_FIELD裝的是AteLotInfo对象
                # # 本来用的是hbr, 由于有的数据不全，现在用new_hbr; 代码改动处： 'hbr':1 => 'hbr':'$new_hbr'
                # single_test_round = self.__agent.basic.find_one(
                #     {"_id": i[TEST_ROUNDS_LIST_FIELD][j]},
                #     {"mir": 1, "filename": 1, "dirname": 1, "mrr": 1, "hbr": '$new_hbr'})

                i[TEST_ROUNDS_LIST_FIELD][j] = mongoConnect.AteLotInfo(single_test_round)

        print("--------finish the second 'for loop step'----------")

        for val in list_from_cursor:
            # 在这里排序
            mongoConnect.bubble_sort(val[TEST_ROUNDS_LIST_FIELD])
            # 添加更多其他的字段
            self.add_more_fields(val)

        return list_from_cursor

    def get_ft_num(self, test_rounds_group):
        """
        得到初测轮次中所测芯片的数量
        test_rounds_group: 装有AteLotInfo对象的list
        """
        total_num = 0
        for lot in test_rounds_group:
            if lot.filenameInfo.is_ft:
                total_num += lot.get_hbin_cnt_num()
        return total_num

    def add_more_fields(self, val):
        """
        val: 单个字典
        在TEST_ROUNDS_LIST_FIELD中，储存测试轮次起始时间， 时长， 测试轮次之间的间隔时间
        """
        test_rounds = val[TEST_ROUNDS_LIST_FIELD]
        total_test_num = self.get_ft_num(test_rounds)
        # program_version_set = set()

        # 添加字段“LotSize”
        val[BASIC_LOT_INFORMATION_FIELD]["LotSize"] = total_test_num

        # 因为要在处理下一个测试轮次时使用上一个测试轮次的结束时间， 所以用ts_last_end记录一下
        ts_last_end = 0
        # 历遍每个TEST_ROUNDS_LIST_FIELD字段元素（测试轮次）
        for index in range(len(test_rounds)):
            single_test_round = test_rounds[index]
            # program_version_set.add(single_test_round.get_program_version())
            temp_round = {}

            # 得到这个测试轮次的开始时间（mir.START_T）
            ts_start = single_test_round.get_start_timestamp()
            # 得到这个测试轮次的结束时间（mrr.FINISH_T）
            ts_stop = single_test_round.get_stop_timestamp()

            temp_round["round"] = single_test_round.filenameInfo.test_round
            try:
                dt_start = datetime.fromtimestamp(ts_start)
                dt_stop = datetime.fromtimestamp(ts_stop)

                temp_round[TEST_ROUND_START_TIME_FIELD] = dt_start.strftime("%Y-%m-%d %H:%M:%S")
                temp_round["stop_time"] = dt_stop.strftime("%Y-%m-%d %H:%M:%S")

                temp_round["duration(hours)"] = "{:.02f}".format((ts_stop - ts_start) / 3600)
                temp_round["intervals(hours)"] = "{:.02f}".format(
                    (ts_start - ts_last_end) / 3600) if ts_last_end != 0 else "N/A"
            except TypeError:
                temp_round[TEST_ROUND_START_TIME_FIELD] = "N/A"
                temp_round["stop_time"] = "N/A"
                temp_round["duration(hours)"] = "N/A"
                temp_round["intervals(hours)"] = "N/A"
                self.__agent.logger.error(
                    "temp_round[start_time]/[stop_time]可能为N/A; ==> ts_start: " + str(ts_start) + ", ts_stop: " + str(
                        ts_stop) + " -- " + single_test_round.filenameInfo.filename)

            temp_round["parts_Number_in_bin"] = single_test_round.get_hbin_cnt_num()

            ts_last_end = ts_stop

            # 用新信息替换test_rounds[index]中的元素
            test_rounds[index] = temp_round


class Requirement2_2(object):
    """
    此类负责完成需求2.2
    """

    def __init__(self, start_datetime: str, end_datetime: str, filter_factory=utilities.DEFAULT_FILTER,
                 filter_mpn=utilities.DEFAULT_FILTER, filter_tester_no=utilities.DEFAULT_FILTER):
        """
        初始化Requirement2.2
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

    def __close_db(self):
        """关闭数据库连接"""
        self.__agent.close()

    def main_function(self):
        """
        将数据根据
        -- TESTER_NO_FIELD: '$mir.NODE_NAM',
        -- TESTER_TYPE_FIELD: '$mir.TSTR_TYP',
        -- 工厂
        分类进行统计

        lot summary: Factory, Timing period , hours(user selected), Tester No, Tester Type, duration of working tester, idle, usage rate, yield
        """

        query2, utc_end, utc_start = self.__get_query(self.__start_datetime, self.__end_datetime)

        print(utc_start, end='')
        print("==>" + self.__start_datetime)
        print(utc_end, end='')
        print("==>" + self.__end_datetime)

        cursor = self.__agent.basic.aggregate(query2, allowDiskUse=True)
        final_data = []

        # 整理格式
        for i in cursor:
            i[FACTORY_FIELD] = self.__get_factory(i["_id"][FACTORY_FIELD])
            i["Time period"] = self.__start_datetime + " -- " + self.__end_datetime
            i[TESTER_NO_FIELD] = i["_id"][TESTER_NO_FIELD]
            i[TESTER_TYPE_FIELD] = i["_id"][TESTER_TYPE_FIELD]
            i.pop("_id")

            final_data.append(i)

        for i in final_data:
            print(i)

        self.__close_db()
        return final_data

    def __get_query(self, start_time, end_time):
        """
        得到 mongodb aggregate， 以解决需求2.2
        """

        time_format = "%Y-%m-%d %H:%M:%S"
        tmin = datetime.strptime(start_time, time_format)
        utc_start = int(tmin.timestamp())
        tmax = datetime.strptime(end_time, time_format)
        utc_end = int(tmax.timestamp())
        query2 = [

            # # 为不存在mrr的数据添加自己计算的 FINISH_T 的逻辑
            utilities.set_finish_time()
            ,
            # 通过时间过滤测试轮次
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
            # 以上： 通过时间进行过滤， 跟 utilities 中的 get_lot() 稍有不同， 此逻辑更为严格，但不能保证lot中的测试轮次完整
            # 因为有一项字段为用户选择时间段时长，所以不用utilities中的那个get_lot()
            # （只选择在时间内的测试轮次）
            utilities.customized_filter(self.filter_factory, self.filter_mpn, self.filter_tester_no),
            {
                '$project': {
                    'dirname': {
                        '$split': [
                            '$dirname', '/'
                        ]
                    },
                    'mir': 1,
                    'mrr': 1,
                    # 用新的sbr； 代码变动： 'sbr':1 --> 'sbr': '$new_sbr'
                    'sbr': "$new_sbr",
                    'filename': 1,
                    # 测试机台工作的时间
                    'work_time': {
                        '$subtract': [
                            '$mrr.FINISH_T', '$mir.START_T'
                        ]
                    },
                    # 用户选择的时间段（时间戳之间的差）
                    'chosen_time_duration': {
                        '$subtract': [
                            utc_end, utc_start
                        ]
                    }
                }
            },
            utilities.add_if_ft_rt()
            , {
                '$group': {
                    '_id': {
                        TESTER_NO_FIELD: '$mir.NODE_NAM',
                        TESTER_TYPE_FIELD: '$mir.TSTR_TYP',
                        # 因为dirname 中不是所有的factory都是以‘/‘划分后的第3项（有的在第二项中）， 所以连起来当做factory可以避免错误。
                        FACTORY_FIELD: {
                            '$toUpper': {
                                "$concat": [
                                    {'$arrayElemAt': [
                                        '$dirname', 2
                                    ]},
                                    "/"
                                    ,
                                    {'$arrayElemAt': [
                                        '$dirname', 3
                                    ]}
                                ]

                            }
                        }
                    },
                    # 分组后，将工作时间相加
                    'work_time': {
                        '$sum': '$work_time'
                    },
                    # 用户使用时间不作处理（取第一个即可）
                    'chosen_time_duration': {
                        '$first': '$chosen_time_duration'
                    },
                    # 初测+复测 的 sbr 添加到set中
                    'merged_sbr': {
                        '$addToSet': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$if_FT_FR', True
                                    ]
                                },
                                'then': '$sbr',
                                'else': None
                            }
                        }
                    }
                }
            }, {
                '$addFields': {
                    'usage_rate(percentage)': {
                        '$multiply': [
                            {
                                '$divide': [
                                    '$work_time', '$chosen_time_duration'
                                ]
                            }, 100
                        ]
                    }
                }
            },
            # 因merged_sbr的结构为array中嵌套array, 所以需要将所有array彻底展开为一个个object
            {
                '$unwind': {
                    'path': '$merged_sbr'
                }
            }, {
                '$unwind': {
                    'path': '$merged_sbr'
                }
            }, {
                # 展开后，根据merged_sbr，添加bin1 output字段(产出)， 不符合要求的 视为产出为0. （这样最后相加时，对于所有的原sbr中的元素， 只有符合要求的才对output有影响）
                '$addFields': {
                    'output': {
                        '$cond': {
                            'if': {
                                '$and': [
                                    {
                                        '$eq': [
                                            '$merged_sbr.HEAD_NUM', 255
                                        ]
                                    }, {
                                        '$eq': [
                                            '$merged_sbr.SBIN_NUM', 1
                                        ]
                                    }
                                ]
                            },
                            'then': '$merged_sbr.SBIN_CNT',
                            'else': 0
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$_id',
                    SINGLE_TEST_ROUND_WORK_DURATION: {
                        "$first": {'$divide': [
                            '$work_time', ONE_HOUR_IN_TIMESTAMP
                        ]}
                    },
                    USER_CHOOSED_TIME_DURATION_FIELD: {
                        '$first': {
                            '$divide': [
                                '$chosen_time_duration', ONE_HOUR_IN_TIMESTAMP
                            ]
                        }
                    },
                    USAGE_RATE_FIELD: {
                        '$first': '$usage_rate(percentage)'
                    },
                    BIN1_OUTPUT_FIELD: {
                        '$sum': '$output'
                    }
                }
            }
        ]
        return query2, utc_end, utc_start

    def __get_factory(self, dir_name: str):
        """
        获取 工厂名称
        dir_name: 从main_function() 中得到的字段 - （data/<工厂名>）
        """
        # 正常路径： /home/data/<工厂名>/....
        # 不正常路径： /home/<工厂名>/FT_DATA_2020/....
        # main_function()中用的query 取了 2,3项全部   eg:'OSAT(factory)': 'DATA/UNISEM', 工厂名一般为第二项，但也有可能为第一项， 所以取2,3项
        factory = (dir_name.split("/")[1]).upper()
        if factory not in ["ASE", "UNISEM", "FOREHOPE", "HT", "JCET_SIP", "JCET", "UM"]:
            factory = (dir_name.split("/")[0]).upper()
            if factory not in ["ASE", "UNISEM", "FOREHOPE", "HT", "JCET_SIP", "JCET", "UM"]:
                factory = "other"
        return factory

    def input_data_to_db(self, result_data: list):
        """
        写入到mongodb数据库中
        """
        # 写入前清理
        self.__writer.R2_2.drop()
        self.__writer.R2_2.insert_many(result_data)

    def export_to_csv(self, result):
        """
        将数据导出为csv - 通过pandas
        """
        df = pd.DataFrame(result)
        df.to_csv('R2_2_result_example_' + str(self.__start_datetime.replace(":", "_")) + '_' + str(self.__end_datetime.replace(":", "_")) + '.csv')


if __name__ == "__main__":

    # R2_1 = Requirement2_1("2021-08-12 00:00:00", "2021-8-15 23:59:59", filter_factory=["HT", "UM"], filter_mpn=["ESP8266EX"])
    # data = R2_1.main_function()
    # for i in data:
    #     print(i)
    # R2_1.close_db()

    R2_2 = Requirement2_2("2021-08-12 00:00:00", "2021-8-15 23:59:59", filter_factory=["HT", "UM"],
                          filter_mpn=["ESP8266EX"])
    data2 = R2_2.main_function()
    for i in data2:
        print(i)
