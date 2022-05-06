from pymongo import MongoClient
from bson.objectid import ObjectId
import logging
import time
import os
import sys
from colorama import Fore, Style
from datetime import date, datetime
import pandas as pd
import utilities

# 字段名称信息 ， 更改最终显示的字段名称，可（只）从此处修改-----------------------------------
CHIP_TEST_TIME_SECONDS_FIELD = "chip_test_time(seconds)"
IDLE_TIME = "idle_time(hour)"
TEST_DURATION_FIELD = "test_duration(day)"
TEST_ROUND_ID = 'test_round_id'
TEST_ROUND_FINISH_TIME_FIELD = "finish_time"
TEST_ROUND_START_TIME_FIELD = "start_time"

TEST_ROUNDS_LIST_FIELD = 'test_rounds_list'
ALL_BIN1_FIELD = 'all_bin1'

ONE_HOUR_IN_TIMESTAMP = 3600
ONE_DAY_IN_TIMESTAMP = 86400

# Database information -----------------
# FREE_ 表示拥有写入权限数据库的相关变量
FREE_DB_ADDRESS = "mongodb://cepq_cal_admin:bONZ2hObNeb5@192.168.8.107/cepq_cal"
LOCAL_DB_ADDRESS = "mongodb://localhost/cepq_cal"
NEW_DB_ADDRESS = "mongodb://cepq_new_user:espressif123@192.168.8.107/cepq_new"
PART_DATA = "PartData"
BASIC_DATA = "BasicData"
DATABASE_NAME = "cepq_new"
FREE_DATABASE_NAME = "cepq_cal"
R1 = "R1"
R2_2 = "R2_2"
R4_1 = "R4_1"
R4_2 = "R4_2"
R5 = "R5"


class CalDb(object):
    """
    连接CalDb数据库，此数据库有写入权限，需要时可用
    """

    def __init__(self):
        """
        initial AteDbAgent
        db_address:   mongodb://user:password@db1_server_ip
        """

        self.client = MongoClient(FREE_DB_ADDRESS)
        self.__db = self.client[FREE_DATABASE_NAME]
        # print(db.list_collection_names())
        self.R1 = self.__db[R1]
        self.R2_2 = self.__db[R2_2]
        self.R4_1 = self.__db[R4_1]
        self.R4_2 = self.__db[R4_2]
        self.R5 = self.__db[R5]
        self.test = self.__db["TestB"]
        self.test2 = self.__db["TestP"]

        self.logger = Logger(logger="MongoDB(cepq_cal)")
        self.logger.set_level_debug()

    def close(self):
        """
        close database
        """
        self.client.close()


class Logger(object):
    def __init__(self, logger=""):
        """
        指定保存日志的文件路径，日志级别，以及调用文件
        将日志存入到指定的文件中
        :param logger:  定义对应的程序模块名name，默认为root
        """

        # 创建一个logger
        logging.basicConfig(level=logging.DEBUG, filename=logger + '.log', filemode='w',
                            format='%(asctime)s %(levelname)s: %(message)s')
        self.logger = logging.getLogger(name=logger)
        self.logger.setLevel(logging.DEBUG)  # 指定最低的日志级别 critical > error > warning > info > debug

        # 创建一个handler，用于写入日志文件
        rq = time.strftime("%H-%M-%S", time.localtime(time.time()))
        # rq = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime(time.time()))
        log_path = os.getcwd() + "/logs/"
        log_name = log_path + rq + ".log"
        #  这里进行判断，如果logger.handlers列表为空，则添加，否则，直接去写日志，解决重复打印的问题
        if not self.logger.handlers:
            # 创建一个handler，用于输出到控制台
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.DEBUG)

            # 定义handler的输出格式
            # formatter = logging.Formatter(
            #     "%(asctime)s - %(filename)s[line:%(lineno)d] - %(message)s")
            formatter = logging.Formatter(
                "%(asctime)s - %(message)s", "%H:%M:%S")
            ch.setFormatter(formatter)

            # 给logger添加handler
            # self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def level(self, level):
        self.logger.setLevel(level)

    def set_level_debug(self):
        self.logger.setLevel(logging.DEBUG)

    def set_level_info(self):
        self.logger.setLevel(logging.INFO)

    def set_level_error(self):
        self.logger.setLevel(logging.ERROR)

    def set_level_warning(self):
        self.logger.setLevel(logging.WARNING)

    def debug(self, msgs):
        """
        defin output color: debug--white，info--green，warning/error/critical--red
        :param msg: 输出的log文字
        :return:
        """
        for msg in msgs.split("\n"):
            if msg:
                self.logger.debug(Fore.WHITE + "[DEBUG] " + str(msg) + Style.RESET_ALL)

    def info(self, msgs):
        for msg in msgs.split("\n"):
            if msg:
                self.logger.info(Fore.GREEN + "[INFO] " + str(msg) + Style.RESET_ALL)

    def info_list(self, _list):
        for item in _list:
            self.logger.info(Fore.GREEN + "[INFO] " + str(item))

    def warning(self, msgs):
        for msg in msgs.split("\n"):
            if msg:
                self.logger.warning(Fore.YELLOW + "[WARNING] " + str(msg) + Style.RESET_ALL)

    def warn_list(self, _list):
        for item in _list:
            self.logger.warning(Fore.YELLOW + "[WARNING] " + str(item))

    def error(self, msgs):
        for msg in msgs.split("\n"):
            if msg:
                self.logger.error(Fore.LIGHTRED_EX + "[ERROR] " + str(msg) + Style.RESET_ALL)

    def critical(self, msgs):
        for msg in msgs.split("\n"):
            if msg:
                self.logger.critical(Fore.RED + "[CRITICAL] " + str(msg) + Style.RESET_ALL)


class FileNameInfo(object):
    """
    负责储存,提取，测试轮次 filename中可获得的信息
    只 满足给出的命名规则文件中的规则， 数据可能有在范围外的，参考文档中其他思路
    """

    def __init__(self, filename, factory_name):
        """
        初始化 FileNameInfo
        filename: 测试轮次的filename
        factory_name: 测试轮次的工厂名 - （不同的工厂有不同的filename命名规则）
        """

        self.logger = Logger(logger="FileNameInfo")
        self.logger.set_level_debug()

        self.factory = factory_name
        self.mpn = "Cannot get from fileName"
        self.test_round = "Cannot get from fileName"

        # 是否为初测第一1轮
        self.is_ft1 = False
        # 是否为初测
        self.is_ft = False
        self.filename = filename
        self.wafer_lot = "N/A"
        self.test_batch_num = "N/A"

        if ("JCET" in self.filename.upper()) or ("JCAP" in self.filename.upper()):
            sp_list = self.filename.split("_")  # checked
            self.mpn = "-".join(sp_list[2].split("-")[:-1])  # checked
            self.test_round = "_".join(sp_list[5:7])  # fixed
            self.is_ft1 = (("FT1_R0" in self.test_round) and not ("R0." in self.test_round))
            self.is_ft = ("FT1_R0" in self.test_round)  # checked
            self.wafer_lot = sp_list[3]  # fixed
            self.test_batch_num = sp_list[4]  # fixed
            self.factory = "JCET"
        elif "ASECL" in self.filename.upper():
            sp_list = self.filename.split("_")  # checked
            self.mpn = "-".join(sp_list[2].split("-")[:-1])  # checked
            self.test_round = "_".join(sp_list[6:8])
            self.is_ft1 = ("ft1_r0".lower() in self.test_round.lower())
            self.is_ft = (("FT1" in self.test_round.upper()) and ("R0" in self.test_round.upper()))
            self.wafer_lot = sp_list[3]
            self.test_batch_num = sp_list[5]  # fixed
            self.factory = "ASE"
        elif "FOREHOPE" in self.filename.upper():
            sp_list = self.filename.split("_")  # checked
            self.test_round = "_".join(sp_list[5:7])  # fixed
            self.mpn = "-".join(sp_list[2].split("-")[:-1])  # checked
            self.is_ft1 = (("FT1_R0" in self.test_round) and not ("R0.1" in self.test_round))
            self.is_ft = ("FT1_R0" in self.test_round)  # checked
            self.wafer_lot = sp_list[3]  # fixed
            self.test_batch_num = sp_list[4]  # fixed
            self.factory = "FOREHOPE"

        elif self.filename.startswith("ESP-"):
            sp_list = self.filename.split("-")  # checked
            self.test_round = "_".join(sp_list[1].split("_")[1:])  # fixed
            if self.factory == "UM":
                self.mpn = "-".join(sp_list[-6:-5])  # checked
                self.is_ft1 = ("FT_01" in self.test_round)
                self.is_ft = (("FT" in self.test_round) and not ("R" in self.test_round))  # checked
                self.wafer_lot = sp_list[2]  # fixed
                self.test_batch_num = sp_list[1].split("_")[0]  # fixed
                self.factory = "UM"
            elif self.factory == "UNISEM":
                self.mpn = "-".join(sp_list[-6:-4])  # checked
                self.is_ft1 = ("FT_01" in self.test_round)
                self.is_ft = (("FT" in self.test_round) and ("_R0" not in self.test_round))  # checked
                self.wafer_lot = "_".join(sp_list[2:4])  # fixed
                self.test_batch_num = sp_list[1].split("_")[0]  # fixed
                self.factory = "UNISEM"

        elif self.filename.startswith("UTTT"):
            sp_list = self.filename.split("_")  # checked
            self.test_round = "_".join(sp_list[4:6])  # fixed
            self.mpn = "-".join(sp_list[1].split("-")[:-1])  # checked
            self.is_ft1 = ("FT_01" in self.test_round)
            self.is_ft = (("FT" in self.test_round) and not ("FT_R0" in self.test_round))  # checked
            self.wafer_lot = sp_list[2]  # fixed
            self.test_batch_num = sp_list[3]  # fixed
            self.factory = "UNISEM"

        elif self.filename.startswith("SH688-"):
            sp_list = self.filename.split("-")  # checked
            self.mpn = "-".join(sp_list[1:2])  # checked
            self.test_round = sp_list[-4]  # checked
            self.is_ft = ("FT" in self.test_round)  # checked
            self.is_ft1 = ("FT1" in self.test_round)  # checked
            self.wafer_lot = "-".join(sp_list[3:5])  # checked
            self.test_batch_num = sp_list[-4]  # fixed
            self.factory = "HT"
        elif self.filename.startswith("SH688_"):
            sp_list = self.filename.split("_")  # checked
            self.mpn = "-".join(sp_list[1].split("-")[:-1])  # checked
            self.test_round = sp_list[4]  # checked
            self.is_ft = ("FT" in self.test_round)  # checked
            self.is_ft1 = ("FT1" in self.test_round)  # checked
            self.wafer_lot = sp_list[2]  # checked
            self.test_batch_num = sp_list[3]  # fixed
            self.factory = "HT"

        else:
            self.logger.error("Un-Known filename format from Unknown folder ==> " + filename)


class AteLotInfo(object):
    """
    Each AteLotInfo instance represents test round information of each record/document in db.basic.
    The class is responsible for getting test rounds information including those in filename
    each instance is a test round (getting test round through parameter)
    """

    def __init__(self, test_round):
        """
        初始化 AteLotInfo
        test_round: 测试轮次 - 即数据库BasicData中的单个document
        """

        self.logger = Logger(logger="LotInfo")
        self.logger.set_level_error()

        self.test_round = test_round
        """保存完整的 test_round 即数据库BasicData中的一个整个的document"""
        self.dir_name = self.test_round["dirname"]
        """directory name - 路径名dirname - 相当于lot的id"""
        self.filenameInfo = FileNameInfo(self.test_round["filename"], self.get_factory())
        """一个filenameInfo对象  - 整个文件名相当于单个测试轮次的id"""

    def get_hbin_cnt_num(self):
        """
        获取 hard bin 中的芯片数量
        HBIN_CNT = Number of parts in bin
        """

        try:
            HBIN_CNT = 0
            for hbin in self.test_round['hbr']:
                if hbin['HEAD_NUM'] == 255:
                    HBIN_CNT += hbin['HBIN_CNT']

        except KeyError:
            # 有些数据没有 hbr 字段
            self.logger.error(
                "The test_round(document in mongodb) doesn't have 'hbr' ==> " + self.test_round["filename"])
            HBIN_CNT = 0
        return HBIN_CNT

    def get_mpn(self):
        """
        从mir字段中获取 mpn；
        mir.PART_TYP字段中不仅有mpn还有别的信息， 不同工厂还可能不同内容，所以最好只用作比对、备用
        """
        try:
            return self.test_round["mir"]["PART_TYP"]
        except KeyError:
            self.logger.error(
                "The test_round(document in mongodb) doesn't have 'mir.PART_TYP' ==> " + self.test_round["filename"])
            return "N/A"

    def get_program_version(self):  # checked
        """
        从mir字段中获取 测试版本；
        mir.JOB_NAM 字段中不仅有 测试版本 还有别的信息， 不同工厂还可能不同内容，
        即使命名规则文件中，也有部分工厂没有将测试版本写入filename, 所以也不可以从 filename 中提取
        """
        try:
            return self.test_round["mir"]["JOB_NAM"]
        except KeyError:
            self.logger.error(
                "The test_round(document in mongodb) doesn't have 'mir.JOB_NAM' ==> " + self.test_round["filename"])
            return "N/A"

    def get_tester_type(self):
        """
        提取 tester_type 测试机台类型
        """
        try:
            return self.test_round["mir"]["TSTR_TYP"]
        except KeyError:
            self.logger.error(
                "The test_round(document in mongodb) doesn't have 'mir.TSTR_TYP' ==> " + self.test_round["filename"])
            return "N/A"

    def get_tester_num(self):
        """
        提取 测试机台 号号号
        """
        try:
            return self.test_round["mir"]["NODE_NAM"]
        except KeyError:
            self.logger.error(
                "The test_round(document in mongodb) doesn't have 'mir.NODE_NAM' ==> " + self.test_round["filename"])
            return "N/A"

    def get_start_timestamp(self):
        """
        提取测试轮次的 测试结束开始时间
        """
        try:
            start_time = self.test_round['mir']["START_T"]
        except KeyError:
            self.logger.error("The lot doesn't have 'mir' ==> " + self.test_round["filename"])
            start_time = "N/A"
        return start_time

    def get_stop_timestamp(self):
        """提取测试轮次的测试结束时间"""

        try:
            return self.test_round['mrr']['FINISH_T']
        except KeyError:
            self.logger.error("The lot doesn't have 'mrr' ==> " + self.test_round["filename"])
            return "N/A"

    def get_factory(self):
        # 正常路径： /home/data/<工厂名>/....
        # 不正常路径： /home/<工厂名>/FT_DATA_2020/....
        factory = (self.dir_name.split("/")[3]).upper()
        if factory not in ["ASE", "UNISEM", "FOREHOPE", "HT", "JCET_SIP", "JCET", "UM"]:
            factory = (self.dir_name.split("/")[2]).upper()
            if factory not in ["ASE", "UNISEM", "FOREHOPE", "HT", "JCET_SIP", "JCET", "UM"]:
                factory = "other"
        return factory


# class ExportData(object):
#     """
#
#     """
#     def __init__(self, db_address):
#         self.db_address = db_address
#         self.client = MongoClient(self.db_address)
#         self.db = self.client["Data"]
#         self.data1 = self.db["data1"]
#         self.data2 = self.db["data2"]
#         self.R2_2 = self.db["R2.2"]
#
#     def close(self):
#         """
#         close database
#         """
#         self.client.close()
#
#     def export_to_data1_collection(self, data):
#         """data is in BSON format like a kind of list"""
#         self.data1.insert_many(data)
#
#     def export_to_data2_collection(self, data):
#         self.data2.insert_many(data)
#
#     def export_to_R2_2_collection(self, data):
#         self.R2_2.insert_many(data)


def bubble_sort(arr):
    """
    排序
    arr: 装有AteLotInfo对象的list
    """
    for i in range(1, len(arr)):
        for j in range(0, len(arr) - i):
            if arr[j].get_start_timestamp() > arr[j + 1].get_start_timestamp():
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    return arr


class AteDbAgent(object):
    """
    Data Analyze Tool for ATE 
    """

    def __init__(self):
        """
        initial AteDbAgent
        db_address:   mongodb://user:password@db1_server_ip
        """
        self.db_address = NEW_DB_ADDRESS
        self.client = MongoClient(self.db_address)
        self.db = self.client[DATABASE_NAME]
        # print(db.list_collection_names())
        self.basic = self.db[BASIC_DATA]
        self.part = self.db[PART_DATA]
        self.logger = Logger(logger="AteDbAgent")
        self.logger.set_level_debug()
        # self.part_res_df = self._get_part_res_df()

    def get_test_time(self, one_dic_element):
        """
        根据每个测试轮次 _id 获取芯片测试时间并添加到 被 R1.main_function（）处理过的统计数据中
        输入： 单个字典元素 --> R1.main_function返回的结果 - 即每个符合要求的 lot的统计信息和为下一步做准备的必须数据
        输出： 添加所有芯片平均测试时间
        """
        test_round_list = one_dic_element[TEST_ROUNDS_LIST_FIELD]
        test_time = 0
        chip_num = 0
        for i in test_round_list:
            id = i[TEST_ROUND_ID]
            filter = {
                'basicDataId': id
            }
            project = {
                'testT': 1
            }
            test_time_cursor = self.part.find(filter, project)
            for single_chip in test_time_cursor:
                test_time += single_chip["testT"]
                chip_num += 1

        one_dic_element[CHIP_TEST_TIME_SECONDS_FIELD] = (test_time / chip_num) / 1000

    def get_idle_time(self, one_dic_element):
        """
        输入： 单个字典元素 --> R1中 final_data 的结果 - 即每个符合要求的 lot的统计信息（以mpn， 测试机台号分组，得到测试版本，site number, bin1数量
        里面还包括一个list裝测试轮次的开始、结束时间）

        处理内容： 添加idle时间, 并添加机台测试时长， 转化数据中时间戳为时间的格式
        """
        # 按时间顺序排序
        single_test_round = sorted(one_dic_element[TEST_ROUNDS_LIST_FIELD],
                                   key=lambda key: key[TEST_ROUND_START_TIME_FIELD])
        idle_time = 0
        # 历遍每两个测试轮次之间的gap, 得到总的idle时间
        for i in range(1, len(single_test_round)):
            difference = single_test_round[i][TEST_ROUND_START_TIME_FIELD] - single_test_round[i - 1][
                TEST_ROUND_FINISH_TIME_FIELD]
            if difference >= ONE_DAY_IN_TIMESTAMP:
                idle_time += difference

        # 看时间戳的差值是多少， （86400 是一天)
        duration = (single_test_round[-1][TEST_ROUND_FINISH_TIME_FIELD] - single_test_round[0][
            TEST_ROUND_START_TIME_FIELD])
        test_duration = (duration - idle_time) / ONE_DAY_IN_TIMESTAMP
        one_dic_element[IDLE_TIME] = idle_time / ONE_HOUR_IN_TIMESTAMP
        one_dic_element[TEST_DURATION_FIELD] = test_duration

        # 转换时间戳转换成时间
        time_format = "%Y-%m-%d %H:%M:%S"
        for i in one_dic_element[TEST_ROUNDS_LIST_FIELD]:
            time_local = time.localtime(i[TEST_ROUND_START_TIME_FIELD])
            i[TEST_ROUND_START_TIME_FIELD] = time.strftime(time_format, time_local)
            time_local = time.localtime(i[TEST_ROUND_FINISH_TIME_FIELD])
            i[TEST_ROUND_FINISH_TIME_FIELD] = time.strftime(time_format, time_local)

    def is_within_timeRange(self, single_lot, start_time_min, start_time_max):
        """
        This is a filter to get the specified data from the basicData collection(information of Single Lot)
        test_start time between test_start_time_min and test_start_time_max will be taken into account.
        e.g. test_start_time_min = "2021-08-21 1:00:00"
             test_start_time_max = "2021-08-22 23:00:00"
        """
        # there are two kind of variable/field can be considered to startTime
        # "mir.START_T" or "mir.SETUP_T"

        time_format = "%Y-%m-%d %H:%M:%S"
        tmin = datetime.strptime(start_time_min, time_format)
        utc_min = int(tmin.timestamp())

        tmax = datetime.strptime(start_time_max, time_format)
        utc_max = int(tmax.timestamp())

        # print("min-time: ", utc_min)
        # print("max-time: ", utc_max)

        single_lot = AteLotInfo(single_lot)
        if single_lot.get_start_timestamp() in range(utc_min, utc_max + 1):
            return True

    def display_cursor(self, lot, target_field):
        """
        display all information
        input: lot(cursor) and target_field(field name) that you want to check
        output: display the result
        """
        for x in lot:
            print("lot directory: " + x['_id'])
            for y in x[target_field]:
                print("single_lot_id: ", end="")
                print(y)

    def find_ft1_lot(self, start_time_min, start_time_max):
        """
        判断是不是符合指定时间内的初测
        返回： 符合要求的初测
        """
        single_lot_list = []
        all_single_test_round = self.basic.find()
        for single_test_round in all_single_test_round:
            is_ft1 = AteLotInfo(single_test_round).filenameInfo.is_ft1
            within_time_range = self.is_within_timeRange(single_test_round, start_time_min, start_time_max)
            if is_ft1 and within_time_range:
                single_lot_list.append(single_test_round)
        # 初测只有一个
        return single_lot_list

    def _get_part_res_df(self, cursor):
        init_flg = True
        res_df = None
        cnt = 0
        # for item in self.part.find():
        for item in cursor:
            resdf = pd.DataFrame(item["res"])
            res_ser = pd.pivot(resdf[["txt", "res"]], columns="txt", index=[])
            resdf = pd.DataFrame(res_ser).T

            if res_df is None:
                res_df = resdf
                init_flg = False
            else:
                print("!!! test resff: ", resdf)
                print("*** test res_df: ", res_df)
                try:
                    res_df.append(resdf)
                except ValueError:
                    self.logger.error("ValueError when dataFrame append: {}".format(resdf))
                print("res_df: ", res_df)
            cnt += 1

        print("test res_df")
        print(res_df)

        print("!!!! est cnt: ", cnt)
        return res_df

    def show_info(self):
        """
        showing database information in the format of "key-value-dataType" by catching the first record(document)
        """
        self.logger.warning("basic keys: {}".format(self.basic.find_one().keys()))
        # zixin
        for i in (self.basic.find_one().keys()):
            self.logger.warning(
                "basic_key: {} || basic_values: {} || value_type: {}".format(i, self.basic.find_one()[i],
                                                                             type(self.basic.find_one()[i])))
            print("===================================================================================================")

        print("*******************************************************************************************************")
        self.logger.warning("part keys: {}".format(self.part.find_one().keys()))

        for i in (self.part.find_one().keys()):
            self.logger.warning("part_key: {} || part_values: {} || value_type: {}".format(i, self.part.find_one()[i],
                                                                                           type(self.part.find_one()[
                                                                                                    i])))
            print("===================================================================================================")

    def get_bin1_cnt(self):
        self.logger.debug("test sbr")
        basic_document = self.basic.find()
        # self.logger.warning("keys: {}".format(basic_document.keys()))
        sbr = basic_document["sbr"]
        self.logger.info_list(sbr)
        # self.logger.info(sbr[0])
        print("test filename: ", self.basic.find_one()["filename"])

        # base = self.basic.find_one({"sbr.HEAD_NUM": 255})
        # if base:
        #     sbr = base['sbr']
        #     self.logger.warn_list(sbr)
        #     # print("test sbr222: ", sbr)
        # else:
        #     self.logger.warning("Found none..")

        # cursor = self.basic.find({"sbr.HEAD_NUM": 255}) 
        # print("test cursor: ", cursor)
        # for item in cursor:
        #     print(item["sbr"]["SITE_NUM"])

        fname_cursor = self.basic.find({"filename": {"$regex": 'ForeHope_*'}})
        # self.logger.info_list(sbr)
        for item in fname_cursor:
            print("test filename: ", item["filename"])
            print("test dirname: ", item["dirname"])

    def close(self):
        """
        close database
        """
        self.client.close()


def check_bin1():
    """
    用于查看/检查单个测试轮次 的bin1芯片数量
    """
    query = [
        {
            '$match': {
                "_id": ObjectId('6183c76738b410128330d82a')
            }
        },
        {
            '$unwind': {
                "path": "$sbr"
            }
        },
        utilities.add_bin1_chips()
        ,
        {
            "$group": {
                '_id': "$_id",
                ALL_BIN1_FIELD: {"$sum": "$bin1"}
            }
        }
    ]
    c = db.basic.aggregate(query)
    for i in c:
        print(i[ALL_BIN1_FIELD])


def check_R4_2():
    query = [
        {
            "$match": {
                "dirname": {
                    "$regex": ".*LXT2128N008-D001.002.*",
                    "$options": "i"
                }
            }
        }
        # ,
        # {
        #     "$project": {
        #         "_id": 1,
        #     }
        # }
    ]
    cursor = db.basic.aggregate(query)
    # writer = CalDb()
    # writer.test.insert_many(list(cursor))

    basicId_list = []
    for i in cursor:
        basicId_list.append(i["_id"])
    print(basicId_list)

    for i in basicId_list:
        query2 = [
            {
                "$match": {
                    "basicDataId": i
                }
            }
        ]
        part_data = db.part.aggregate(query2, allowDiskUse=True)
        # for item in part_data:
        #     print(item)

        writer = CalDb()
        writer.test2.insert_many(list(part_data))


if __name__ == "__main__":
    db = AteDbAgent()

    db.close()
