from datetime import datetime
import mongoConnect

TEST_LOT_FIELD = "test_lot"
DEFAULT_FILTER = "ALL"


def regexMatch(a_input: str, regex: str):
    """
    mongodb中的regexMatch函数
    需/只能在mongodb 的query中运用
    """
    query = {'$regexMatch': {
        'input': {
            '$toUpper': a_input
        },
        'regex': regex
    }}
    return query


def get_lot(start_time_min, start_time_max):
    """
    只要有lot的测试轮次在指定时间内，则视为此lot在时间范围内
    可以保证lot中的测试轮次不会缺少，从而避免统计数据出问题
    输入：起止时间； 格式例子："2021-08-12 00:00:00"
    返回:
    符合时间范围内的 dirname list(即 lot)

    使用方法：
    # 配合下面代码中逻辑使用即可
    # query2 = [
    #     {
    #         '$match': {
    #             "dirname": {"$in": dir_list}
    #         }
    #     }
    # ]
    """

    target_field = "mir.START_T"
    time_format = "%Y-%m-%d %H:%M:%S"
    tmin = datetime.strptime(start_time_min, time_format)
    utc_min = int(tmin.timestamp())

    tmax = datetime.strptime(start_time_max, time_format)
    utc_max = int(tmax.timestamp())

    query = [
        {'$match': {
            "$and": [{target_field: {"$gte": utc_min}},
                     {target_field: {"$lte": utc_max}}]
        }},
        {
            '$group': {
                '_id': 'dirname_list',
                "dir_list": {
                    '$addToSet': '$dirname'
                }
            }
        }
    ]
    db = mongoConnect.AteDbAgent()
    temp_data = db.basic.aggregate(query)
    dir_list = []
    for i in temp_data:
        dir_list = i["dir_list"]

    return dir_list


def get_test_lot_from_dirname(field_name: str):
    """
    获取测试轮次
    将指定字段（测试轮次的路径名 dirname）按“/"划分并取最后一项，作为test_lot的值
    输入： 指定字段名字
    返回： mongodb aggregation query中的一部分， 和其他语句配合使用
    """
    aggr_query = {"$project": {
        TEST_LOT_FIELD:
            {
                '$toUpper': {
                    '$arrayElemAt': [
                        {
                            '$split': [
                                "$" + field_name, '/'
                            ]
                        }, -1
                    ]
                }
            }}
    }

    return aggr_query


def add_if_ft():
    """ 
    添加常用字段 - 是否为初测
    适用所有工厂
    输出 aggregate中的一段 pipline
    """
    query = {
        "$addFields": {
            'if_FT': {
                '$cond': {
                    'if': {
                        '$or': [
                            {
                                '$and': [
                                    regexMatch('$filename', ".*FT.*")
                                    , {
                                        '$not': regexMatch('$filename', ".*R0.*")
                                    },
                                    {
                                        '$not': regexMatch('$filename', ".*FT1_R.*")
                                    }
                                ]
                            }, regexMatch('$filename', ".*FT1_R0.*")
                        ]
                    },
                    'then': True,
                    'else': False
                }
            }
        }
    }
    return query


def add_if_ft_rt():
    """
    添加常用字段 - 是否为初测或复测
    适用所有工厂
    输出 aggregate中的一段 pipline
    """
    query = {
        "$addFields": {
            'if_FT_FR': {
                '$cond': {
                    'if': {
                        '$or': [
                            regexMatch('$filename', ".*_QA.*")
                            ,
                            regexMatch('$filename', ".*QC.*")
                        ]
                    },
                    'then': False,
                    'else': True
                }
            }
        }
    }
    return query


def add_if_class_2():
    """
    添加常用字段 - 是否为二级品
    适用所有工厂
    输出 aggregate中的一段 pipline
    """
    query = {
        "$addFields": {
            'if_level_2': {
                '$cond': {
                    'if': {
                        '$or': [
                            regexMatch('$filename', ".*ESP32-D0WD.*")
                            ,
                            regexMatch('$filename', ".*ESP32-D2WD.*")
                            ,
                            regexMatch('$filename', ".*ESP32-U4WD.*")
                            ,
                            regexMatch('$filename', ".*XM240.*")
                            ,
                            regexMatch('$filename', ".*ESP32-PICO.*")
                        ]
                    },
                    'then': True,
                    'else': False
                }
            }
        }
    }
    return query


def add_bin1_chips():
    """
    添加常用字段 - 得到bin1的芯片数量（SBIN_CNT）
    适用所有工厂
    输出 aggregate中的一段 pipline
    """
    query = {
        "$addFields": {
            'bin1': {
                '$cond': {
                    'if': {
                        '$and': [
                            {
                                '$eq': [
                                    '$sbr.HEAD_NUM', 255
                                ]
                            }, {
                                '$eq': [
                                    '$sbr.SBIN_NUM', 1
                                ]
                            }
                        ]
                    },
                    'then': '$sbr.SBIN_CNT',
                    'else': 0
                }
            }
        }
    }
    return query


def add_chips_num_for_class2():
    """
    添加常用字段 - 得到二级品的芯片数量
    适用所有工厂
    输出 aggregate中的一段 pipline
    """
    query = {
        "$addFields": {
            'sbin_cnt_for_level_2': {
                '$cond': {
                    'if': {
                        '$and': [
                            {
                                '$eq': [
                                    '$sbr.HEAD_NUM', 255
                                ]
                            }, {
                                '$or': [
                                    {
                                        '$eq': [
                                            {
                                                '$toUpper': '$sbr.SBIN_NAM'
                                            }, 'CPU_240M_FAIL'
                                        ]
                                    }, {
                                        '$eq': [
                                            {
                                                '$toUpper': '$sbr.SBIN_NAM'
                                            }, 'CLASS_B'
                                        ]
                                    }
                                ]
                            }, {
                                '$eq': [
                                    '$if_level_2', True
                                ]
                            }
                        ]
                    },
                    'then': '$sbr.SBIN_CNT',
                    'else': 0
                }
            }
        }
    }
    return query


def get_mpn(test_round_info):
    """
    mongodb query语句中取 mpn 的逻辑
    test_round_info： mongodb中单个document (test_round) 装入一个AteLotInfo python 类的一个实例
    -- 即 test_round_info = mongoConnect.AteLotInfo(test_round)

    不同的工厂有不同的对filename的拆解规则， 但只跟两个因素有关： split_with （用“-”还是“_” 拆分）， 位置 （拆分后取哪里的值）；
    用法：
    返回 split_with & query_for_mpn
    """
    logger = mongoConnect.Logger(logger="utilities-get_mpn")
    logger.set_level_debug()

    split_with = ""
    query_for_mpn = ""
    # 根据观察四个工厂的命名规则在  取MPN时  一致
    if test_round_info.get_factory() in ["JCET", "ASE", "FOREHOPE", "JCET_SIP"]:
        split_with = '_'
        query_for_mpn = {
            '$arrayElemAt': [
                '$filename2', 2
            ]}
        print("------use mongodb query 1 for " + test_round_info.get_factory())

    # HT 工厂有两种情况
    elif test_round_info.get_factory() == "HT":

        if test_round_info.filenameInfo.filename.startswith("SH688_"):
            split_with = "_"
            query_for_mpn = {
                '$arrayElemAt': [
                    '$filename2', 1
                ]
            }
        elif test_round_info.filenameInfo.filename.startswith("SH688-"):
            split_with = "-"
            query_for_mpn = {
                '$concat': [
                    {
                        '$arrayElemAt': [
                            '$filename2', 1
                        ]
                    }, '-', {
                        '$arrayElemAt': [
                            '$filename2', 2
                        ]
                    }
                ]
            }
        print("------use mongodb query for HT starts with 'SH688' ")

    elif (test_round_info.get_factory() == "UM") or (test_round_info.get_factory() == "UNISEM"):
        split_with = "-"

        if test_round_info.get_factory() == "UM":
            query_for_mpn = {'$concat': [
                {
                    '$arrayElemAt': [
                        '$filename2', -6
                    ]
                }, '-', {
                    '$arrayElemAt': [
                        '$filename2', -5
                    ]
                }
            ]}
            print("------use mongodb query3 for UM")
        # 如果 工厂是UNISEM
        else:
            if test_round_info.filenameInfo.filename.startswith("UTTT"):
                split_with = "_"
                query_for_mpn = {
                    '$arrayElemAt': [
                        '$filename2', 1
                    ]
                }
            else:
                query_for_mpn = {'$concat': [
                    {
                        '$arrayElemAt': [
                            '$filename2', -6
                        ]
                    }, '-', {
                        '$arrayElemAt': [
                            '$filename2', -5
                        ]
                    }, '-', {
                        '$arrayElemAt': [
                            '$filename2', -4
                        ]
                    }
                ]}
            print("------use mongodb query3 for UC")

    else:
        # 若以上情况都不是（基本不可能）， 则返回空list
        logger.error("get_query()中无法匹配到正确工厂，query 为空； dirname: " + test_round_info.dir_name)
        return split_with, []

    return split_with, query_for_mpn


def set_finish_time():
    """
    为不存在mrr的数据添加自己计算的 FINISH_T 的逻辑， 应用于所有用到FINISH_T的代码处
    """
    query = {
        '$set': {
            'mrr.FINISH_T': {
                '$ifNull': [
                    '$mrr.FINISH_T', {
                        '$add': [
                            '$mir.START_T', {
                                '$ceil': {
                                    '$divide': [
                                        '$chips_test_duration_ms', 1000
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        }
    }
    return query


def customized_filter(filter_factory, filter_mpn, filter_tester_no):
    """
    使用者 在运行各个脚本时，需要输入这两种参数，用于决定是否通过factory和mpn过滤
    默认为“ALL”字符串，则为不额外进行过滤
    filter_factory： 默认不输入 或 输入list []
    filter_mpn： 默认不输入 或 输入list []  filter_tester_no
    """
    or_list_for_mpn = []
    or_list_for_factory = []
    or_list_for_tester = []
    and_list = []

    if filter_factory != DEFAULT_FILTER:
        or_list_for_factory = customized_filter_helper(filter_factory, or_list_for_factory, "dirname")

    if filter_mpn != DEFAULT_FILTER:
        or_list_for_mpn = customized_filter_helper(filter_mpn, or_list_for_mpn, "filename")

    if filter_tester_no != DEFAULT_FILTER:
        or_list_for_tester = customized_filter_helper(filter_tester_no, or_list_for_tester, 'mir.NODE_NAM')

    for i in [or_list_for_factory, or_list_for_mpn, or_list_for_tester]:
        # 若非空
        if len(i) != 0:
            and_list.append({"$or": i})

    if len(and_list) != 0:
        query = {
            "$match": {
                "$and": and_list
            }
        }
    else:
        # 若两个参数都是默认值则说明使用者不过滤任何数据，则返回一个无效query
        query = {
            "$match": {
            }
        }

    return query


def customized_filter_helper(filters, or_list, filed_name: str):
    for i in filters:
        temp_query = {filed_name:
            {
                "$regex": ".*" + i + ".*",
                "$options": "i"
            }
        }
        or_list.append(temp_query)

    return or_list
