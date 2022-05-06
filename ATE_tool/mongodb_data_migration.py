from pymongo import MongoClient

import mongoConnect
from mongoConnect import Logger

FREE_DATABASE_NAME = "cepq_cal"
FREE_DB_ADDRESS = "mongodb://cepq_cal_admin:bONZ2hObNeb5@192.168.8.107/cepq_cal"
NEW_DB_ADDRESS = "mongodb://cepq_new_user:espressif123@192.168.8.107/cepq_new"
NEW_DATABASE_NAME = "cepq_new"
BASIC_DATA = "BasicData"
PART_DATA = "PartData"


class CalDb(object):
    def __init__(self):
        """
        initial AteDbAgent
        db_address:   mongodb://user:password@db1_server_ip
        """

        self.client = MongoClient(FREE_DB_ADDRESS)
        self.__db = self.client[FREE_DATABASE_NAME]
        # print(db.list_collection_names())
        self.basic = self.__db[BASIC_DATA]
        self.part = self.__db[PART_DATA]

        self.logger = Logger(logger="MongoDB(cepq_cal)")
        self.logger.set_level_debug()

    def close(self):
        """
        close database
        """
        self.client.close()


class NewDb(object):

    def __init__(self):
        """
        initial AteDbAgent
        db_address:   mongodb://user:password@db1_server_ip
        """

        self.client = MongoClient(NEW_DB_ADDRESS)
        self.__db = self.client[NEW_DATABASE_NAME]
        # print(db.list_collection_names())
        self.basic = self.__db[BASIC_DATA]
        self.part = self.__db[PART_DATA]

        self.logger = Logger(logger="MongoDB_newest")
        self.logger.set_level_debug()

    def close(self):
        """
        close database
        """
        self.client.close()

    def get_new_sbr(self):
        target_place = {
            'db': 'cepq_new',
            'coll': 'BasicData'
        }
        query = [
            {
                '$addFields': {
                    'HEAD_NUM': 0
                }
            }, {
                '$group': {
                    '_id': {
                        'id': '$basicDataId',
                        'SITE_NUM': '$siteNum',
                        'SBIN_NUM': '$softBin',
                        'HEAD_NUM': '$HEAD_NUM',
                        'SBIN_NAM': None,
                        'SBIN_PF': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$softBin', 1
                                    ]
                                },
                                'then': 'P',
                                'else': 'F'
                            }
                        }
                    },
                    'SBIN_CNT': {
                        '$sum': 1
                    }
                }
            }, {
                '$addFields': {
                    'SITE_NUM': '$_id.SITE_NUM',
                    'HEAD_NUM': '$_id.HEAD_NUM',
                    'SBIN_NUM': '$_id.SBIN_NUM',
                    'SBIN_NAM': None,
                    'SBIN_PF': '$_id.SBIN_PF'
                }
            }, {
                '$set': {
                    '_id': '$_id.id'
                }
            }, {
                '$group': {
                    '_id': {
                        'id': '$_id',
                        'SBIN_NUM': '$SBIN_NUM',
                        'SBIN_PF': '$SBIN_PF'
                    },
                    'new_sbr': {
                        '$addToSet': {
                            'SBIN_CNT': '$SBIN_CNT',
                            'SITE_NUM': '$SITE_NUM',
                            'HEAD_NUM': '$HEAD_NUM',
                            'SBIN_NUM': '$SBIN_NUM',
                            'SBIN_NAM': None,
                            'SBIN_PF': '$SBIN_PF'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'statistics': {
                        'SBIN_NAM': None,
                        'HEAD_NUM': 255,
                        'SITE_NUM': 0,
                        'SBIN_CNT': {
                            '$sum': '$new_sbr.SBIN_CNT'
                        },
                        'SBIN_NUM': '$_id.SBIN_NUM',
                        'SBIN_PF': '$_id.SBIN_PF'
                    }
                }
            }, {
                '$set': {
                    '_id': '$_id.id',
                    'temp': {
                        '$concatArrays': [
                            '$new_sbr', [
                                '$statistics'
                            ]
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': '$_id',
                    'new_sbr': {
                        '$addToSet': '$temp'
                    }
                }
            }, {
                '$unwind': {
                    'path': '$new_sbr'
                }
            }, {
                '$unwind': {
                    'path': '$new_sbr'
                }
            }, {
                '$group': {
                    '_id': '$_id',
                    'new_sbr': {
                        '$addToSet': '$new_sbr'
                    }
                }
            }
            , {
                '$merge': {
                    'into': target_place,
                    'on': '_id',
                    'whenNotMatched': 'discard'
                }
            }
        ]
        print("start processing sbr")
        self.part.aggregate(query, allowDiskUse=True)


    def get_new_hbr(self):
        target_place = {
            'db': 'cepq_new',
            'coll': 'BasicData'
        }

        query = [
            {
                '$addFields': {
                    'HEAD_NUM': 0
                }
            }, {
                '$group': {
                    '_id': {
                        'id': '$basicDataId',
                        'SITE_NUM': '$siteNum',
                        'HBIN_NUM': '$hardBin',
                        'HEAD_NUM': '$HEAD_NUM',
                        'HBIN_NAM': None,
                        'HBIN_PF': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$hardBin', 1
                                    ]
                                },
                                'then': 'P',
                                'else': 'F'
                            }
                        }
                    },
                    'HBIN_CNT': {
                        '$sum': 1
                    }
                }
            }, {
                '$addFields': {
                    'SITE_NUM': '$_id.SITE_NUM',
                    'HEAD_NUM': '$_id.HEAD_NUM',
                    'HBIN_NUM': '$_id.HBIN_NUM',
                    'HBIN_NAM': None,
                    'HBIN_PF': '$_id.HBIN_PF'
                }
            }, {
                '$set': {
                    '_id': '$_id.id'
                }
            }, {
                '$group': {
                    '_id': {
                        'id': '$_id',
                        'HBIN_NUM': '$HBIN_NUM',
                        'HBIN_PF': '$HBIN_PF'
                    },
                    'new_hbr': {
                        '$addToSet': {
                            'HBIN_CNT': '$HBIN_CNT',
                            'SITE_NUM': '$SITE_NUM',
                            'HEAD_NUM': '$HEAD_NUM',
                            'HBIN_NUM': '$HBIN_NUM',
                            'HBIN_NAM': None,
                            'HBIN_PF': '$HBIN_PF'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'statistics': {
                        'HBIN_NAM': None,
                        'HEAD_NUM': 255,
                        'SITE_NUM': 0,
                        'HBIN_CNT': {
                            '$sum': '$new_hbr.HBIN_CNT'
                        },
                        'HBIN_NUM': '$_id.HBIN_NUM',
                        'HBIN_PF': '$_id.HBIN_PF'
                    }
                }
            }, {
                '$set': {
                    '_id': '$_id.id',
                    'temp': {
                        '$concatArrays': [
                            '$new_hbr', [
                                '$statistics'
                            ]
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': '$_id',
                    'new_hbr': {
                        '$addToSet': '$temp'
                    }
                }
            }, {
                '$unwind': {
                    'path': '$new_hbr'
                }
            }, {
                '$unwind': {
                    'path': '$new_hbr'
                }
            }, {
                '$group': {
                    '_id': '$_id',
                    'new_hbr': {
                        '$addToSet': '$new_hbr'
                    }
                }
            }
            , {
                '$merge': {
                    'into': target_place,
                    'on': '_id',
                    'whenNotMatched': 'discard'
                }
            }
        ]
        # partData的副本collection里面运行
        print("start processing hbr")
        self.part.aggregate(query, allowDiskUse=True)

        # print("------------")
        # for i in data_hbr:
        #     print(i)
        # return data_hbr

    def write_new_FINISH_T(self):
        # basicData
        target_collection_name = 'BasicData'
        chips_test_duration = 'chips_test_duration_ms'
        query_for_part = [
            {
                '$group': {
                    '_id': '$basicDataId',
                    chips_test_duration: {
                        '$sum': {
                            '$add': [
                                '$testT', 1000
                            ]
                        }
                    }
                }
            }, {
                '$merge': {
                    'into': target_collection_name,
                    'on': '_id',
                    'whenNotMatched': 'discard'
                }
            }
        ]

        self.part.aggregate(query_for_part)

# 下面的逻辑可放到需要提取 FINISH_T的代码中，可以避免改变原始数据太多
# 结束时间 =  开始时间+（每颗芯片测试时间+1秒）× 对应芯片数量
        # query_for_basic = [
        #     {
        #         '$set': {
        #             'mrr.FINISH_T': {
        #                 '$ifNull': [
        #                     '$mrr.FINISH_T', {
        #                         '$add': [
        #                             '$mir.START_T', {
        #                                 '$ceil': {
        #                                     '$divide': [
        #                                         '$' + chips_test_duration, 1000
        #                                     ]
        #                                 }
        #                             }
        #                         ]
        #                     }
        #                 ]
        #             }
        #         }
        #     },
        #     # {
        #     #     '$out': target_collection_name
        #     # }
        # ]
        #
        # self.basic.aggregate(query_for_basic)

def get_new_sbr_hbr():
    # 将原数据迁移到有写入权限的数据库中
    new_db = NewDb()
    free_db = CalDb()

    new_db.get_new_hbr()
    new_db.get_new_sbr()

    new_db.close()
    free_db.close()



if __name__ == "__main__":
    get_new_sbr_hbr()