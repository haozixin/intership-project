from mongoConnect import AteDbAgent
import mongoConnect
import utilities


def check1(number: int):
    """
    目的： 用于帮助检查dirname路径结构（是不是所有的以‘/’划分后的第3项为工厂名？ 会不会有异常）
    number 为想要查看以“/”划分后的第几项
    代码将所有数据处理，并将这一项放入set, 可清晰的看到第n个'/'后有几种情况

    例子：
    此函数check1(2)运行后的结果：{'_id': 'check', 'factor_list': ['ase', 'jcsip', 'unisem', 'data']} ， 可以得到 dirname的 第2项并不都是 data
    也有工厂名
    """
    db = AteDbAgent()
    query = [
        {
            '$project': {
                'factor': {
                    '$arrayElemAt': [
                        {
                            '$split': [
                                '$dirname', '/'
                            ]
                        }, number
                    ]
                }
            }
        }, {
            '$group': {
                '_id': 'check',
                'factor_list': {
                    '$addToSet': '$factor'
                }
            }
        }
    ]
    data = db.basic.aggregate(query)
    for i in data:
        print(i)


def check1_1(number, value: str):
    """
    目的： 用于进一步查看从check1（）中发现的异常值所在的dirname值

    number： check1（）中查看的位置（第几个‘/’后面的值）
    value: 值（第number个‘/’后面的值）

    eg: 因为check1（2）得到的结果为 {'_id': 'check', 'factor_list': ['ase', 'jcsip', 'unisem', 'data']}
    位置2后面有异常值 ase， jcsip, unisem, 想查看这ase这个异常值原始数据的dirname是什么样的
    则--> number = 2; value = ase
    """
    db = AteDbAgent()
    query = [
        {
            '$project': {
                'dirname2': {
                    '$split': [
                        '$dirname', '/'
                    ]
                },
                "dirname": 1
            }
        }, {
            '$match': {
                # 第number个‘/’后面的值是value的数据， 展示出它的dirname
                'dirname2.' + str(number): value
            }
        },
        {
            "$project": {
                "dirname": 1
            }
        }
    ]
    a = db.basic.aggregate(query)
    for i in a:
        print(i)
    db.close()


def check_filename_info():
    """
    测试 FileNameInfo（）是否涵盖全了命名规则文件中的规则
    检查不符合规则的数据
    """
    db = AteDbAgent()
    all_single_test_round = db.basic.find()
    for single_test_round in all_single_test_round:
        filename = mongoConnect.AteLotInfo(single_test_round).filenameInfo

    db.close()


if __name__ == "__main__":
    check1(2)
    check1_1(2, "jcsip")

    # check_filename_info()
