"""
1.正常的单词进行单词计数；
2.特殊字符统计出现有多少个
"""

from pyspark import SparkConf,SparkContext
from pyspark.storagelevel import storageLevel
from operator import add
import re

if __name__ =="__main__":
    # 0.构建spark执行环境
    conf= SparkConf().setAppName("helloworld").\
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1.读取文件
    file_rdd = sc.textFile("../data/accumulator_broadcast_data.txt")
    # 特殊字符的list集合，注册成广播变量，节省内存和网络IO开销
    abnormal_char = [",",".","!","#","$","%"]
    broadcast = sc.broadcast(abnormal_char)
    # 注册一个累加器，用于对特殊字符出现的时候进行+1计数
    acmlt = sc.accumulator(0)

    # 2.过滤空行
    #字符串的.strip方法,可以去除 前后的空格,然后返回字符串本身
    # 如果是空行数据 返回None,在判断中None标记为False
    lines_rdd = file_rdd.filter(lambda line:line.strip())

    # 3.将前后空格去除
    data_rdd = lines_rdd.map(lambda x:x.strip())

    # 4.对字符串使用空格切分
    # 由于数据中 有的单词之间是多个空格进行分隔的,那么使用正则进行划分
    # \s+在正则中表示任意数量的空格
    words_rdd = data_rdd.flatMap(lambda x: re.split("\s+",x))

    def filter_func(data):
        """过滤单词，保留正常单词"""
        global acmlt
        abnormal_char = broadcast.value
        if data in abnormal_char:
            # 在过滤不要特殊字符的同时，对特殊字符执行+1计数，使用累加器计数确保结果的准确
            acmlt +=1
            return False
        else:
            return True

    # 5.过滤出来正常单词
    normal_words_rdd = words_rdd.filter(filter_func)

    # 6.正常单词的计数
    result_rdd = normal_words_rdd.map(lambda x:(x,1)).reduceByKey(add)

    print("正常单词计数结果：",result_rdd.collect())
    print("特殊字符数量：",acmlt)
