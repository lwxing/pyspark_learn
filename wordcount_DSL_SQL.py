"""
单词计数需求，使用DSL和SQL两种风格来实现
"""

from pyspark.sql import SparkSession
# 导入StructType对象
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd
from pyspark.sql import functions as F


if __name__ =="__main__":
    
    spark= SparkSession.builder.\
        appName("create df").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # TODO1:SQL风格处理，以RDD为基础做数据加载
    rdd = sc.textFile("hdfs://node1:8020/input/words.txt").\
        flatMap(lambda x:x.split(" ")).\
        map(lambda x:x[x])
    # 转换RDD到df
    df = rdd.toDF(["word"])
    # 注册df为表
    df.createTempView("words")
    # 使用sql语句处理df注册的表
    spark.sql("""
        SELECT word,COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC
        """).show()
    
    # TODO2: DSL风格处理，纯sparksql api做数据加载
    # df 当前只有一个列 叫做value
    df = spark.read.format("text").load("hdfs://node1:8020/input/words.txt")
    # df.select(F.explode(F.split(df["value"]," "))).show()
    # 通过withColumn方法 对一个列进行操作
    # 方法功能：对老列执行操作，返回一个全新列，如果列名一样就替换，不一样就拓展一个列
    df2 = df.withColumn("value",F.explode(F.split(df["value"]," ")))
    df2.groupBy("value").\
        count().\
        withcolumnRenamed("count","cnt").\
        orderBy('cnt',ascending=False).\
        show()
    
    
