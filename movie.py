"""
电影评分数据分析：
- 1.查询用户平均分
- 2.查询电影平均分
- 3.查询大于平均分的电影的数量
- 4.查询高分电影中(>3)打分次数最多的用户,并求出此人打的平均分
- 5.查询每个用户的平均打分,最低打分,最高打分
- 6.查询被评分超过100次的电影,的平均分排名TOP1
"""
import time
from pyspark.sql import SparkSession
# 导入StructType对象
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd
from pyspark.sql import functions as F


if __name__ =="__main__":
    spark= SparkSession.builder.\
        appName("create df").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()
    sc = spark.sparkContext

    # 1.读取数据集
    schema = StructType().\
        add("user_id",StringType(),nullable=True).\
        add("movie_id",IntegerType(),nullable=True).\
        add("rank",IntegerType(),nullable=True).\
        add("ts",StringType(),nullable=True)
    
    df = spark.read.format("csv").\
        option("seq","\t").\
        option("header","False").\
        option("encoding","utf-8").\
        schema(schema).\
        load("../data/sql/u.data")
    
    # 2.注册成一个临时表，方便sql处理
    df.createTempView("movie")
    # 需求1：用户平均分
    df.groupBy("user_id").\
        avg("rank").\
        withColumRenamed("avg(rank)","avg_rank").\
        withColumn("avg_rank",F.round("avg_rank",2)).\
        orderBy("avg_rank",ascending=False).\
        show()
    # 需求2：查询电影平均分
    spark.sql("""
        SELECT movie_id,ROUND(AVG(rank),2) AS avg_rank FROM movie GROUP BY movie_id ORDER BY avg_rank DESC
        """).show()

    # 需求3：查询大于平均分的电影的数量
    # print(df.select(F.avg(df['rank'])).first()['avg(rank)])
    print("大于平均分的电影数量：",df.where(df['rank']>df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

    # 需求4：查询高分电影(>3)中打分次数最多的用户，并求出此人打分的平均分
    # 先找出这个人
    user_id = df.where("rank>3").\
        groupby("user_id").\
        count().\
        withColumnRenamed("count","cnt").\
        orderBy("cnt",ascending = False).\
        limit(1).\
        first()['user_id']
    # 计算这个人打的平均分
    df.filter(df['user_id'] == user_id).\
        select(F.round(F.avg('rank'),2)).show()
    # select round(avg(rank),2) from biao where userid =450;

    # 需求5：查询每个用户的平均打分、最低打分、最高打分
    # select user_id ,min(rank) as min_rank,max(rank) as max_rank,avg(rank) as avg_rank from table group by user_id;
    df.groupBy("user_id").\
        agg(
            F.round(F.avg('rank'),2).alias("avg_rank"),
            F.min('rank').alias("min_rank"),
            F.max('rank').alias("max_rank"),
        ).show()


    # 需求6：查询被评分超过100次的电影的平均份 排名top10
    # select movie_id ,count(movie_id) as cnt,avg(rank) as avg_rank from table group by movie_id having cnt>100 order by avg_rank desc limit 10;
    df.groupBy("movie_id").\
        agg(
            F.count("movie_id").alias("cnt"),
            F.round(F.avg("rank"),2).alias("avg_rank")
        ).where("cnt>100").\
        orderBy("avg_rank",ascending =False).\
        limit(10).show()
    time.sleep(600)
    
    
