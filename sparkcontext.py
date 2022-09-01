from pyspark import SparkConf,SparkContext

if __name__ =="__main__":
    # 0.构建spark执行环境
    conf= SparkConf().setAppName("create rdd").\
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # sc对象的parallelize方法，可以将本地集合转换成RDD返回给你
    data = [1,2,3,4,5,6,7,8,9]
    # numSlice分区数
    rdd = sc.parallelize(data,numSlices=3)
    

    print(rdd.collect())
