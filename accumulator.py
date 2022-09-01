from pyspark import SparkConf,SparkContext
from pyspark.storagelevel import storageLevel

if __name__ =="__main__":
    # 0.构建spark执行环境
    conf= SparkConf().setAppName("helloworld").\
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)

    acmlt = sc.accumulator(0)
    
    def map_func(data):
        global acmlt
        acmlt +=1
        print(acmlt)
    rdd.map(map_func).collect()
    print(acmlt)
#最终打印结果为10
