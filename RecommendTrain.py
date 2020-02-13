# -*- coding: UTF-8 -*-
from pyspark.mllib.recommendation import ALS
from pyspark import SparkConf, SparkContext

def SetLogger( sc ):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)    

def SetPath(sc):
    global Path
    if sc.master[0:5]=="local" :
        Path="file:/home/zkpk/Desktop/Code/python/sparkRecommendation/"
    else:   
        Path="hdfs://master:9000/tmp/sparkRecommend/"
  
def CreateSparkContext():
    sparkConf = SparkConf()                                                       \
                         .setAppName("RecommendTrain")                         \
                         .set("spark.ui.showConsoleProgress", "false") 
    sc = SparkContext(conf = sparkConf)
    print ("master="+sc.master)    
    SetLogger(sc)
    SetPath(sc)
    return (sc)
    
  
def PrepareData(sc): 
    #----------------------1.建立用户评价数据-------------
    print("开始读取用户评分数�?..")
    rawUserData = sc.textFile(Path+"data/u.data")
    rawRatings = rawUserData.map(lambda line: line.split("\t")[:3] )
    ratingsRDD = rawRatings.map(lambda x: (x[0],x[1],x[2]))
    #----------------------2.显示数据项数-------------
    numRatings = ratingsRDD.count()
    numUsers = ratingsRDD.map(lambda x: x[0] ).distinct().count()
    numMovies = ratingsRDD.map(lambda x: x[1]).distinct().count() 
    print("共计：ratings: " + str(numRatings) +    
             " User:" + str(numUsers) +  
             " Movie:" +    str(numMovies))
    return(ratingsRDD)

def SaveModel(sc): 
    try:        
        model.save(sc,Path+"ALSmodel")
        print("已存�?Model 在ALSmodel")
    except Exception :
        print "Model已经存在,请先删除再存�?"        
    
if __name__ == "__main__":
    sc=CreateSparkContext()
    print("==========数据准备阶段===========")
    ratingsRDD = PrepareData(sc)
    print("==========训练阶段===============")
    print("开始ALS训练,参数rank=5,iterations=10, lambda=0.1");
    model = ALS.train(ratingsRDD, 5, 10, 0.1)
    print("========== 存储Model========== ==")
    SaveModel(sc)

