import sys
from pyspark.sql import SparkSession

if len(sys.argv) != 2:
    print "Invalid argument,Pls pass input file path"
    sys.exit(-1)

spark = SparkSession.builder.appName("countAlphbets").getOrCreate()
texts = spark.read.text(sys.argv[1]).rdd.map(lambda x:x[0])

chars = texts.flatMap(lambda x:list(x))\
            .filter(lambda x: x.isalnum())\
            .map(lambda x:(x,1))\
            .reduceByKey(lambda x,y:x+y)
for x in chars.collect():
    print x


