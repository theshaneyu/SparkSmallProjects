import pyspark
from pprint import pprint
import operator
print('------------------------')

class PM25Spark(object):
    def __init__(self):
        self.sc = pyspark.SparkContext()
        
    def computing(self):
        def toKeyValuePair(linelist):
            pair = list()
            valueList = list()
            pair.append(linelist[1]) # 地點當成key
            for item in linelist[3:]:
                try:
                    valueList.append(int(item)) # 每個值轉成整數append成list
                except:
                    valueList.append(0)
            pair.append([str(linelist), max(valueList)]) # [整串, max]當做value
            return tuple(pair)

        def findTop3(numList): # 會拿到很多[整串, max]
            dateMaxDict = dict()
            for item in numList:
                dateMaxDict[item[0]] = item[1]
            sortedDict = sorted(dateMaxDict.items(), key=operator.itemgetter(1), reverse=True)
            return sortedDict[:3]


        rdd = self.sc.textFile('./pm2.5Taiwan.csv')
        # split成list並過濾掉PM2.5以外的，並建立key value pair，順便把字串都轉成整數
        keyValueRdd = rdd.map(lambda line:line.split(',')) \
                        .filter(lambda linelist:linelist[2]=='PM2.5') \
                        .map(toKeyValuePair) \
                        .groupByKey().mapValues(findTop3).collect()
        # pprint(keyValueRdd)
        resultDict = dict()
        for item in keyValueRdd:
            for pair in item[1]:
                resultDict[str(pair[0])] = pair[1]
        pprint(sorted(resultDict.items(), key=operator.itemgetter(1), reverse=True)[:3])

    


if __name__ == '__main__':
    obj = PM25Spark()
    obj.computing()

# conf = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '3'), ('spark.cores.max', '3'), ('spark.driver.memory','8g')])
# sc.stop()
# sc = pyspark.SparkContext(conf=conf)