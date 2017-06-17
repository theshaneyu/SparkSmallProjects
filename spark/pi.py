import random, pyspark
print('------------------------')
def sample(p):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y < 1 else 0

count = pyspark.SparkContext().parallelize(range(0, 100000)).map(sample) \
             .reduce(lambda a, b: a + b)
print("Pi is roughly %f" % (4.0 * count / 100000))