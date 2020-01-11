from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors
from datasketch import MinHash, MinHashLSH
from nltk import ngrams
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat
# from pyspark.sql.functions import col
import functools

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

def testtest(spark, sc):
  df = spark.read.format('xml').options(rowTag='article').load('./result/articles.xml')
  articles = df.select('_id',concat('chapters.chapter._VALUE'))
  # articles.show()
  articles_collection = articles.collect()
  print(len(articles_collection))

  # for i in range(0, len(articles_collection)-1):
  #   for j in range (i+1, len(articles_collection)):
  #     print(articles_collection[i][0], articles_collection[j][0])
  #
  # print(articles_collection[0][1])
  data = []
  for i in range(0, len(articles_collection)):
      if(articles_collection[i][1] != None and not articles_collection[i][1] ):
        data.append(articles_collection[i][1])
  # data.append(articles_collection[0][1])
  print(data)
  # mh = MinHashLSH(inputCol="concat(chapters.chapter._VALUE)", outputCol="hashes", numHashTables=5)
  # model = mh.fit(articles)
  #
  # # Feature Transformation
  # print("The hashed dataset where hashed values are stored in the column 'hashes':")
  # model.transform(articles).show()

  # data = ['minhash is a probabilistic data structure for estimating the similarity between datasets',
  #         'finhash dis fa frobabilistic fata ftructure for festimating the fimilarity fetween fatasets',
  #         'weights controls the relative importance between minizing false positive',
  #         'wfights cfntrols the rflative ifportance befween minizing fflse posftive',
  #         'cfntrols the rflative fflse ifportance befween minizing posftive wfights ',
  #         ]

  # Create an MinHashLSH index optimized for Jaccard threshold 0.5,
  # that accepts MinHash objects with 128 permutations functions
  lsh = MinHashLSH(threshold=0.1, num_perm=128)

  # Create MinHash objects
  minhashes = {}
  for c, i in enumerate(data):
      minhash = MinHash(num_perm=128)
      for d in ngrams(i, 3):
          minhash.update("".join(d).encode('utf-8'))
      lsh.insert(c, minhash)
      minhashes[c] = minhash

  for i in range(len(minhashes.keys())):
      result = lsh.query(minhashes[i])
      print("Candidates with Jaccard similarity > 0.5 for input", i, ":", result)
def calculate_distance():
    spark, sc = init_spark()
    testtest(spark, sc)