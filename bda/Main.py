from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat
import functools
import xml.etree.ElementTree as ET
import re
import os

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

# schema = StructType(
#     [
#         StructField('article_type', StringType()),
#         StructField('journal_title', StringType()),
#         StructField('article_title', StringType()),
#         StructField('authors', ArrayType(StringType()))
#     ]
# )
class Article(object):
  article_type = ''
  journal_title = ''
  article_categories = []
  article_title = ''
  authors = []
  chapters = []
  article_id = ''

  def __init__(self, article_type, journal_title, article_title, authors, article_categories, chapters, article_id):
    self.article_type = article_type
    self.journal_title = journal_title
    self.article_categories = article_categories
    self.article_title=article_title
    self.authors=authors
    self.chapters = chapters
    self.article_id = article_id

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def read_record(xmlns, record):
  article_id = record.find(xmlns + 'header/' + xmlns + 'identifier').text
  article = record.find(xmlns + 'metadata')[0]
  article_type = article.attrib['article-type']
  journal_title = ''
  article_categories = []
  article_title = ''
  authors = []
  chapters = []

  m = re.search('{.*}', article.tag)
  article_xmlns = m.group(0)

  front = article.find(article_xmlns + 'front')
  body = article.find(article_xmlns + 'body')

  journal_title = front.find(article_xmlns+'journal-meta/'+article_xmlns+'journal-title-group/'+article_xmlns+'journal-title').text

  article_meta = front.find(article_xmlns+'article-meta')

  article_categories_el = article_meta.find(article_xmlns+'article-categories')
  for el in article_categories_el:
    article_categories.append(el[0].text)

  article_title = article_meta.find(article_xmlns+'title-group/'+article_xmlns+'article-title').text
  contrib_group = article_meta.find(article_xmlns + 'contrib-group')

  if contrib_group is not None:
    for contrib in contrib_group:
      first_name = contrib.find(article_xmlns+'name/'+article_xmlns+'given-names')
      last_name = contrib.find(article_xmlns+'name/'+article_xmlns+'surname')
      if first_name != None and last_name != None:
        authors.append((first_name.text, last_name.text))

  body_content = body.findall(article_xmlns+'sec')
  for sec in body_content:
    title, content = extract_title_and_body(sec,article_xmlns)
    chapters.append((title, content))

  return Article(article_type, journal_title, article_title, authors, article_categories, chapters, article_id)

def extract_title_and_body(chapter, article_xmlns):
  title_tag = chapter.find(article_xmlns + 'title')
  title = ''
  if title_tag is not None:
    title = title_tag.text

  paragraphs = chapter.findall(article_xmlns + 'p')
  content = ''
  for p in paragraphs:
    if p.text != None:
      content = content + p.text
  inner_secvences = chapter.findall(article_xmlns + 'sec')
  for sec in inner_secvences:
    inner_title, inner_content = extract_title_and_body(sec, article_xmlns)
    content = content + inner_content
  return title, content

def read_xml(path):
  tree = ET.parse(path)
  root = tree.getroot()

  m = re.search('{.*}', root.tag)
  xmlns = m.group(0)

  records = root.findall(xmlns + "ListRecords/" + xmlns + "record")

  articles = []
  for record in records:
    articles.append(read_record(xmlns, record))
  return articles

def create_xml_file(articles):
  data = ET.Element('data')
  for article in articles:
    article_tag = ET.SubElement(data, 'article')
    article_tag.set('id', article.article_id)
    title_tag = ET.SubElement(article_tag,'title')
    title_tag.text = article.article_title
    type_tag = ET.SubElement(article_tag,'article-type')
    type_tag.text = article.article_type
    journal_title_tag = ET.SubElement(article_tag, 'journal-title')
    journal_title_tag.text = article.journal_title
    authors_tag = ET.SubElement(article_tag,'authors')
    for author in article.authors:
      author_tag = ET.SubElement(authors_tag,'author')
      first_name = ET.SubElement(author_tag,'first-name')
      last_name = ET.SubElement(author_tag, 'last-name')
      first_name.text = author[0]
      last_name.text = author[1]

    article_categories_tag = ET.SubElement(article_tag,'article-categories')
    for category in article.article_categories:
      category_tag = ET.SubElement(article_categories_tag, 'category')
      category_tag.text = category

    chapters_tag = ET.SubElement(article_tag,'chapters')
    for chapter in article.chapters:
      chapter_tag = ET.SubElement(chapters_tag, 'chapter')
      if chapter[0] is not None:
        chapter_tag.set('title',chapter[0])
      else:
        chapter_tag.set('title', 'None')
      chapter_tag.text = chapter[1]

  tree = ET.ElementTree(data)
  tree.write("./result/articles.xml")

def read_directory():
  files = []
  articles = []
  for r, d, f in os.walk('./xml/'):
    for file in f:
      if '.xml' in file:
        files.append(os.path.join(r, file))

  for file in files:
    articles.extend(read_xml(file))

  create_xml_file(articles)

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

def testtest(spark, sc):
  df = spark.read.format('xml').options(rowTag='article').load('./result/articles.xml')
  articles = df.select('_id',concat('chapters.chapter._VALUE'))
  articles.show()
  articles_collection = articles.collect()
  print(len(articles_collection))
  for i in range(0, len(articles_collection)-1):
    for j in range (i+1, len(articles_collection)):
      print(articles_collection[i][0], articles_collection[j][0])

def main():
  spark,sc = init_spark()
  testtest(spark, sc)
  # read_directory()

if __name__ == '__main__':
  main()
