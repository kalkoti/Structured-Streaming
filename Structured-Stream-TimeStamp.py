
# coding: utf-8

# In[1]:


import findspark


# In[2]:


findspark.init('/home/kalyank/spark-2.2.1-bin-hadoop2.7')


# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window


# In[4]:


spark = SparkSession.builder.appName("Struct-Stream-TimeStamp").getOrCreate()


# In[5]:


lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9989).option("includeTimestamp","true").load()


# In[6]:


words = lines.select(
        explode(split(lines.value, ' ')).alias('word'),
        lines.timestamp
)


# In[7]:


#wordCounts = words.groupBy("word").count()
windowedCounts = words.groupBy(
    window(words.timestamp, "10 minutes", "5 minutes"),
    words.word
).count()


# In[13]:


query = windowedCounts.writeStream.outputMode("complete").format("console").option('truncate', 'false').start()


# In[14]:


query.status


# In[ ]:


query.awaitTermination()

