import findspark
import os
findspark.init()
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
import unicodedata
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.functions import col

sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)

#Running comands
#open a new terminal and do these two steps: 
#1- cd Downloads/spark-2.1.0-bin-hadoop2.7/bin/
#2- ./spark-submit --jars /home/hana/Downloads/mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar /home/hana/Desktop/dataRead.py 
#Connect to the Database
dataframe_mysql = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost/moodle").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "mdl_quiz").option("user", "root").option("password", "hana").load()

dataframe_mysql.registerTempTable("quiz_grades")

sqlContext.sql("select name from quiz_grades").show()

#  finding the grade foreach Question 
ndf = sqlContext.sql("select grade/sumgrades from quiz_grades")
ndf.registerTempTable("weightQ")

df2 = sqlContext.sql("select name, grade, sumgrades from quiz_grades")
newlist= df2.collect()

namelist=[item[0] for item in newlist][0]
name=unicodedata.normalize('NFKD', namelist).encode('ascii','ignore')

grade = [item[1] for item in newlist][0]

sumgrade = [item[2] for item in newlist][0]

questionGrade = grade/sumgrade

# connect again to database to retrieve a new table (question_analysis)
dataframe2_mysql = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost/moodle").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "mdl_question_response_analysis ").option("user", "root").option("password", "hana").load()

dataframe2_mysql.registerTempTable("question_response_analysis")

#selecting credit and questionid
response = sqlContext.sql("select questionid, credit from question_response_analysis ")
#register the table
response.registerTempTable("response")

#total number of attempts for each question
attempts = sqlContext.sql("SELECT questionid,COUNT(*) as totalCount FROM response GROUP BY questionid ORDER BY questionid ASC")

#sqlContext.sql("select wrongAnswers.questionid, wrongAnswers.count, attempts.questionid, attempts.count, ndf.(grade / sumgrades) #from wrongAnswers , attempts, weightQ")

# attempts.join(wrongAnswers).select("count", "questionid", (attempts.questionid * wrongAnswers["count"]).alias("product"))

#number of wrong answers
wrongAAnswers= sqlContext.sql("select questionid from response where credit = 0")
wrongAAnswers.registerTempTable("wa")
wrongAnswers = sqlContext.sql("SELECT questionid,COUNT(*) as wrongCount FROM wa GROUP BY questionid ORDER BY questionid ASC")
#wlist = rightAnswers.collect()
#number of right answers
rightAnswers = sqlContext.sql("select questionid from response where credit = 1")
rightAnswers.registerTempTable("ra")
rightAnswers = sqlContext.sql("SELECT questionid,COUNT(*) as rightCount FROM ra GROUP BY questionid ORDER BY questionid ASC")
#rlist = rightAnswers.collect()
#rlist = unicodedata.normalize('NFKD', rightAnswers).encode('ascii','ignore')

#join the right answers, wrong answers and number of attempts.
first = rightAnswers.join(attempts, rightAnswers.questionid == attempts.questionid).drop(rightAnswers.questionid).sort(col("questionid").asc())
second = first.join(wrongAnswers, first.questionid == wrongAnswers.questionid).drop(first.questionid).sort(col("questionid").asc())


cols = second.columns
cols = cols[2:] + cols[:2]
second = second[cols]
'''
# start from one to exclude row, end at -1 to exclude alpha column
for x in range(0, len(cols)): 
    new_column_name = cols[x] + "x totalCount" # get good new column names
    df = second.withColumn(new_column_name, ((getattr(second, cols[1])*getattr(second, cols[x]))))

'''
#Find the % difficulty of each question based on wrong answers
new_column_name = cols[1] + "/" + cols[3] # get good new column names
df = second.withColumn("average", ((getattr(second, cols[1])/getattr(second, cols[3]))*questionGrade))

graphList=df.collect()

#plot the results for question difficulty level
questions = ["Q"+str(item[0]) for item in graphList]
difficultyLevel = [item[4] for item in graphList]
y_pos = np.arange(len(questions))
plt.bar(y_pos, difficultyLevel, align='center', alpha=0.5)
plt.xticks(y_pos, questions)
plt.ylabel('Difficulty Level by Average')
plt.xlabel("Questions")
plt.title(name)
plt.show()

#plotting the number of wrong answers out of total attempts 
wrong = [item[1] for item in graphList]
total= [item[3] for item in graphList]
y_pos = np.arange(len(questions))
plt.bar(y_pos, wrong, align='center', alpha=0.5)
plt.bar(y_pos, total, align='center', alpha=0.5)
plt.xticks(y_pos, questions)
plt.ylabel('Wrong Answers out of Total Attempts')
plt.xlabel("Questions")
plt.title(name)
plt.show()

#plotting the number of right answers out of total attempts 
right = [item[2] for item in graphList]
total= [item[3] for item in graphList]
y_pos = np.arange(len(questions))
plt.bar(y_pos, right, align='center', alpha=0.5)
plt.bar(y_pos, total, align='center', alpha=0.5)
plt.xticks(y_pos, questions)
plt.ylabel('Right Answers out of Total Attempts')
plt.xlabel("Questions")
plt.title(name)
plt.show()

# Connect to the database from terminal
# mysql -u root -p

