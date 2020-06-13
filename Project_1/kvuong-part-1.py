#To run:
# spark-submit --master yarn --num-executors 16 --executor-memory 1g --executor-cores 2 kvuong-part-1.py

# To get output files:
# hadoop fs -getmerge hbp_cvd_output hbp_cvd_output.tsv
# hadoop fs -getmerge cancer_output cancer_output.tsv
# hadoop fs -getmerge diabetes_output diabetes_output.tsv

#Used this command to run pyspark on random port
# pyspark --master yarn --conf spark.ui.port="$(shuf -i 10000-60000 -n 1)"

import pyspark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext(appName="project1")
sqlContext = SQLContext(sc)

#Cleaning data file
df = pd.read_csv('RegionalInterestByConditionOverTime.csv')
#clean dataset:
#clean dmc column in RegionalInterestByConditionOverTime csv file:
#remove rows with hyphen in dma column (eliminate all combination cities/areas)
no_multicities_df = df[~df.dma.str.contains('-')]
# print(no_multicities_df)

#split at the last space in string " " into a city column and state column
city_state_split_df= no_multicities_df['dma'].str.rsplit(' ', 1,expand=True)
# print(city_state_split_df)
#add city column and state column to original df at the beginning
df_sequence = [city_state_split_df, no_multicities_df]
data_split = pd.concat(df_sequence, axis=1)
#remove dma column
data_split = data_split.drop(columns=['dma'])
#rename city and state columns
data_split.columns.values[0] = 'city'
data_split.columns.values[1] = 'state_abbrev'
# print(data_split)

#write dataframe to csv file, to be read by spark
data_split.to_csv('clean_RegionalInterestByConditionOverTime.csv', index=False)
#need to add this file to hadoop using command: hadoop fs -put 'clean_RegionalInterestByConditionOverTime.csv' before running SparkSQL




#use sparksql to create data frames, run queries, and to later convert to rdd
healthsearch_df = sqlContext.read.csv("clean_RegionalInterestByConditionOverTime.csv", header=True)
# healthsearch_df = spark.read.format("csv").option("header", "true").load('clean_RegionalInterestByConditionOverTime.csv')
healthsearch_df.registerTempTable("healthsearch")

cdc_df = sqlContext.read.csv("500_Cities_CDC.csv", header=True)
# cdc_df = sqlContext.read.load("hdfs:///user/kvuong/500_Cities_CDC.csv", header=True)
cdc_df.registerTempTable("cdcdata")


#write query to get all places listed in table with the age-adjusted prevalence of high blood pressure, cancer, and diabetes
q1 = sqlContext.sql('SELECT StateAbbr, PlaceName, BPHIGH_AdjPrev, CANCER_AdjPrev, DIABETES_AdjPrev FROM cdcdata')
# q1.show()
q1.registerTempTable("prevalence")


#use back tick ` for selecting columns in healthsearch data to get values`
#write query to get all places listed in table with the % search for cardiovascular, cancer, diabetes within 4 years prior to CDC survey data of prevalence (used to get 3 difference values between each year):
#CDC cardiovascular data from 2015 = search data 2012-2015, CDC cancer data from 2016 = search data from 2013-2016, CDC diabetes data from 2016 = need search data from 2013-2016
q2 = sqlContext.sql('SELECT state_abbrev, city, `2012+cardiovascular`, `2013+cardiovascular`,`2014+cardiovascular`, `2015+cardiovascular`, `2013+cancer`, `2014+cancer`, `2015+cancer`, `2016+cancer`, `2013+diabetes`, `2014+diabetes`, `2015+diabetes`, `2016+diabetes` FROM healthsearch')
# q2.show()
q2.registerTempTable("related_searches")


#create table with high blood pressure prevalence and table with cardiovascular searches
q3 = sqlContext.sql('SELECT StateAbbr, PlaceName, BPHIGH_AdjPrev FROM prevalence')
# q3.show()
q3.registerTempTable("BP")

q4 = sqlContext.sql('SELECT state_abbrev, city, `2012+cardiovascular`, `2013+cardiovascular`, `2014+cardiovascular`, `2015+cardiovascular` FROM related_searches')
# q4.show()
q4.registerTempTable("CV")
#join tables were city=PlaceName AND StateAbbr=state_abbrev to get one table for high blood pressure prevalence and cardiovascular searches
q5 = sqlContext.sql('SELECT BP.StateAbbr, BP.PlaceName, BP.BPHIGH_AdjPrev, CV.`2012+cardiovascular`, CV.`2013+cardiovascular`, CV.`2014+cardiovascular`, CV.`2015+cardiovascular` FROM CV AS CV JOIN BP ON BP.StateAbbr=CV.state_abbrev WHERE BP.PlaceName=CV.city')


#convert to RDD
q5.rdd
#calculate difference between years for health searches %
rdd1 = q5.rdd.map(lambda x: (x[0], x[1], x[2], float(x[4]) - float(x[3]), float(x[5]) - float(x[4]), float(x[6]) - float(x[5])))
#calculate average difference between all years
avg_rdd1 = rdd1.map(lambda x: (x[0], x[1], x[2], (x[3] + x[4] + x[5])/3))
#sort by the highest average increase in search
sorted_rdd1 = avg_rdd1.sortBy(lambda x: x[3], ascending=False)
formatted_rdd1 = sorted_rdd1.map(lambda x: x[0] + '\t' + x[1] + '\t' + x[2] + '\t' + str(x[3]))

formatted_rdd1.saveAsTextFile("hbp_cvd_output")
#use commands at top to merge and get tsv file


#create table with cancer prevalence and table with cancer searches
q6 = sqlContext.sql('SELECT StateAbbr, PlaceName, CANCER_AdjPrev FROM prevalence')
# q3.show()
q6.registerTempTable("CanPrev")

q7 = sqlContext.sql('SELECT state_abbrev, city, `2013+cancer`, `2014+cancer`, `2015+cancer`, `2016+cancer` FROM related_searches')
# q4.show()
q7.registerTempTable("CanSearch")
#join tables were city=PlaceName AND StateAbbr=state_abbrev to get one table for cancer prevalence and cancer searches
q8 = sqlContext.sql('SELECT CanPrev.StateAbbr, CanPrev.PlaceName, CanPrev.CANCER_AdjPrev, CanSearch.`2013+cancer`, CanSearch.`2014+cancer`, CanSearch.`2015+cancer`, CanSearch.`2016+cancer` FROM CanSearch AS CanSearch JOIN CanPrev ON CanPrev.StateAbbr=CanSearch.state_abbrev WHERE CanPrev.PlaceName=CanSearch.city')

#convert to RDD
q8.rdd
#example from hw6: usefulness = useful_words.map(lambda x: (x[0], math.log(float(x[1][1])/useful_review_count) - math.log(float(x[1][0])/counts_all_review)))
#calculate difference between years for health searches %
rdd2 = q8.rdd.map(lambda x: (x[0], x[1], x[2], float(x[4]) - float(x[3]), float(x[5]) - float(x[4]), float(x[6]) - float(x[5])))
#calculate average difference between all years
avg_rdd2 = rdd2.map(lambda x: (x[0], x[1], x[2], (x[3] + x[4] + x[5])/3))
#sort by the highest average increase in search
sorted_rdd2 = avg_rdd2.sortBy(lambda x: x[3], ascending=False)
formatted_rdd2 = sorted_rdd2.map(lambda x: x[0] + '\t' + x[1] + '\t' + x[2] + '\t' + str(x[3]))

formatted_rdd2.saveAsTextFile("cancer_output")
#use command at top to merge and get tsv file


#create table with diabetes prevalence and table with diabetes searches
q9 = sqlContext.sql('SELECT StateAbbr, PlaceName, DIABETES_AdjPrev FROM prevalence')
# q3.show()
q9.registerTempTable("DiaPrev")

q10 = sqlContext.sql('SELECT state_abbrev, city, `2013+diabetes`, `2014+diabetes`, `2015+diabetes`, `2016+diabetes` FROM related_searches')
# q4.show()
q10.registerTempTable("DiaSearch")
#join tables were city=PlaceName AND StateAbbr=state_abbrev to get one table for diabetes prevalence and diabetes searches
q11 = sqlContext.sql('SELECT DiaPrev.StateAbbr, DiaPrev.PlaceName, DiaPrev.DIABETES_AdjPrev, DiaSearch.`2013+diabetes`, DiaSearch.`2014+diabetes`, DiaSearch.`2015+diabetes`, DiaSearch.`2016+diabetes` FROM DiaSearch AS DiaSearch JOIN DiaPrev ON DiaPrev.StateAbbr=DiaSearch.state_abbrev WHERE DiaPrev.PlaceName=DiaSearch.city')


#convert to RDD
q11.rdd
#calculate difference between years for health searches %
rdd3 = q11.rdd.map(lambda x: (x[0], x[1], x[2], float(x[4]) - float(x[3]), float(x[5]) - float(x[4]), float(x[6]) - float(x[5])))
#calculate average difference between all years
avg_rdd3 = rdd3.map(lambda x: (x[0], x[1], x[2], (x[3] + x[4] + x[5])/3))
#sort by the highest average increase in search
sorted_rdd3 = avg_rdd3.sortBy(lambda x: x[3], ascending=False)
formatted_rdd3 = sorted_rdd3.map(lambda x: x[0] + '\t' + x[1] + '\t' + x[2] + '\t' + str(x[3]))

formatted_rdd3.saveAsTextFile("diabetes_output")
#use command at top to merge and get tsv file
