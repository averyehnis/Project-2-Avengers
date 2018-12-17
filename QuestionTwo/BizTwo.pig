REGISTER /home/cloudera/p1workspace/UDFTest/target/QuestionTwoUdf.jar;

data = load 'Sqoop/BizTwo/part-m-00000' using PigStorage('|') AS (country_name:chararray, indicator_name:chararray, year_2000:chararray, year_2001:chararray,year_2002:chararray,year_2003:chararray,year_2004:chararray,year_2005:chararray,year_2006:chararray,year_2007:chararray,year_2008:chararray,year_2009:chararray,year_2010:chararray,year_2011:chararray,year_2012:chararray,year_2013:chararray,year_2014:chararray,year_2015:chararray,year_2016:chararray );

avg = foreach data generate country_name, indicator_name, com.revature.udf.UDFPig(year_2000,year_2001,year_2002,year_2003,year_2004,year_2005,year_2006,year_2007,year_2008,year_2009,year_2010,year_2011,year_2012,year_2013,year_2014,year_2015,year_2016) as averageV;

avg_append = foreach avg generate country_name,indicator_name,averageV;

final = RANK avg_append;

STORE final INTO 'hbase://BizTwo' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage ('result:country,result:description,result:average');

