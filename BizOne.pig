register /home/cloudera/Jars/GradRateUDF.jar;

data = LOAD '/user/cloudera/Sqoop/BizOne/part-m-00000' USING PigStorage('|') AS
(COUNTRY_NAME:chararray, COUNTRY_CODE:chararray, INDICATOR_NAME:chararray, INDICATOR_CODE:chararray, 
YEAR_1960:chararray, YEAR_1961:chararray, YEAR_1962:chararray, YEAR_1963:chararray,
YEAR_1964:chararray, YEAR_1965:chararray, YEAR_1966:chararray, YEAR_1967:chararray,
YEAR_1968:chararray, YEAR_1969:chararray, YEAR_1970:chararray, YEAR_1971:chararray,
YEAR_1972:chararray, YEAR_1973:chararray, YEAR_1974:chararray, YEAR_1975:chararray,
YEAR_1976:chararray, YEAR_1977:chararray, YEAR_1978:chararray, YEAR_1979:chararray,
YEAR_1980:chararray, YEAR_1981:chararray, YEAR_1982:chararray, YEAR_1983:chararray,
YEAR_1984:chararray, YEAR_1985:chararray, YEAR_1986:chararray, YEAR_1987:chararray,
YEAR_1988:chararray, YEAR_1989:chararray, YEAR_1990:chararray, YEAR_1991:chararray,
YEAR_1992:chararray, YEAR_1993:chararray, YEAR_1994:chararray, YEAR_1995:chararray,
YEAR_1996:chararray, YEAR_1997:chararray, YEAR_1998:chararray, YEAR_1999:chararray,
YEAR_2000:chararray, YEAR_2001:chararray, YEAR_2002:chararray, YEAR_2003:chararray,
YEAR_2004:chararray, YEAR_2005:chararray, YEAR_2006:chararray, YEAR_2007:chararray,
YEAR_2008:chararray, YEAR_2009:chararray, YEAR_2010:chararray, YEAR_2011:chararray,
YEAR_2012:chararray, YEAR_2013:chararray, YEAR_2014:chararray, YEAR_2015:chararray,
YEAR_2016:chararray);

data_q1 = foreach data generate COUNTRY_NAME, com.revature.UDF.GradRatePigUDF(COUNTRY_NAME, COUNTRY_CODE, INDICATOR_NAME, INDICATOR_CODE,
YEAR_1960,YEAR_1961,YEAR_1962,YEAR_1963,YEAR_1964,YEAR_1965,YEAR_1966,YEAR_1967,YEAR_1968,YEAR_1969,YEAR_1970,YEAR_1971,
YEAR_1972,YEAR_1973,YEAR_1974,YEAR_1975,YEAR_1976,YEAR_1977,YEAR_1978,YEAR_1979,YEAR_1980,YEAR_1981,YEAR_1982,YEAR_1983,
YEAR_1984,YEAR_1985,YEAR_1986,YEAR_1987,YEAR_1988,YEAR_1989,YEAR_1990,YEAR_1991,YEAR_1992,YEAR_1993,YEAR_1994,YEAR_1995,
YEAR_1996,YEAR_1997,YEAR_1998,YEAR_1999,YEAR_2000,YEAR_2001,YEAR_2002,YEAR_2003,YEAR_2004,YEAR_2005,YEAR_2006,YEAR_2007,
YEAR_2008,YEAR_2009,YEAR_2010,YEAR_2011,YEAR_2012,YEAR_2013,YEAR_2014,YEAR_2015,YEAR_2016) as VALUE;

filtered_final = filter data_q1 by (double)VALUE<30.0;

final = RANK filtered_final;

STORE final INTO 'hbase://BizOne' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage ('result:country,result:value');
