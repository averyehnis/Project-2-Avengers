register BizThreeFourUDF.jar;

data = LOAD 'Sqoop/BizThree/part-m-00000' USING PigStorage('|') AS (ID:int, COUNTRY_NAME:chararray, COUNTRY_CODE:chararray, INDICATOR_NAME:chararray, INDICATOR_CODE:chararray, YEAR_1960:double, YEAR_1961:double, YEAR_1962:double, YEAR_1963:double, YEAR_1964:double, YEAR_1965:double, YEAR_1966:double, YEAR_1967:double, YEAR_1968:double, YEAR_1969:double, YEAR_1970:double, YEAR_1971:double, YEAR_1972:double, YEAR_1973:double, YEAR_1974:double, YEAR_1975:double, YEAR_1976:double, YEAR_1977:double, YEAR_1978:double, YEAR_1979:double, YEAR_1980:double, YEAR_1981:double, YEAR_1982:double, YEAR_1983:double, YEAR_1984:double, YEAR_1985:double, YEAR_1986:double, YEAR_1987:double, YEAR_1988:double, YEAR_1989:double, YEAR_1990:double, YEAR_1991:double, YEAR_1992:double, YEAR_1993:double, YEAR_1994:double, YEAR_1995:double, YEAR_1996:double, YEAR_1997:double, YEAR_1998:double, YEAR_1999:double, YEAR_2000:double, YEAR_2001:double, YEAR_2002:double, YEAR_2003:double, YEAR_2004:double, YEAR_2005:double, YEAR_2006:double, YEAR_2007:double, YEAR_2008:double, YEAR_2009:double, YEAR_2010:double, YEAR_2011:double, YEAR_2012:double, YEAR_2013:double, YEAR_2014:double, YEAR_2015:double, YEAR_2016:double);

data_biz_three = foreach data generate COUNTRY_NAME, INDICATOR_NAME, com.revature.BizThreeFour(*) AS VALUE;

data_biz_final = RANK data_biz_three;

STORE data_biz_final INTO 'hbase://BizThree' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('RESULT:COUNTRY, RESULT:INDICATOR, RESULT:VALUE');
