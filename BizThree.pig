/* Can include multi-line
	comments this way. */

-- Can do single line comments this way.

register BizThreeUDF.jar;

data = LOAD 'Sqoop/BizThree/part-m-00000' USING PigStorage('|') AS (ID:int, COUNTRY_NAME:chararray, COUNTRY_CODE:chararray, INDICATOR_NAME:chararray, INDICATOR_CODE:chararray, YEAR_1960:double, YEAR_1961:double, YEAR_1962:double, YEAR_1963:double, YEAR_1964:double, YEAR_1965:double, YEAR_1966:double, YEAR_1967:double, YEAR_1968:double, YEAR_1969:double, YEAR_1970:double, YEAR_1971:double, YEAR_1972:double, YEAR_1973:double, YEAR_1974:double, YEAR_1975:double, YEAR_1976:double, YEAR_1977:double, YEAR_1978:double, YEAR_1979:double, YEAR_1980:double, YEAR_1981:double, YEAR_1982:double, YEAR_1983:double, YEAR_1984:double, YEAR_1985:double, YEAR_1986:double, YEAR_1987:double, YEAR_1988:double, YEAR_1989:double, YEAR_1990:double, YEAR_1991:double, YEAR_1992:double, YEAR_1993:double, YEAR_1994:double, YEAR_1995:double, YEAR_1996:double, YEAR_1997:double, YEAR_1998:double, YEAR_1999:double, YEAR_2000:double, YEAR_2001:double, YEAR_2002:double, YEAR_2003:double, YEAR_2004:double, YEAR_2005:double, YEAR_2006:double, YEAR_2007:double, YEAR_2008:double, YEAR_2009:double, YEAR_2010:double, YEAR_2011:double, YEAR_2012:double, YEAR_2013:double, YEAR_2014:double, YEAR_2015:double, YEAR_2016:double);

data_biz_three = foreach data generate COUNTRY_NAME, com.revature.BizThree({values}) AS VALUE;

STORE data INTO 'hbase://BizThree' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('RESULT:Country_Name, RESULT:Country_Code, RESULT:Indicator_Name, RESULT:Indicator_Code, RESULT:1960, RESULT:1961, RESULT:1962, RESULT:1963, RESULT:1964, RESULT:1965, RESULT:1966, RESULT:1967, RESULT:1968, RESULT:1969, RESULT:1970, RESULT:1971, RESULT:1972, RESULT:1973, RESULT:1974, RESULT:1975, RESULT:1976, RESULT:1977, RESULT:1978, RESULT:1979, RESULT:1980, RESULT:1981, RESULT:1982, RESULT:1983, RESULT:1984, RESULT:1985, RESULT:1986, RESULT:1987, RESULT:1988, RESULT:1989, RESULT:1990, RESULT:1991, RESULT:1992, RESULT:1993, RESULT:1994, RESULT:1995, RESULT:1996, RESULT:1997, RESULT:1998, RESULT:1999, RESULT:2000, RESULT:2001, RESULT:2002, RESULT:2003, RESULT:2004, RESULT:2005, RESULT:2006, RESULT:2007, RESULT:2008, RESULT:2009, RESULT:2010, RESULT:2011, RESULT:2012, RESULT:2013, RESULT:2014, RESULT:2015, RESULT:2016');

--store data_filter_men into 'H2Pig/men/' using PigStorage(',');

--data_filter_women = filter data by indicator_code matches '.+[.]FE[.].+';

--store data_filter_women into 'H2Pig/women/' using PigStorage(',');l
