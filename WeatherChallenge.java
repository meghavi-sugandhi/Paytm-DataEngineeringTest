package paytm.test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WeatherChallenge {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Test");
		sparkConf.setMaster("local[*]");
        //Step1 Setting up the Data
		//1. LoadData
        SparkSession spark = SparkSession.builder().config(sparkConf).appName("Paytm").getOrCreate();
        Dataset<Row> globalDataDf = spark.read().option("header","true").csv("C:\\Users\\Kay Gee\\Downloads\\Paytm_Input\\data\\2019");
        Dataset<Row> stationListDf = spark.read().option("header","true").csv("C:\\Users\\Kay Gee\\Downloads\\Paytm_Input\\StationData"); 
        Dataset<Row> countryListDf = spark.read().option("header","true").csv("C:\\Users\\Kay Gee\\Downloads\\Paytm_Input\\CountryListData");
        globalDataDf.show();
        stationListDf.show();
        countryListDf.show();
        
       // 2. Join the stationlist.csv with the countrylist.csv to get the full country name for each station number.
        Dataset<Row> fullCountryNamesdf=stationListDf.join(countryListDf, stationListDf.col("COUNTRY_ABBR").equalTo(countryListDf.col("COUNTRY_ABBR")));
        fullCountryNamesdf.show();  
        
        //
        Dataset<Row> globalwithFullCountrydf=globalDataDf.join(fullCountryNamesdf,globalDataDf.col("STN---").equalTo(fullCountryNamesdf.col("STN_NO")));
        globalwithFullCountrydf.show();
        
        
        //Step2.1: //Hottest Tempaerature  
        Dataset<AvgTemp> globalwithFullCountrydf1= globalwithFullCountrydf.select(("COUNTRY_FULL"),("TEMP")).as(Encoders.bean(AvgTemp.class));
        globalwithFullCountrydf1.groupBy(("COUNTRY_FULL"),("TEMP")).avg("TEMP").show();
       
        
        //2.3 average wind speed
        globalwithFullCountrydf.withColumn("WDSP", globalwithFullCountrydf.col("WDSP").cast("NumericType"));
        Dataset<Row> avgWindSpeed=globalwithFullCountrydf.groupBy(("COUNTRY_FULL"),("WDSP")).avg("WDSP");
        avgWindSpeed.show();       
	}

}
