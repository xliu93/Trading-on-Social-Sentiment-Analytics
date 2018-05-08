package invAssistant

import com.cloudera.sparkts._
//import com.cloudera.sparkts.Frequency._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

object TimeSeriesUtils {
  def loadObservations(sqlCtx: SQLContext, ticker: String, raw_data: RDD[String]): DataFrame = {
    // First get date and price
    val stock_data = raw_data.map(_.split(",")).filter(
      _(0) != "Date").map( array => (
        array(0).substring(0,4).toInt, // year
        array(0).substring(5,7).toInt, // month
        array(0).substring(8,10).toInt, // day
        ticker,             // add column of ticker name 
        array(5).toDouble) // price
      )

    val rowRdd = stock_data.map{ tuple =>
      val dt = ZonedDateTime.of(tuple._1, tuple._2, tuple._3, 0,0,0,0,
        ZoneId.systemDefault())
      Row(Timestamp.from(dt.toInstant), tuple._4, tuple._5)
    }

    val fields = Seq(
      StructField("Timestamp", TimestampType, true),
      StructField("Ticker", StringType, false),
      StructField("AdjPrice", DoubleType, true)
    )
    
    val schema = StructType(fields)
    sqlCtx.createDataFrame(rowRdd, schema)
  }

  def computeStockReturn(sqlCtx: SQLContext, ticker_name: String, price_raw: RDD[String]): Unit = {
    
    val ticker_obs = loadObservations(sqlCtx, ticker_name, price_raw)

    /* Create a daily DateTimeIndex over the whole time period:
     * 2010-01-04 (first trading day in 2010)
     * 2018-03-31 (03-30 24:00 last trading day before 2018-04-01)
     */

    // Modified:
    // The DateTimeIndex should match the time series of stock price. 

    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2010-01-04T00:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2018-03-31T00:00:00"), zone),
      new BusinessDayFrequency(1))

    // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
    val tickerTSrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(
      dtIndex, ticker_obs, "Timestamp", "Ticker", "AdjPrice")

    //tickerTSrdd.cache()

    println("The Total number of records in Time Series of Stock Price: ")
    println(tickerTSrdd.count())

    val filled = tickerTSrdd.fill("linear")
    val return_daily = filled.returnRates()
    //println(return_daily.getClass)
    //return_daily.take(10).foreach(println)

    //return_daily
  }
}
