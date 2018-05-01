val twitterRdd = sc.textFile("/user/dz1104/bdad-project/twitter_data/*.csv") 
val twitterRdd1 = twitterRdd.map(line => line.split(";", -1))
val twitterRdd2 = twitterRdd1.filter(array => array(1) != "" && array(1) != "date" && array(1).length == 16 && array(0)!= "" && array(3)!= "" && array(2)!= "" && array(4)!= "")
val twitterRdd3 = twitterRdd2.map(array => (array(1), array(0), array(3), array(2), array(4)))
val twitterRdd4 = twitterRdd3.filter(array => array._1.length > 10).map(array => (array._1.substring(0, 10), array._2, array._3, array._4, array._5))
val twitterRdd5 = twitterRdd4.map(array => (array._1.replace("-", ""), array._2, array._3, array._4, array._5))
val twitterRdd6 = twitterRdd5.filter(array => array._5 != null && array._5.length > 2).map(array => (array._1, array._2, array._3.toInt, array._4.toInt, array._5.toString.substring(1, array._5.length -1)))
val sortedTwitter = twitterRdd6.keyBy(a => a._1).groupByKey().sortByKey()




/*

spark-shell --packages com.databricks:spark-csv_2.10:1.5.0

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
import sqlContext._
import sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameNaFunctions

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val twitterRdd7 = twitterRdd6.map(array => Row(array._1, array._2, array._3, array._4, array._5))

val customSchema = StructType(Array(
    StructField("date", StringType, false),
    StructField("username", StringType, false),
    StructField("favorites", IntegerType, false),
    StructField("retweets", IntegerType, false),
    StructField("text", StringType, false)))

val twitterDF = sqlContext.createDataFrame(twitterRdd7, customSchema)
twitterDF.registerTempTable("Twitter")

val dropNull = twitterDF.na.drop("any", Seq("date", "username", "favorites", "retweets", "text"))

sqlContext.sql(""" SELECT * FROM Twitter WHERE retweets = 1 """)
*/


