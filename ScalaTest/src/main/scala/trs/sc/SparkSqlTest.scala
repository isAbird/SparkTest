package trs.sc

/**
  * Created by Administrator on 2016/1/22.
  */

import org.apache.spark.sql.types.{StructType, IntegerType, StructField, StringType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.api.java.function.Function;

//import org.apache.spark.sql.types.StructType;
//import org.apache.spark.sql.types._

object SparkSqlTesdef {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkSql").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val schame = StructType(
      Array(
        StructField("group", StringType, true),
        StructField("site", StringType, true),
        StructField("channel", StringType, true),
        StructField("ip", StringType, true),
        StructField("port", StringType, true),
        StructField("num", IntegerType, true)
      )
    )
    val rows = sc.textFile("G:\\task\\hive表.txt", 3)
      .map(_.split("\t"))
      .map(x => Row(x(0), x(1), x(2), x(3), x(4).toInt, x(5).toInt))
    val hybaseDf=sqlContext.createDataFrame(rows,schame)
    hybaseDf.registerTempTable("hybaseDf")
    val ports=sqlContext.sql("select distinct ip from hybaseDf")
    for(str<-ports.collect())
    println(str)
    while (true) {

    }
  }
}
