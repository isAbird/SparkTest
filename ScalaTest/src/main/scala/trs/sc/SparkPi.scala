package trs.sc

/**
  * Created by Administrator on 2016/1/22.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.math.random

object SparkPi {
  def main(args:Array[String]){
    val conf=new SparkConf().setAppName("SparkPi").setMaster("local")
    val sc=new SparkContext(conf)
    var testFile=sc.textFile("G:\\task\\hive表.txt",3)
    println("num = " +  testFile.count())
    val num=500000;
    val numRdd=sc.parallelize(1 to num)
    val  count= numRdd.map{
      n=>{
        val x=random*2-1
        val y=random*2-1
        if(x*x+y*y<1)
          1
        else
          0
      }
    }.reduce(_+_)
    println("结果是："+4.0*count/num)
  }
}
