package practice.lab2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object SimpleRakesh {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Rakesh Date Convert").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.parallelize(Array(("365327", "7777 COOPER RD", "Montgomery Care Center", "HAMILTON", "CINCINNATI ", "OH ", "45242 ", "03 ", "1004 ", "I ", "1034600024", "2017-05-08 21:26:15.600", "0", "1034600024"),
      ("165033", "815 E LOCUST ST", "Manorcare Health Services", "SCOTT", "DAVENPORT ", "IA ", "52803 ", "03 ", "1004 ", "I ", "1034600024", "2017-05-08 21:26:15.600", "0", "1034600024"),
      ("IL3309", "1607 VISA DR STE 2A", "Foot And Ankle Surgery Center", "MCLEAN", "NORMAL ", "IL ", "61761 ", "06 ", "1004 ", "I ", "1034600024", "2017-05-08 21:26:15.600", "0", "1034600024")))
      .toDF("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n")
      
      df.show()
    df.select(date_format(col("l"), "yyyy/MM/dd")).show

  }
}