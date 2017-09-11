package practice.lab2

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._


object MadianOfMedian {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MediaOfMedian").setMaster("local[2]")
    val sc = new SparkContext(conf)
//  val data = Source.fromFile("/home/vara/Vasu/Spark/Practice/Examples/MedianOfMedian.txt") 
    
    val data = sc.textFile("/home/vara/Vasu/Spark/Practice/Examples/MedianOfMedian.txt",1)
    
    data.collect().foreach(println)
    val med = data.map(r=>r.split(" ")).map(s=>s.toList.sorted).foreach(println)
    /*.map(r=>r.toList)
    //.map(numbers => Vectors.dense(numbers.map(_.toInt)))
    .map(r=>r.sorted)
    .foreach(println)*/
    
  }
}