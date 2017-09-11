package practice.lab2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.lang.Math

object Incidents {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Crime Incidents").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val input = "/home/vara/Vasu/spark-srini/DEV3600_LAB_DATA/data/sfpd.csv"
    val outputDir = "/home/vara/Vasu/spark-srini/DEV3600_LAB_DATA/data/output/"
    
    val spdf = sc.textFile(input,1).map(_.split(","))
    
//    println(spdf.count())
    //val totalCat = spdf.map(x=>(x(1),1)).reduceByKey((x,y)=>x+y).repartition(1)
    
    val tendInc = spdf.filter(x=>x(6).equalsIgnoreCase("TENDERLOIN")).count
    println("Number of incidents were there in the Tenderloin district: %s".format(tendInc))
    
  }
}