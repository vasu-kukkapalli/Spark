package practice.lab2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

object IncidentsDF {
    def main(args: Array[String]): Unit = {
  
      val conf = new SparkConf().setAppName("Crime Incidents DF").setMaster("local[*]").set("spark.hadoop.validateOutputSpecs", "false")
      val sc = new SparkContext(conf)
      
      val sqlContext = new SQLContext(sc)
      val input = "/home/vara/Vasu/spark-srini/DEV3600_LAB_DATA/data/sfpd.json"
      val outputDir = "/home/vara/Vasu/spark-srini/DEV3600_LAB_DATA/data/output/"
    
    val spdfDF = sqlContext.jsonFile(input)
    
//    spdfDF.printSchema
    spdfDF.select("Category").distinct.foreach(println)
    
    //spdfDF.describe(_)
    
//    println("Number of incidents were there in the Tenderloin district: %s".format(tendInc))
    
  }
}  