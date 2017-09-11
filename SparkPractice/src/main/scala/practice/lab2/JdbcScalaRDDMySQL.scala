package practice.lab2

import java.sql.{Connection,DriverManager,ResultSet,SQLException}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.rdd._
//import org.apache.spark.rdd.RDD

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row;

import java.util.Properties



object JdbcScalaRDDMySQL {

  def main(args: Array[String]): Unit = {
    
    val url = "jdbc:mysql://localhost:3306/itversity"
    val user = "vasu";
    val password = "Admin-11";
    val prop = new Properties()
    prop.put("user", "vasu")
    prop.put("password", "Admin-11")
    
    Class.forName("com.sql.jdbc.Driver").newInstance
    
    val conf = new SparkConf().setAppName("JDBC RDD").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)
    //select * from departments where department_id = ? and department_name = ?
    val rdd = new JdbcRDD(sc,() => DriverManager.getConnection(url, user, password) , "select * from departments limit ?, ?",
        3, 5, 1, r=>r.getString("department_id") + ", " + r.getString("department_name"))
        
    rdd.foreach(println)
    rdd.saveAsTextFile("/home/vara/Vasu/Spark/Practice/SparkJdbcSQL")
  }
  
}