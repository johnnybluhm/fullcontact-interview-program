package com.fullcontact.interview
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object RecordFinder {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RecordFinder")

    val sc = new SparkContext(conf)


    //create 2 RDD's filled with data from files
    val queries_rdd = sc.textFile("./Queries.txt")
    val records_rdd = sc.textFile("./Records.txt")
    val empty_rdd = sc.emptyRDD[String]
    val joined_rdd = queries_rdd.union(records_rdd)


    println("##Get data Using collect")
    joined_rdd.collect().foreach(f=>{
      println(f)
    })






   /* val queries = Source.fromFile("./Queries.txt").getLines.toArray
    val records = Source.fromFile("./Records.txt").getLines.toArray*/
  }
}//main
