package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/** Compute the total amount spent per customer in some fake e-commerce data. **/

object PurchaseByCustomer extends App{

  def parseLine(line: String) ={
    val fields = line.split(",")
    val id = fields(0).toInt
    val amount = fields(2).toFloat
    (id,amount)
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","PurchaseByCustomer")
  val input = sc.textFile("../../data/customer-orders.csv")
  val mappedInput = input.map(parseLine)
  val totalByCustomer = mappedInput.reduceByKey(_+_)
  val result = totalByCustomer.collect()
  result.foreach(println)
}
