package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomer {
  
  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")   
    
    val input = sc.textFile("../../data/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)
    
    val totalByCustomerSorted = mappedInput.reduceByKey( (x,y) => x + y ).map(x => (x._2,x._1)).sortByKey(false)
    
    val results = totalByCustomerSorted.collect()
    
    // Print the results.
    for(row <- results){
      println(s"(${row._2} : ${row._1} )")
    }
  }
  
}

