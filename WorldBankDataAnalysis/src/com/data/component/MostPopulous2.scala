package com.data.component

import org.apache.spark.sql.SparkSession
import java.lang.Long

object MostPopulous2 {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: MostPopulous <Input-File> <Output-File>");
      System.exit(1);
    }
      val spark = SparkSession
				.builder
				.appName("MostPopulous")
				.getOrCreate()
				
			val data = spark.read.csv(args(0)).rdd
			
			val result = data.map { line => {
			  val tPopulation = line.getString(9)
			  var uPopNum = 0L
			  var uPoP = new StringBuilder()
			  if (tPopulation != null && !tPopulation.isEmpty && tPopulation.length()>0) {
			    uPoP.append(tPopulation.replaceAll(",", ""))
			    uPopNum = Long.parseLong(uPoP.toString())  
			  }  else {
			      uPopNum = 0L			      
			    }
			    (line.getString(0),uPopNum )		  
			 }
			}
      .groupByKey()
      //Group's By key India [22344,444455,....]
      .map(rec => {
        
        var per =(rec._2.last - (rec._2.head))/(rec._2.head)
        (rec._1,per)  
      })
			.sortBy(pair => pair._2,false)
			.take(10)

     println(result)
     spark.stop
  }
}