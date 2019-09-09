package com.data.component

import org.apache.spark.sql.SparkSession
import java.lang.Long

object UrbanPopulationCount {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: UrbanPopulationCount <Input-File> <Output-File>");
      System.exit(1);
    }
      val spark = SparkSession
				.builder
				.appName("UrbanPopulation")
				.getOrCreate()
				
			val data = spark.read.csv(args(0)).rdd
			
			val result = data.map { line => {
			  val uPopulation = line.getString(10)
			  var uPopNum = 0L
			  var uPoP = new StringBuilder()
			  if (uPopulation != null && !uPopulation.isEmpty && uPopulation.length()>0) {
			    uPoP.append(uPopulation.replaceAll(",", ""))
			    uPopNum = Long.parseLong(uPoP.toString())  
			  }  else {
			      uPopNum = 0L			      
			    }
			    (uPopNum, line.getString(0))		  
			 }
			}
			.sortByKey(false)
			.first

			spark.sparkContext.parallelize(Seq(result)).saveAsTextFile(args(1))
			
			spark.stop
  }
}