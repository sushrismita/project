package candidate_exercise

import java.io._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types

object employee {

    def connectionDetails(epoint:String,aKey:String,sKey:String) {
	
      	   val spark = SparkSession
                       .builder()
                       .appName("Connect IBM COS")
                       .master("local")
                       .getOrCreate()
	    
             var sc = new SparkContext()
      		
      	     import spark.implicits._
      	
	     var credentials = scala.collection.mutable.HashMap[String, String](
                                      "endPoint" -> epoint,
                                      "accessKey" -> aKey,
                                      "secretKey" -> sKey
                                     )
		
	    sc.hadoopConfiguration.set("fs.cos.servicename.endpoint",credentials("endPoint"))
	    sc.hadoopConfiguration.set("fs.cos.servicename.access.key",credentials("accessKey"))
            sc.hadoopConfiguration.set("fs.cos.servicename.secret.key",credentials("secretKey"))
		 
	}
	
     def loadFile(filePath:String) {
	  
	  
	    val spark = SparkSession
                        .builder()
                        .appName("Connect IBM COS")
                        .config("spark.master","local")
                        .getOrCreate()	
	   
            import spark.implicits._
     
	    var inputFileDf1 = spark 
		               .read
			       .format("csv")
			       .option("header", "true")
                               .option("inferSchema", "true")
			       .load("cos://candidate-exercise.myCos/emp-data.csv")
						
	    var inputFileDf2 = inputFileDf1.take(30)
		
	    inputFileDf2.foreach(println)
		
	    inputFileDf1.write
                    .format("jdbc")
                    .option("url", "jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50000/BLUDB")
                    .option("user", "xvh20836")
                    .option("password", "P@ssw1rd1234567")			  
                    .option("dbtable", "cos_project.candidate_exercise")
                    .save()

	    var dbTableDf1 = spark 
		        .read
			.format("jdbc")
                        .option("url", "jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50000/BLUDB")
                        .option("user", "xvh20836")
                        .option("password", "P@ssw1rd1234567")						
                        .option("dbtable", "cos_project.candidate_exercise")
                        .load()	
		
	    dbTableDf1.take(20).foreach(println)
		
	    dbTableDf1.createOrReplaceTempView("emp")
		
	    var problem1 = s"""select department,(total_female/total_male)f as gender_ratio
		               from (select department,sum(if(gender="Female",1,0)) as total_female, sum(if(gender="Male",1,0)) as total_male,count(*) as total_emp 
					   from emp group by department)x """
		
	    var deptGenderRatioDf = spark.sql(problem1)
		
	    deptGenderRatioDf.show()
		
	    var problem2 = s"""select department,avg(salary) as averageSalary 
					   from emp group by department """
		
	    var avgDeptSalaryDf = spark.sql(problem2)
		
	    avgDeptSalaryDf.show()
		
	    var problem3 = s"""select department,(total_male_salary/total_male) - (total_female_salary/total_female) as salary_gap from (
		select department,gender,sum(if(gender="Female",1,0)) as total_female, sum(if(gender="Female",salary,0)) as total_female_salary,
		sum(if(gender="Male",1,0)) as total_male,sum(if(gender="Female",salary,0)) as total_male_salary
		from emp)x """
		
	    var deptSalaryGapDf = spark.sql(problem3)
		
	    deptSalaryGapDf.show()
		
	    deptGenderRatioDf.repartition(3).write.format("parquet").mode("append").save("cos://candidate-exercise.myCos/deptGenderRatio.parquet")
	    avgDeptSalaryDf.repartition(3).write.format("parquet").mode("append").save("cos://candidate-exercise.myCos/avgDeptSalary.parquet")
	    deptSalaryGapDf.repartition(3).write.format("parquet").mode("append").save("cos://candidate-exercise.myCos/deptSalaryGap.parquet")	
		
		
	}
	
    def main(args: Array[String]) {
	
           val stdin = scala.io.StdIn

           val st = stdin.readLine.split(" ")
           val bucketName = st(0).trim
           val endpoint = st(1).trim
           val accessKey = st(2).trim
           val secretKey = st(3).trim
           val fpath = st(4).trim	
                
           connectionDetails(endpoint,accessKey,secretKey)	
		
	   loadFile(fpath)		
  	
	}
}
