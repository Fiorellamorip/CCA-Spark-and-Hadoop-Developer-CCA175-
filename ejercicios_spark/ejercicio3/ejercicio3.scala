//Write a Spark program, which will join the data based on first and last name and save the joined results in following format,  
//first.Last.technology.salary
val tech=spark.read.format("csv").option("header", "true").load("/user/cloudera/escenario45/spark12/technology.txt");
val salary=spark.read.format("csv").option("header", "true").load("/user/cloudera/escenario45/spark12/salary.txt");
tech.registerTempTable("tech");
salary.registerTempTable("salary");


val sol=spark.sql("select tech.first, tech.last,tech.technology,salary.salary from tech JOIN salary ON tech.first=salary.first AND tech.last=salary.last");
import org.apache.spark.sql.SaveMode
sol.write.format("com.databricks.spark.csv").option("delimiter", ",").mode(SaveMode.Overwrite).save("outputCSV")
