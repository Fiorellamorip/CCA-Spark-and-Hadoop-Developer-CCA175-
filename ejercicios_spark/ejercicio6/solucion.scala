//Problem Scenario 91 : You have been given data in json format as below.
//{"first_name":"Ankit", "last_name":"Jain"}
//{"first_name":"Amir", "last_name":"Khan"}
//{"first_name":"Rajesh", "last_name":"Khanna"}
//{"first_name":"Priynka", "last_name":"Chopra"}
//{"first_name":"Kareena", "last_name":"Kapoor"}
//{"first_name":"Lokesh", "last_name":"Yadav"}

//Do the following activity
//1. create employee.json tile locally.
//sudo vi data_esc91.json
//sudo mv data_esc91.json employee.json

//2. Load this tile on hdfs
//hdfs dfs -put employee.json /cloudera/ejercicios_spark/escenario91
//3. Register this data as a temp table.
val jsonLeido=sqlContext.read.json("/cloudera/ejercicios_spark/escenario91")
jsonLeidores2.registerTempTable("employee")
//4. Write select query and print this data.
val resultado=sqlContext.sql("select * from employee")
//5. Now save back this selected data in json format.
resultado.write.json("/cloudera/ejercicios_spark/escenario91/out")
