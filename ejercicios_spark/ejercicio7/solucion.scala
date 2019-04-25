//Write a Spark program, which will join the data based on first and last name and save the
//joined results in following format, first Last.technology.salary
val tecLeidos= sc.textFile("/cloudera/ejercicios_spark/escenario45/technology.txt");
val tecRDD=tecLeidos.map((x)=>{var aux=x.split(','); ((aux(0),aux(1)),aux(2))});
val salLeidos= sc.textFile("/cloudera/ejercicios_spark/escenario45/salary.txt");
val salRDD=salLeidos.map((x)=>{var aux=x.split(','); ((aux(0),aux(1)),aux(2))});
val joined=tecRDD.join(salRDD);
val textSalida=joined.map((x)=>x._1._1+" "+x._1._2+"."+x._2._1+"."+x._2._2);
textSalida.repartition(1).saveAsTextFile("/cloudera/ejercicios_spark/escenario45/out3/sal.txt")


//forma 2
//Explanation: Solution : Step 1 : Create 2 files first using Hue in hdfs. Step 2 : Load all file as an RDD 
val technology = sc.textFile("/cloudera/ejercicios_spark/escenario45/technology.txt").map(e => e.split(",")); 
val salary = sc.textFile("/cloudera/ejercicios_spark/escenario45/salary.txt").map(e => e.split(".")); 
val joined = technology.map(e=>((e(0),e(1)),e(2))).join(salary.map(e=>((e(0),e(1)),e(2)))); joined.repartition(1).saveAsTextFile("/cloudera/ejercicios_spark/escenario45/out8/multiColumnJoined.txt")

//con sparksql:
val tecLeidos= sc.textFile("/cloudera/ejercicios_spark/escenario45/technology.txt");
val tecRDD=tecLeidos.map((x)=>{var aux=x.split(','); (aux(0),aux(1),aux(2))})
val tecDF=tecRDD.toDF("first","last","technology")

val salLeidos= sc.textFile("/cloudera/ejercicios_spark/escenario45/salary.txt");
val salRDD=salLeidos.map((x)=>{var aux=x.split(','); (aux(0),aux(1),aux(2))});
val salDF=salRDD.toDF("first","last","salary");
salDF.registerTempTable("sal")
val resQuery=sqlContext.sql("select tec.first,tec.last,tec.technology,sal.salary from tec JOIN sal ON tec.first=sal.first AND tec.last=sal.last")
val textContent=resQuery.map((x)=>(x(0)+" "+x(1)+"."+x(2)+"."+x(3)))
joined.repartition(1).saveAsTextFile("spark12/multiColumn Joined.txt")