val file=spark.read.format("csv").option("header","true").load("ejercicios_spark/ejercicio1/data/user.csv")
val rdd1=file.rdd
rdd1.first
