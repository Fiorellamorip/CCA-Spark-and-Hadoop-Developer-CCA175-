//Problem Scenario 46 : You have been given belwo list in scala (name,sex,cost) for each
//work done.
List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",
2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
//Now write a Spark program to load this list as an RDD and do the sum of cost for
//combination of name and sex (as key)

List(("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000));
val rdd1=sc.parallelize(List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)));
val rdd2=rdd1.map((x)=>((x._1,x._2),x._3))
rdd2.reduceByKey(_+_)


