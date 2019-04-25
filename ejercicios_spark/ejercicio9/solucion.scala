//Problem Scenario 57 : You have been given below code snippet.
val a = sc.parallelize(1 to 9, 3) operationl
//Write a correct code snippet for operationl which will produce desired output, shown below.
//Array[(String, Seq[lnt])] = Array((even,ArrayBuffer(2, 4, G, 8)), (odd,ArrayBuffer(1, 3, 5, 7,9)))
a.groupBy(x => {if (x % 2 == 0) "even" else "odd" }).collect

