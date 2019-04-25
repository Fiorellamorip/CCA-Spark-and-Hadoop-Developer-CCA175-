//Problem Scenario 65 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2);
val b = sc.parallelize(1 to a.count.tolnt, 2);
val c = a.zip(b)
//operation1
//Write a correct code snippet for operationl which will produce desired output, shown below.
//Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))
c.sortByKey(false)
