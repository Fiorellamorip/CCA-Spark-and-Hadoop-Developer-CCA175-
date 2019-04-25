//3.-(Usando el ejercicio 5 de sqoop) Using Spark Scala load data at /user/cloudera/problem1/orders and ///user/cloudera/problem1/orders-items items as dataframes.
import com.databricks.spark.avro._
sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
val orderDF = sqlContext.read.avro("/user/cloudera/problem1/orders")
val ordersItems = sqlContext.read.avro("/user/cloudera/problem1/orders-items")

//4.- Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and //total amount per status per day. The result should be sorted by order date in descending, order status in ascending and total amount in //descending and total orders in ascending. Aggregation should be done using below methods. However, sorting can be done using a dataframe //or RDD. Perform aggregation in each of the following ways
orderDF.registerTempTable("order");
ordersItems.registerTempTable("ordersItems")

sqlContext.sql("select to_date(from_unixtime(order_date/1000)) as Order_Date,order_status,count(order_id) as total_orders,  sum(order_item_subtotal) as total_amount from order join ordersItems ON order.order_id=ordersItems.order_item_order_id group by Order_Date,order_status order by Order_Date DESC, order_status DESC,total_amount DESC, total_orders ASC")

//5.- Store the result as parquet file into hdfs using gzip compression under folder
///user/cloudera/problem1/result4a-gzip

sqlContext.setConf("spark.sql.avro.compression.codec", "gzip")
res2.write.parquet("/user/cloudera/problem1/result4a-gzip")


//6.- Store the result as parquet file into hdfs using snappy compression under folder
///user/cloudera/problem1/result4a-snappy
sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
res2.write.parquet("/user/cloudera/problem1/result4a-snappy")

//7.- Store the result as CSV file into hdfs using No compression under folder
///user/cloudera/problem1/result4a-csv
res2.map(x=>x(0)+","+x(1)+","+x(2)+","+x(3)).saveAsTextFile("/user/cloudera/problem1/result4a-csv")


//create a mysql table named result and load data from /user/`whoami`/problem1/result4a-csv to mysql table named result
create table result(order_date datetime,order_status varchar(45) ,total_orders int(11), total_amount float);
sqoop export --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera \
--table result --input-fields-terminated-by ',' --input-lines-terminated-by '\n' --export-dir /user/cloudera/problem1/result4a-csv -m 12 

