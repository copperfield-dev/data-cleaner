
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;

import java.util.concurrent.TimeoutException;


public class MoyvCleaner2 {
  
  public static void main(String[] args) throws TimeoutException {
    SparkSession spark = SparkSession
                           .builder()
                           .master("local[2]")
                           .appName("StructuredKafkaProcess")
//                           .enableHiveSupport()
                           .getOrCreate();
  
    Dataset<Row> df = spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "longzhou-lkv1.lz.dscc.99.com:5000,longzhou-lkv2.lz.dscc.99.com:5000,longzhou-lkv3.lz.dscc.99.com:5000")
      .option("subscribe", "log-ue4my")
      .load();
    
    Dataset<Row> value = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
//                                .map((MapFunction<Row, String>) row -> row.getString(1), Encoders.STRING());

//    value.printSchema();
//    Dataset<Row> value = df.selectExpr("CAST(value AS STRING)").writeStream().format("console").start();
    
    value.writeStream().format("console").start();
    
    
//    value.writeStream().foreachBatch(
//        new VoidFunction2<Dataset<String>, Long>() {
//          public void call(Dataset<String> dataset, Long batchId) {
//            // Transform and write batchDF
//            dataset.show();
//            dataset.printSchema();
//            dataset.write().save("/Users/copperfield/Desktop");
////            dataset.write().mode(SaveMode.Append).format("orc").insertInto("testKafka");
//          }
//        }
//    ).start();
  }
}
