///**
// * Created by copperfield @ 2021/3/9 14:12
// */
//
//import avro.shaded.com.google.common.collect.ImmutableSet;
//import com.google.common.collect.ImmutableMap;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.spark.SparkConf;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//import scala.Tuple2;
//
//import java.util.*;
//
//
//public class MoyvCleaner {
//
//  public static void main(String[] args) throws InterruptedException {
//    SparkConf conf = new SparkConf().setAppName("KafkaEtl").setMaster("local[2]");
//    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
//
////    ssc.checkpoint();  // enable WAL
//
//    Map<String, Object> kafkaParams = new HashMap<>();
//    kafkaParams.put("bootstrap.servers", "longzhou-lkv1.lz.dscc.99.com:5000,longzhou-lkv2.lz.dscc.99.com:5000,longzhou-lkv3.lz.dscc.99.com:5000");
//    kafkaParams.put("key.deserializer", StringDeserializer.class);
//    kafkaParams.put("value.deserializer", StringDeserializer.class);
//    kafkaParams.put("group.id", "Copperfield");
//    kafkaParams.put("auto.offset.reset", "earliest");
//    kafkaParams.put("enable.auto.commit", false);
//
//    Collection<String> topics = Arrays.asList("test20210302");
//
//    JavaInputDStream<ConsumerRecord<String, String>> stream =
//        KafkaUtils.createDirectStream(
//            jssc,
//            LocationStrategies.PreferConsistent(),
//            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//        );
//
//    JavaDStream<String> dStream = stream.map(ConsumerRecord::value);
//    dStream.foreachRDD(rdd -> {
//      SparkSession spark = SparkSession.builder().config(rdd.rdd().sparkContext().getConf()).getOrCreate();
//
//      Dataset<Row> ds = spark.read().json(rdd.rdd());
//      ds.show(10);
//    });
//
//    jssc.start();
//    jssc.awaitTermination();
//  }
//}
