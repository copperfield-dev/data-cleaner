import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

/**
 * Created by copperfield @ 2020/8/18 14:45
 */
public class DarkCleaner {
  public static final String INPUT_DATA_PATH = "hdfs://ice-test-851pm-m1:11000/user/bigdata_prod/oss/Others0514/streaming";
  
  public static void main(String[] args) throws IOException {
  
    String dateStr = "";
    if (args.length > 0) {
      dateStr = args[0];
    }
  
    SparkSession spark = SparkSession.builder()
                             .master("yarn")
                             .appName("etl-spark-darkcleaner-hive")
                             .enableHiveSupport()
                             .config("hive.exec.dynamic.partition.mode", "nonstrict")
                             .getOrCreate();
  
    Configuration conf = spark.sparkContext().hadoopConfiguration();
  
    Path path = new Path(INPUT_DATA_PATH);
    FileSystem fileSystem = path.getFileSystem(conf);
    Preconditions.checkArgument(fileSystem.exists(path), "Raw data files don't exist!");
  
    Dataset<Row> rawDs = spark.read().json(INPUT_DATA_PATH + "/" + dateStr + "/*");
    Dataset<Row> tableDs = rawDs.filter(col("eventName").equalTo("userinfo"))
                               .withColumn("date", from_unixtime(col("eventPayload.simple.loginTime"), "yyyy-MM-dd"))
                               .withColumn("time", from_unixtime(col("eventPayload.simple.loginTime"), "yyyy-MM-dd HH:mm:ss"))
                               .withColumn("user_name", lit("NaN"))
                               .select(col("trackingId").as("game_id"),
                                   col("eventPayload.simple.uid").as("user_id"),
                                   col("user_name").as("user_name"),
                                   col("eventPayload.simple.level").as("rank_level"),
                                   col("eventCreatedAt").$minus(col("eventPayload.simple.loginTime").$times(1000)).as("login_duration"),
                                   col("time").as("time_str"),
                                   col("date").as("date_str"));
  
    tableDs.write().mode(SaveMode.Append).format("orc").insertInto("game_analysis.dwd_user_login");
    spark.sql("msck repair table game_analysis.dwd_user_login");
  }
}