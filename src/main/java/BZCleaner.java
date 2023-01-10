import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;

/**
 * Created by copperfield @ 2020/8/12 18:17
 */
public class BZCleaner {
  public static final String INPUT_DATA_PATH = "hdfs://ice-test-851pm-m1:11000/user/bigdata_prod/oss/GMS0013/streaming";
  
  public static void main(String[] args) throws IOException {
    
    String dateStr = "";
    if (args.length > 0) {
      dateStr = args[0];
    }
    
    SparkSession spark = SparkSession.builder()
        .master("yarn")
        .appName("etl-spark-bzcleaner-hive")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate();
    
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    
    Path path = new Path(INPUT_DATA_PATH);
    FileSystem fileSystem = path.getFileSystem(conf);
    Preconditions.checkArgument(fileSystem.exists(path), "Raw data files don't exist!");
    
    Dataset<Row> rawDs = spark.read().json(INPUT_DATA_PATH + "/" + dateStr + "/*");
    Dataset<Row> tableDs = rawDs.filter(col("event_name").equalTo("MAIN_SCENE"))
                               .withColumn("date", from_unixtime(col("event_created_at").$div(1000), "yyyy-MM-dd"))
                               .withColumn("time", from_unixtime(col("event_created_at").$div(1000), "yyyy-MM-dd HH:mm:ss"))
                               .select(col("tracking_id").as("game_id"),
                                       col("event_payload.userInfo.id").as("user_id"),
                                       col("event_payload.userInfo.name").as("user_name"),
                                       col("event_payload.userInfo.rank_lev").as("rank_level"),
                                       col("event_payload.userInfo.accLoginTime").as("login_duration"),
                                       col("time").as("time_str"),
                                       col("date").as("date_str"));
    
    tableDs.write().mode(SaveMode.Append).format("orc").insertInto("game_analysis.dwd_user_login");
    spark.sql("msck repair table game_analysis.dwd_user_login");
  }
}
