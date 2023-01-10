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
 * Created by copperfield @ 2020/8/18 14:45
 */
public class DarkDeathCleaner {
  public static final String INPUT_DATA_PATH = "hdfs://ice-test-851pm-m1:11000/user/bigdata_prod/oss/Others0575/logstore/QA_Pub";
  
  public static void main(String[] args) throws IOException {
  
    String dateStr = "";
    if (args.length > 0) {
      dateStr = args[0];
    }
  
    SparkSession spark = SparkSession.builder()
                             .master("yarn")
                             .appName("etl-spark-dark_game_cleaner-hive")
                             .enableHiveSupport()
                             .config("hive.exec.dynamic.partition.mode", "nonstrict")
                             .getOrCreate();
  
    Configuration conf = spark.sparkContext().hadoopConfiguration();
  
    Path path = new Path(INPUT_DATA_PATH);
    FileSystem fileSystem = path.getFileSystem(conf);
    Preconditions.checkArgument(fileSystem.exists(path), "Raw data files don't exist!");
  
    Dataset<Row> rawDs = spark.read().json(INPUT_DATA_PATH + "/" + dateStr + "/*");
    Dataset<Row> tableDs = rawDs.filter(col("eventName").equalTo("attribute"))
                               .withColumn("date", from_unixtime(col("eventCreatedAt").$div(1000), "yyyy-MM-dd"))
                               .withColumn("time", from_unixtime(col("eventCreatedAt").$div(1000), "yyyy-MM-dd HH:mm:ss"))
                               .select(col("trackingId").as("game_id"),
                                   col("eventPayload.login.uid").as("user_id"),
                                   col("eventPayload.attribute.core").as("core_points"),
                                   col("eventPayload.attribute.attack").as("attack_points"),
                                   col("eventPayload.attribute.defense").as("defense_points"),
                                   col("eventPayload.attribute.auciliary").as("auciliary_points"),
                                   col("eventPayload.deaths.deathsCount").as("deaths_count"),
                                   col("time").as("time_str"),
                                   col("date").as("date_str"));
  
    tableDs.write().mode(SaveMode.Append).format("orc").insertInto("game_analysis.dwd_dark_death");
    spark.sql("msck repair table game_analysis.dwd_dark_death");
  }
}