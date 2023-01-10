import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;

public class MyAccountCleaner {
    public static final String INPUT_DATA_PATH = "hdfs://192.168.19.25:11000/tmp/topics/myaccount8/partition=0";

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession.builder()
                .master("yarn")
                .appName("etl-spark-simple-hive-local")
                .config("hive.metastore.uris", "thrift://ice-test-851pm-m1:9083")
                .enableHiveSupport()
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .getOrCreate();

        Configuration conf = spark.sparkContext().hadoopConfiguration();

        Path path = new Path(INPUT_DATA_PATH);
        FileSystem fileSystem = path.getFileSystem(conf);
        Preconditions.checkArgument(fileSystem.exists(path), "Raw data files don't exist!");

        Dataset<Row> rawDs = spark.read().json(INPUT_DATA_PATH + "/*");
        spark.udf().register("toDate", new UDF1<Long, String>() {
            @Override
            public String call(Long aLong) throws Exception {
                return aLong.toString().substring(0, 4) + "-" + aLong.toString().substring(4, 6) + "-" + aLong.toString().substring(6, 8);
            }
        }, DataTypes.StringType);
        spark.udf().register("toTime", new UDF1<Long, String>() {
            @Override
            public String call(Long aLong) throws Exception {
                try {
                    return aLong.toString().substring(0, 4) + "-" + aLong.toString().substring(4, 6) + "-" + aLong.toString().substring(6, 8) + " " +
                            aLong.toString().substring(8, 10) + ":" + aLong.toString().substring(10, 12) + ":" + aLong.toString().substring(12, 14);
                } catch (Exception e) {
                    System.out.println(aLong.toString());
                }
                return "";
            }
        }, DataTypes.StringType);
        Dataset<Row> tableDs = rawDs.filter(length(col("timestamp")).$greater(13) )
                .withColumn("date", functions.callUDF("toDate", col("timestamp")))
                .withColumn("time", functions.callUDF("toTime", col("timestamp")))
//                               .withColumn("time", from_unixtime(col("event_created_at").$div(1000), "yyyy-MM-dd HH:mm:ss"))
                .select(
                        col("date").as("date"),
                        col("time").as("time"),
                        col("id").cast(DataTypes.LongType).as("id"),
                        col("servername").as("servername"),
                        col("userid").cast(DataTypes.LongType).as("userid"),
                        col("username").as("username"),
                        col("starttime").as("starttime"),
                        col("endtime").as("endtime"),
                        col("ip").as("ip"),
                        col("accountdbid").as("accountdbid"),
                        col("province").as("province"),
                        col("city").as("city"),
                        col("ipint").cast(DataTypes.LongType).as("ipint"),
                        col("machinecode").as("machinecode"),
                        col("gameuserid").cast(DataTypes.LongType).as("gameuserid")
                );
        tableDs.show();

//        tableDs.write().mode(SaveMode.Append).format("orc").insertInto("game_analysis.myaccount8");
        tableDs.write().mode(SaveMode.Append).format("orc").insertInto("simple.myaccount8_202004");
        spark.sql("msck repair table simple.myaccount8_202004");
    }
}
