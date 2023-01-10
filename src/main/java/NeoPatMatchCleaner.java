import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;

/**
 * Created by copperfield @ 2020/11/13 17:27
 */
public class NeoPatMatchCleaner {

    //    Date dayBefore = new Date();
    public static final String INPUT_DATA_PATH = "hdfs://ice-test-851pm-m1:11000/tmp/topics/neopetmatch_product/2020/";

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("etl-spark-neopetmatchcleaner-hive")
                .config("hive.metastore.uris", "thrift://ice-test-851pm-m1:9083")
                .enableHiveSupport()
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.exec.max.dynamic.partitions", 10000)
                .getOrCreate();

        Configuration conf = spark.sparkContext().hadoopConfiguration();

        Path path = new Path(INPUT_DATA_PATH);
        FileSystem fileSystem = path.getFileSystem(conf);
        Preconditions.checkArgument(fileSystem.exists(path), "Raw data files don't exist!");


//        JavaRDD<String> rdd = spark.read().textFile(INPUT_DATA_PATH)
//                .javaRDD().map(StringEscapeUtils::unescapeJava)
//                .map(s -> CharMatcher.anyOf("\"").trimLeadingFrom(s));

//        Dataset<Row> data = spark.read().json(spark.createDataset(rdd.rdd(), Encoders.STRING()));
        Dataset<Row> data = spark.read().json(INPUT_DATA_PATH);

        Dataset<Row> burPointRecord = data.filter(col("name").equalTo("BurPointRecord"))
                .withColumn("triggerDate", from_unixtime(col("value.triggerTime"), "yyyy-MM-dd"))
                .withColumn("triggerTime", from_unixtime(col("value.triggerTime"), "yyyy-MM-dd HH:mm:ss"))
                .select(col("value.bugSteps").as("bugSteps"),
                        col("triggerDate").as("triggerDate"),
                        col("triggerTime").as("triggerTime"),
                        col("value.buildingID").as("buildingID"),
                        col("value.buyLives").as("buyLives"),
                        col("value.channelCode").as("channelCode"),
                        col("value.deviceID").as("deviceID"),
                        col("value.deviceModel").as("deviceModel"),
                        col("value.eventID").as("eventID"),
                        col("value.failLevel").as("failLevel"),
                        col("value.getCoins").as("getCoins"),
                        col("value.levelNum").as("levelNum"),
                        col("value.platform").as("platform"),
                        col("value.playerID").as("playerID"),
                        col("value.plotID").as("plotID"),
                        col("value.quitLevel").as("quitLevel"),
                        col("value.surplusSteps").as("surplusSteps"),
                        col("value.useBoosterBig").as("useBoosterBig"),
                        col("value.useBoosterBomb").as("useBoosterBomb"),
                        col("value.useBoosterSmall").as("useBoosterSmall"),
                        col("value.usePrePropColorBomb").as("usePrePropColorBomb"),
                        col("value.usePrePropLuck").as("usePrePropLuck"),
                        col("value.usePrePropPlane").as("usePrePropPlane"),
                        col("value.winLevel").as("winLevel"));
        burPointRecord.show(10);
//        burPointRecord.write().mode(SaveMode.Overwrite).format("hive").partitionBy("triggerDate").saveAsTable("game_analysis.bur_point_record");
//        spark.sql("msck repair table game_analysis.bur_point_record");

//        Dataset<Row> customReport = data.filter(col("name").equalTo("CustomReport"))
//                .withColumn("createDate", from_unixtime(col("value.createTime"), "yyyy-MM-dd"))
//                .withColumn("createTime", from_unixtime(col("value.createTime"), "yyyy-MM-dd HH:mm:ss"))
//                .select(col("value.accountID").as("accountID"),
//                        col("value.appKey").as("appKey"),
//                        col("value.channelCode").as("channelCode"),
//                        col("createDate").as("createDate"),
//                        col("createTime").as("createTime"),
//                        col("value.deviceID").as("deviceID"),
//                        col("value.deviceModel").as("deviceModel"),
//                        col("value.eventCnt").as("eventCnt"),
//                        col("value.eventID").as("eventID"),
//                        col("value.eventValue").as("eventValue"),
//                        col("value.platform").as("platform"),
//                        col("value.serverID").as("serverID"),
//                        col("value.userID").as("userID"));
////        customReport.show();
//        customReport.write().mode(SaveMode.Overwrite).format("hive").partitionBy("createDate").saveAsTable("game_analysis.custom_report");
//        spark.sql("msck repair table game_analysis.custom_report");
//
//        Dataset<Row> loginRecord = data.filter(col("name").equalTo("LoginRecord"))
//                .withColumn("createDate", from_unixtime(col("value.createTime"), "yyyy-MM-dd"))
//                .withColumn("createTime", from_unixtime(col("value.createTime"), "yyyy-MM-dd HH:mm:ss"))
//                .select(col("value.accountID").as("accountID"),
//                        col("value.appKey").as("appKey"),
//                        col("value.actType").as("actType"),
//                        col("value.appVer").as("appVer"),
//                        col("value.channelCode").as("channelCode"),
//                        col("createDate").as("createDate"),
//                        col("createTime").as("createTime"),
//                        col("value.deviceID").as("deviceID"),
//                        col("value.deviceModel").as("deviceModel"),
//                        col("value.deviceVer").as("deviceVer"),
//                        col("value.iP").as("iP"),
//                        col("value.netMode").as("netMode"),
//                        col("value.platform").as("platform"),
//                        col("value.screenX").as("screenX"),
//                        col("value.screenY").as("screenY"),
//                        col("value.serverID").as("serverID"),
//                        col("value.sessionID").as("sessionID"),
//                        col("value.userID").as("userID"));
////        loginRecord.show();
//        loginRecord.write().mode(SaveMode.Overwrite).format("hive").partitionBy("createDate").saveAsTable("game_analysis.login_record");
//        spark.sql("msck repair table game_analysis.login_record");
//
//        Dataset<Row> comsumeReport = data.filter(col("name").equalTo("ComsumeReport"))
//                .withColumn("statDate", from_unixtime(col("value.statTime"), "yyyy-MM-dd"))
//                .withColumn("statTime", from_unixtime(col("value.statTime"), "yyyy-MM-dd HH:mm:ss"))
//                .select(col("value.accountID").as("accountID"),
//                        col("value.appKey").as("appKey"),
//                        col("value.itemCnt").as("itemCnt"),
//                        col("value.itemType").as("itemType"),
//                        col("value.moneyType").as("moneyType"),
//                        col("statDate").as("statDate"),
//                        col("statTime").as("statTime"),
//                        col("value.number").as("number"),
//                        col("value.price").as("price"),
//                        col("value.serverID").as("serverID"),
//                        col("value.type").as("type"),
//                        col("value.userID").as("userID"));
////        comsumeReport.show();
//        comsumeReport.write().mode(SaveMode.Overwrite).format("hive").partitionBy("statDate").saveAsTable("game_analysis.consume_report");
//        spark.sql("msck repair table game_analysis.consume_report");
    }
}
