package com.sparkstreaming;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

public class StockAnalysis implements Serializable  {
    private static Logger logger = Logger.getLogger(StockAnalysis.class);
    private static String kafkaBrokerId;
    private static String kafkaTopicName;
    private static String kafkaGroupId;
    private final static Long batchDuration = 1L;
    private final static Long windowDuration = 2L;
    private final static Long slidingWindowDuration = 1L;

    public static void main(String[] args) throws Exception {
        StockAnalysis stockAnalysisSparkDriver = new StockAnalysis();
        stockAnalysisSparkDriver.startAnalysis();

    }

    private void startAnalysis() throws StreamingQueryException {
        SparkSession sparkSession = initializeSparkSession();
        Dataset<Row> df = readKafkaTopic(sparkSession);
        applyTransformationsAndPersistToSink(sparkSession,df);

    }


    private SparkSession initializeSparkSession() {
        return SparkSession.builder()
                .appName("Stock Analysis Test App")
                .config("spark.master", "local[*]")
                .config("spark.executor.heartbeatInterval", "600s")
                .config("spark.network.timeout", "700s").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
    }


    private Dataset<Row> readKafkaTopic(SparkSession sparkSession) {
        return sparkSession.readStream().format("kafka")
                .option("subscribe", "TopicA")
                .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .load();
    }

    public void applyTransformationsAndPersistToSink(SparkSession sparkSession,Dataset<Row> df) throws StreamingQueryException {
        Dataset<Row> parsedDf =
                df.select(from_json(col("value").cast("string"), getKafkaSchema()).as("json")).select("json.*");
        parsedDf.writeStream().option("checkpointLocation", "checkpoint_dir")
                .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
                .foreachBatch((batchDf,batchId) -> {
                    batchDf.show();
                    Dataset<Row> transformedDs = batchDf.groupBy(col("stockName"))
                            .agg(avg("price").alias("price"),
                                    min("price").alias("minPrice"), max("price").alias("maxPrice"),
                                    count("price").alias("count"))
                            .select("stockName", "price", "minPrice", "maxPrice", "count");
                    sinkToCsv(sparkSession,transformedDs);
                }).start().awaitTermination();
    }

    public void sinkToCsv(SparkSession sparkSession, Dataset<Row> transformedDf) throws StreamingQueryException {
        transformedDf.writeStream().option("checkpointLocation", "checkpoint_dir")
                .foreachBatch((batchDf, batchId) -> {
                    batchDf.write()
                            .format("csv")
                            .option("header", "false")
                            .mode("overwrite")
                            .save("C://StockAnalysis.csv");
                }).start().awaitTermination();
    }



    protected StructType getKafkaSchema()
    {
        return new StructType()
                .add("stockName", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("timestamp", DataTypes.TimestampType);
    }




}

