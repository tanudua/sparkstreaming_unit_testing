import com.sparkstreaming.StockAnalysis;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Tested;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertTrue;


public class TestStockAnalysis {
    protected static SparkSession spark;
    protected static final String checkpointDir = "C://Checkpoint";

    @Tested
    private StockAnalysis stockAnalysis;

    @BeforeClass
    public static void setUpClass() throws Exception {
        spark = SparkSession.builder()
                .appName("Prescore-Test")
                .master("local[*]")
                .getOrCreate();
        File file = new File(checkpointDir);
        if(file.exists()) {
            FileUtils.deleteDirectory(file);
        }

    }


    @Test
    public void test_stock_analysis() throws StreamingQueryException {
        new Expectations() {{
            new MockUp<StockAnalysis>() {
                @Mock
                public void sinkToCsv(SparkSession sparkSession, Dataset<Row> transformedDf) throws StreamingQueryException {
                    List<Row> dataToBePersisted = transformedDf.sort(col("stockName")).collectAsList();
                    assertTrue("All required columns are set",Arrays.stream(transformedDf.schema().fieldNames()).anyMatch(
                            x -> {
                                return (x.equals("stockName") || x.equals("price") || x.equals("minPrice") || x.equals("maxPrice") || x.equals("count"));
                            }));
                    Assert.assertEquals("Average Validation", new Double(20.0), (Double)dataToBePersisted.get(0).getAs("price"));
                    stopAllStreamingQueries();
                }
            };
        }};
        MemoryStream<String> inputMemoryStream = createMemoryStream("test_files/stock.csv");
        stockAnalysis.applyTransformationsAndPersistToSink(spark,inputMemoryStream.toDF());
    }

    protected MemoryStream<String> createMemoryStream(String csvFileName) {
        String path = getClass().getResource(csvFileName).getPath();
        Dataset<String> ds = spark.sqlContext().read().format("csv").option("header","true").option("inferSchema", false).schema(getKafkaSchema()).load(path.toString()).toJSON();
        ds.show();
        List<String> testCsvData = ds.collectAsList();
        MemoryStream<String> inputMemoryStream = new MemoryStream<String>(1, spark.sqlContext(), Encoders.STRING());
        inputMemoryStream.addData(JavaConversions.asScalaBuffer(testCsvData).toSeq());
        return inputMemoryStream;
    }



    protected void stopAllStreamingQueries() {
        StreamingQuery[] queries = spark.streams().active();
        for(StreamingQuery query : queries) {
            query.stop();
        }
    }

    protected StructType getKafkaSchema()
    {
        return new StructType()
                .add("stockName", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("timestamp", DataTypes.TimestampType);
    }



}
