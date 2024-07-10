import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.sum;

public class EstimateSummation {
    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession
                .builder()
                .getOrCreate();

        // Create a JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Define the schema
        String schemaString = "year_month,month_of_release,passenger_type,direction,citizenship,visa,country_of_residence,estimate,standard_error,status";
        String[] fields = schemaString.split(",");

        // Convert the fields array into a list
        List<String> fieldList = Arrays.asList(fields);

        StructField[] structFields = sc.parallelize(fieldList).map(fieldName ->
                DataTypes.createStructField(fieldName, DataTypes.StringType, true)
        ).collect().toArray(new StructField[0]);

        StructType schema = DataTypes.createStructType(structFields);

        // Read CSV
        Dataset<Row> sdfData = spark.read()
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load("../../migration.csv");

        // Explicitly repartition the DataFrame
        sdfData = sdfData.repartition(3);

        // Start timer
        long startTime = System.currentTimeMillis();

        // Sum all of the column Age values in parallel
        Dataset<Row> sumAge = sdfData.agg(sum(sdfData.col("estimate")));
        sumAge.show();

        // End timer
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.println("Time taken for sum calculation: " + elapsedTime + " milliseconds");

        spark.stop();
    }
}
