package Wuzzuf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class preprocessing {
    public static Dataset<Row> clean(Dataset<Row> df){
        df = df.withColumn("Title", df.col("Title").cast("String"))
                .filter(df.col("Title").isNotNull());
        df = df.withColumn("Company", df.col("Company").cast("String"))
                .filter(df.col("Company").isNotNull());
        df = df.withColumn("Location", df.col("Location").cast("String"))
                .filter(df.col("Location").isNotNull());
        df = df.withColumn("Type", df.col("Type").cast("String"))
                .filter(df.col("Type").isNotNull());
        df = df.withColumn("Level", df.col("Level").cast("String"))
                .filter(df.col("Level").isNotNull());
        df = df.withColumn("YearsExp", df.col("YearsExp").cast("String"))
                .filter(df.col("YearsExp").isNotNull());
        df = df.withColumn("Country", df.col("Country").cast("String"))
                .filter(df.col("Country").isNotNull());
        df = df.withColumn("Skills", df.col("Skills").cast("String"))
                .filter(df.col("Skills").isNotNull());

        df = df.distinct();

        // Drop Duplicates
        df = df.dropDuplicates();
        df.show();
        return df;
    }


}