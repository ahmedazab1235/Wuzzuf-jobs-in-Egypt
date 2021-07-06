package Wuzzuf;
import  Wuzzuf.MainClass;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import javax.swing.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.desc;

public class graphs
{

    public Dataset<Row> readCSV(String fileName)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        final SparkSession sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]")
                .getOrCreate ();
        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        dataFrameReader.option ("header", "true");
        final Dataset<Row> csvDataFrame = dataFrameReader.csv (fileName);

        return csvDataFrame;

    }

    public void showData(String csvFile)
    {
        Dataset<Row> df = readCSV(csvFile);

        // Structure and Summary of Data
        System.out.println("=========== Structure and Summary of Data ===============");
        df.printSchema();
    }

    public void sampleOfData(String csvFile)
    {
        // Sample of Data
        Dataset<Row> df = readCSV(csvFile);
        System.out.println("============== Sample of Data ===============");
        df.show();
    }

    public void cleanNulls(String csvFile)
    {

        Dataset<Row> df = readCSV(csvFile);
        // Clean the Data form Null Values
        System.out.println("============ Clean the Data form Null Values =============");
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
        df.show();
    }

    public void showDuplicates(String csvFile)
    {
        Dataset<Row> df = readCSV(csvFile);
        // Show Duplicates
        System.out.println("=============== Show Duplicates ===================");
        df.distinct().show();
    }

    public void dropDuplicates(String csvFile)
    {
        Dataset<Row> df = readCSV(csvFile);
        // Drop Duplicates
        System.out.println("================ Drop Duplicates ==============");
        df.dropDuplicates().show();
    }

    public void countJobs(String csvFile)
    {
        Dataset<Row> df = readCSV(csvFile);
        // Count the jobs for each company and display that in order
        System.out.println("========== Count the jobs for each company and display that in order =====================");

        Dataset<Row> sortedCompany = df.groupBy("company").count().sort(desc("count"));
        System.out.println(sortedCompany);

        System.out.println("============ Most demanding companies for jobs ==============");
        sortedCompany.show(1);
    }

    public void pieChart(String csvFile)
    {
        Dataset<Row> df = readCSV(csvFile);
        Dataset<Row> mostCompany = df.groupBy("company").count().sort(desc("count"));

        // Pie Chart
        System.out.println("================ Pie Chart =================");
        Dataset<Row> compname1 = mostCompany.select("company");
        List<Row> arrayList= new ArrayList<>();
        arrayList = compname1.collectAsList();
        Dataset<Row> count1 = mostCompany.select("count");
        List<Row> arrayList2= new ArrayList<>();
        arrayList2 = count1.collectAsList();
        List<Row> finalArrayList = arrayList;
        List<Row> finalArrayList1 = arrayList2;
        SwingUtilities.invokeLater(() -> {
            MainClass.PieChartExample example = new MainClass.PieChartExample("Companies Pie Chart", finalArrayList, finalArrayList1);
            example.setSize(800, 400);
            example.setLocationRelativeTo(null);
            example.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            example.setVisible(true);
        });

    }

    public void popularJobs(String csvFile)
    {
        Dataset<Row> df = readCSV(csvFile);
        System.out.println("================= What are it the most popular job titles ====================");
        // What are it the most popular job titles

        Dataset<Row> jobTitles = df.groupBy("Title").count().sort(desc("count"));
        jobTitles.show();

        System.out.println("============ Most demanding Job Titles ==============");
        jobTitles.show(1);

        System.out.println("============ Show JobTitles in bar chart ============");
        List<Row> mostJobTitlesList= jobTitles.collectAsList();
        HashMap<String, Long> mostJobTitlesMap = new HashMap<>();
        for (Row R: mostJobTitlesList
        ) {
            mostJobTitlesMap.put(String.valueOf(R.get(0)),Long.parseLong(String.valueOf(R.get(1))));
        }
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();

        mostJobTitlesMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        MainClass.graphPopularJobTitles( reverseSortedMap);
        reverseSortedMap.entrySet().stream().limit(20).forEach(count -> System.out.println(count));

    }

    public void popularAreas(String csvFile)
    {
        Dataset<Row> df = readCSV(csvFile);
        System.out.println("============ the most popular areas ==============");
        Dataset<Row> areas = df.groupBy("Location").count().sort(desc("count"));
        areas.show();

        System.out.println("============ the most popular area ==============");
        areas.show(1);



        System.out.println("============ Show MostPopularAreas in bar chart ============");

        List<Row> mostAreasList = areas.collectAsList();
        HashMap<String, Long> mostAreasMap = new HashMap<>();
        for (Row R: mostAreasList
        ) {
            mostAreasMap.put(String.valueOf(R.get(0)),Long.parseLong(String.valueOf(R.get(1))));
        }
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();
        reverseSortedMap.clear();
        mostAreasMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        MainClass.graphPopularAreas( reverseSortedMap);
        //reverseSortedMap.entrySet().stream().limit(20).forEach(count -> System.out.println(count));

    }

    public void skillOneByOne(String csvFile)
    {
        System.out.println("============ Skills one by one ==============");
        Dataset<Row> df = readCSV(csvFile);

        JavaRDD<Row> wuzzufData = df.select("Skills").toJavaRDD();
        JavaRDD<String> skills = wuzzufData
                .map(MainClass::extractSkills)
                .filter(StringUtils::isNotBlank);


        Map<String, Long> skillsList =
                skills
                        .countByValue()
                        .entrySet()
                        .stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (oldValue, newValue)-> oldValue, LinkedHashMap::new)
                        );

        System.out.println("Most important skills required: ");
        skillsList.entrySet().stream().limit(20).forEach(count -> System.out.println(count));

    }

    public void factorize(String csvFile)
    {
        System.out.println("=============== Factorize the YearsExp ============");
        Dataset<Row> df = readCSV(csvFile);
        StringIndexer indexer = new StringIndexer()
                .setInputCol("YearsExp")
                .setOutputCol("categoryIndex");
        Dataset<Row> indexed = indexer.fit(df).transform(df);
        indexed.show();
    }

}
