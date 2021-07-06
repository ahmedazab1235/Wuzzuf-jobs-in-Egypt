import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;


import java.io.IOException;
import java.util.*;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import static org.apache.spark.sql.functions.desc;
import org.apache.spark.ml.feature.StringIndexer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


//--------------pie chart-------------------


import java.text.DecimalFormat;
import java.util.stream.Collectors;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.PieSectionLabelGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;


import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



public class MainClass {
    public static void main(String[] args) throws IOException {

        DAO dao = new DAO();
        String fileName = "/Users/ahmed/Downloads/Wuzzuf_Jobs.csv";
        Dataset<Row> df = dao.readCSV(fileName);

        // Structure and Summary of Data
        System.out.println("=========== Structure and Summary of Data ===============");
        df.printSchema();

        System.out.println("=============================================");

        // Sample of Data
        System.out.println("============== Sample of Data ===============");
        df.show();

        System.out.println("=============================================");
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

        System.out.println("############### 12 Kmeans Model ################");
        
        System.out.println("############### End of Kmeans Model ################");

        System.out.println("=============================================");
        // Show Duplicates
        System.out.println("=============== Show Duplicates ===================");
        df.distinct().show();

        System.out.println("=============================================");
        // Drop Duplicates
        System.out.println("================ Drop Duplicates ==============");
        df.dropDuplicates().show();

        System.out.println("=============================================");
        // Count the jobs for each company and display that in order
        System.out.println("========== Count the jobs for each company and display that in order =====================");

        Dataset<Row> sortedCompany = df.groupBy("company").count().sort(desc("count"));
        System.out.println(sortedCompany);

        System.out.println("============ Most demanding companies for jobs ==============");
        Dataset<Row> mostCompany = df.groupBy("company").count().sort(desc("count"));
        sortedCompany.show(1);

        System.out.println("=============================================");

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
            PieChartExample example = new PieChartExample("Companies Pie Chart", finalArrayList, finalArrayList1);
            example.setSize(800, 400);
            example.setLocationRelativeTo(null);
            example.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            example.setVisible(true);
        });

        System.out.println("=============================================");
        System.out.println("================= What are it the most popular job titles ====================");
        // What are it the most popular job titles

        Dataset<Row> jobTitles = df.groupBy("Title").count().sort(desc("count"));
        jobTitles.show();

        System.out.println("=============================================");
        System.out.println("============ Most demanding Job Titles ==============");
        Dataset<Row> mostJobTitles = df.groupBy("Title").count().sort(desc("count"));
        mostJobTitles.show(1);
        System.out.println("############Show step 6 in bar chart################");
        List<Row> mostJobTitlesList= mostJobTitles.collectAsList();
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


        System.out.println("##################### end of step 7 ");
        System.out.println("================= the most ALL popular areas  ====================");
        // What are it the most popular areas

        System.out.println("=============================================");
        Dataset<Row> areas = df.groupBy("Location").count().sort(desc("count"));
        areas.show();

        System.out.println("============ the most popular areas ==============");
        Dataset<Row> mostAreas = df.groupBy("Location").count().sort(desc("count"));
        mostAreas.show(1);

        System.out.println("#############the most popular areas Bar chart #############");

        mostJobTitles.show(1);
        System.out.println("############Show step 6 in bar chart################");
        List<Row> mostAreasList= mostAreas.collectAsList();
        HashMap<String, Long> mostAreasMap = new HashMap<>();
        for (Row R: mostAreasList
        ) {
            mostAreasMap.put(String.valueOf(R.get(0)),Long.parseLong(String.valueOf(R.get(1))));
        }
        reverseSortedMap.clear();
        mostAreasMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        MainClass.graphPopularAreas( reverseSortedMap);
        //reverseSortedMap.entrySet().stream().limit(20).forEach(count -> System.out.println(count));





        System.out.println("=============================================");
        System.out.println("============ Skills one by one ==============");

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
        System.out.println("=============================================");

        System.out.println("################### 11-String indexer######################");
        StringIndexer indexer = new StringIndexer()
                .setInputCol("YearsExp")
                .setOutputCol("categoryIndex");
        Dataset<Row> indexed = indexer.fit(df).transform(df);
        indexed.show();
        System.out.println("################### end of String indexer######################");


        Dataset<Row> dataset = df.select("Title");

// Trains a k-means model.
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel model = kmeans.fit(dataset);

// Evaluate clustering by computing Within Set Sum of Squared Errors.
        double WSSSE = model.computeCost(dataset);
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

// Shows the result.
        Vector[] centers = model.clusterCenters();
        System.out.println("Cluster Centers: ");
        for (Vector center: centers) {
            System.out.println(center);
        }




    }

    public static class PieChartExample extends JFrame {
        private static final long serialVersionUID = 6294689542092367723L;

        public PieChartExample(String title, List<Row> compname , List<Row> countnum) {
            super(title);

            // Create dataset
            PieDataset dataset = createDataset(compname, countnum);

            // Create chart
            JFreeChart chart = ChartFactory.createPieChart(
                    "Highest 10 companies",
                    dataset,
                    true,
                    true,
                    false);

            //Format Label
            PieSectionLabelGenerator labelGenerator = new StandardPieSectionLabelGenerator(
                    "{0} : ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
            ((PiePlot) chart.getPlot()).setLabelGenerator(labelGenerator);

            // Create Panel
            ChartPanel panel = new ChartPanel(chart);
            setContentPane(panel);
        }

        private PieDataset createDataset(List<Row> compname, List<Row> countnum) {

            DefaultPieDataset dataset = new DefaultPieDataset();

            for (int i = 0; i < 10; i++) {

                String a = countnum.get(i).toString();

                a = a.substring(1,(a.length()-1));

                int count = Integer.parseInt(a);

                String b = compname.get(i).toString();

                dataset.setValue(b.substring(1,(b.length()-1)) , count);


            }


            return dataset;
        }


    }

    public static String extractSkills(Row skills) {
        try {
            return skills.toString().substring(1,skills.toString().length() - 1).split(",")[7];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static void graphPopularJobTitles(Map<String, Long> jobTitles) {
        //filter to get an array of passenger ages
        List<String> jobs = jobTitles.keySet().stream().limit(5).collect(Collectors.toList());
        List<Long> count = jobTitles.values().stream().limit(5).collect(Collectors.toList());

        CategoryChart chart = new CategoryChartBuilder()
                .width(1024)
                .height(768)
                .title("Most popular job titles")
                .xAxisTitle("Job titles").yAxisTitle("Count")
                .build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);

        // Series
        chart.addSeries("Most popular job titles", jobs, count);
        // pass to wrapper and show graph
        new SwingWrapper(chart).displayChart();
    }
    public static void graphPopularAreas(Map<String, Long> jobTitles) {
        //filter to get an array of passenger ages
        List<String> jobs = jobTitles.keySet().stream().limit(5).collect(Collectors.toList());
        List<Long> count = jobTitles.values().stream().limit(5).collect(Collectors.toList());

        CategoryChart chart = new CategoryChartBuilder()
                .width(1024)
                .height(768)
                .title("Most popular job Areas")
                .xAxisTitle("Job Areas").yAxisTitle("Count")
                .build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);

        // Series
        chart.addSeries("Most popular job Areas", jobs, count);
        // pass to wrapper and show graph
        new SwingWrapper(chart).displayChart();
    }
    public static LinkedHashMap<String, Long> sortHashMapByValues(
            HashMap<String, Long> passedMap) {
        List<String> mapKeys = new ArrayList<>(passedMap.keySet());
        List<Long> mapValues = new ArrayList<>(passedMap.values());
        Collections.sort(mapValues);
        Collections.sort(mapKeys);

        LinkedHashMap<String, Long> sortedMap =
                new LinkedHashMap<>();

        Iterator<Long> valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            Long val = valueIt.next();
            Iterator<String> keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                String key = keyIt.next();
                Long comp1 = passedMap.get(key);
                Long comp2 = val;

                if (comp1.equals(comp2)) {
                    keyIt.remove();
                    sortedMap.put(key, val);
                    break;
                }
            }
        }
        return sortedMap;
    }

}


