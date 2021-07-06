package Wuzzuf.controller;

import Wuzzuf.DAO;
import Wuzzuf.MainClass;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.openxml4j.opc.internal.FileHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

// import static jdk.xml.internal.SecuritySupport.getResourceAsStream;
import static org.apache.spark.sql.functions.desc;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;

import javax.servlet.http.HttpServletResponse;

@RestController
public class controller{
    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private JavaSparkContext javaSparkContext;

    @Autowired
    private Dataset<Row> Data;


    @GetMapping("/companies")
    public Map<String, Integer> pop_companies() {
        Dataset<Row> sortedCompany = Data.groupBy("company").count().sort(desc("count"));
        sortedCompany = sortedCompany.limit(5);
        Map<String, Integer> outMap = new LinkedHashMap<>();
        for(Row r: sortedCompany.collectAsList()) {
            String first = r.get(0).toString();
            int second = Integer.parseInt(r.get(1).toString());
            outMap.put(first, second);
        }
        return outMap;
    }

    @GetMapping("/titles")
    public Map<String, Integer> pop_titles() {
        Dataset<Row> mostJobTitles = Data.groupBy("Title").count().sort(desc("count"));
        mostJobTitles.show(5);
        mostJobTitles = mostJobTitles.limit(5);
        Map<String, Integer> rowsMap = new LinkedHashMap<>();
        for(Row r: mostJobTitles.collectAsList()) {
            String first = r.get(0).toString();
            int second = Integer.parseInt(r.get(1).toString());
            rowsMap.put(first, second);
        }
        return rowsMap;
    }

    @GetMapping("/areas")
    public Map<String, Integer> pop_areas() {
        System.out.println("============ the most popular areas ==============");
        Dataset<Row> areas = Data.groupBy("Location").count().sort(desc("count"));
        areas.show(5);
        areas = areas.limit(5);
        System.out.println("############Show step 6 in bar chart################");
        Map<String, Integer> rowsMap = new LinkedHashMap<>();
        for(Row r: areas.collectAsList()) {
            String first = r.get(0).toString();
            int second = Integer.parseInt(r.get(1).toString());
            rowsMap.put(first, second);
        }
        return rowsMap;

    }

    @GetMapping("/skills")
    public Map<String, Long> pop_skills() {

        JavaRDD<Row> wuzzufData = Data.select("Skills").toJavaRDD();
        JavaRDD<String> skills = wuzzufData
                .map(MainClass::extractSkills)
                .filter(StringUtils::isNotBlank)
                ;


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
        skillsList.entrySet().stream().limit(10).forEach(count -> System.out.println(count));
        System.out.println("=============================================");

        return skillsList;
    }

    @GetMapping("/show-data")
    public List<List<String>> show_data() {
        String[] columns = Data.columns();
        List<String> columns_data = new ArrayList<>(Arrays.asList(columns));
        List<Row> rows = Data.limit(10).collectAsList();
        List<List<String>> string_data = new ArrayList<>();
        string_data.add(columns_data);
        for(Row row : rows) {
            List<String> record = new ArrayList<>();
            for(int i = 0; i < columns.length; i++) {
                record.add(row.get(i).toString());
            }
            string_data.add(record);
        }
        return string_data;
    }

    @GetMapping("/schema")
    public ArrayList<String> print_schema() {
        StructType schema = Data.schema();
        StructField[] data = schema.fields();
        ArrayList<String> schem = new ArrayList<>();
        for(StructField f : data) {
            schem.add("|-- "+f.name() + " : " + f.dataType().typeName().toString()+"(nullable = true)");
        }

        System.out.println("=========== Structure and Summary of Data ===============");
        Data.printSchema();

        return schem;
    }

    @GetMapping("/summary")
    public ArrayList<String> print_summary() {
        StructType schema = Data.schema();
        ArrayList<String> summary = new ArrayList<>();
        StructField[] data = schema.fields();
        for(StructField f : data) {
            List<String> col = Data.select(f.name()).as(Encoders.STRING()).collectAsList();
            JavaRDD rdd = javaSparkContext.parallelize(col);
            Map<String, Long> unordered = rdd.countByValue();
            summary.add(f.name()+" ------------ "+String.valueOf(unordered.size()));
            System.out.println("============== Sample of Data ===============");
            Data.show();

        }
        return summary;
    }

    @GetMapping("/factorize")
    public Map<String, String> factorize_years() {
        System.out.println("=============== Factorize the YearsExp ============");
        StringIndexer indexer = new StringIndexer()
                .setInputCol("YearsExp")
                .setOutputCol("categoryIndex");
        Dataset<Row> indexed = indexer.fit(Data).transform(Data);
        indexed.select("YearsExp","categoryIndex").show(20);
        indexed = indexed.select("YearsExp","categoryIndex").limit(20);
        Map<String, String> m = new HashMap<>();
        for(Row r: indexed.collectAsList()) {
            String first = r.get(0).toString();
            String second = String.valueOf(r.get(1));
            m.put(first, second);
        }

        return m;
    }


}