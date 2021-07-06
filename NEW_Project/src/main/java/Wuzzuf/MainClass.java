package Wuzzuf;
import org.apache.spark.sql.Row;
import java.io.IOException;
import java.util.*;

//--------------pie chart-------------------
import java.text.DecimalFormat;
import java.util.stream.Collectors;
import javax.swing.JFrame;
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


public class MainClass
{

    public static void CallAll() throws IOException
    {

        graphs dao = new graphs();
        String filePath = "/Users/ahmed/Downloads/Wuzzuf_Jobs.csv";
        System.out.println("=======================================");
        // Displaying the Summary and the its Structure
        dao.showData(filePath);

        System.out.println("=======================================");
        // Display SampleOfData
        dao.sampleOfData(filePath);

        System.out.println("=======================================");
        // Clean Nulls from Data
        dao.cleanNulls(filePath);

        System.out.println("=======================================");
        // Show Duplicates
        dao.showDuplicates(filePath);

        System.out.println("=======================================");
        // DropDuplicates
        dao.dropDuplicates(filePath);

        System.out.println("=======================================");
        // Count the jobs for each company and display that in order
        // and Most Demanding Company
        dao.countJobs(filePath);

        System.out.println("=======================================");
        // Show step 4 in a pie chart
        dao.pieChart(filePath);

        System.out.println("=======================================");
        // Find out What are it the most popular job titles
        // and Show it in a Bar Chart
        dao.popularJobs(filePath);

        System.out.println("=======================================");
        // Find out the most popular areas
        // and Show it in a Bar Chart
        dao.popularAreas(filePath);

        System.out.println("=======================================");
        // Print skills one by one and
        // how many each repeated and
        // order the output to find out the most important skills required
        dao.skillOneByOne(filePath);

        System.out.println("=======================================");
        // Factorize the YearsExp feature and convert it to numbers in new col
        dao.factorize(filePath);


    }




    // Configuration Methods
    public static class PieChartExample extends JFrame
    {
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

