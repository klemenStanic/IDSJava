import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.org.apache.xpath.internal.SourceTree;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkConf.*;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext.*;
import org.apache.spark.SparkContext.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.*;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;




public class IDSJava {
    public Long lastCheckTime;
    public static void main(String[] args) {
        int THRESHOLD = Integer.parseInt(args[0]);
        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("local");
        conf.set("es.index.auto.create", "true");
        System.out.println("GET CURRENT DATE: " + getCurrentDate(true));
        JavaSparkContext jsc = new JavaSparkContext(conf);


        //perform ICMP tests, if there are more than THRESHOLD ICMP events, write a warning back to elasticsearch
        ICMPTesting(THRESHOLD, jsc);

    }

    // checks  for ICMP events in Elasticsearch, if more than THRESHOLD, calls writeEventToES()
    private static void ICMPTesting(int THRESHOLD, JavaSparkContext jsc) {

        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, getCurrentDate(true) + "/syslog");
        JavaRDD<Map<String, Object>> esRDD1 = JavaEsSpark.esRDD(jsc, getCurrentDate(true) + "/syslog").values().filter(doc -> doc.containsValue(" Generic ICMP event"));

        long lastCheck = getLastCheck();
        Object[] icmpAndTime = getOnlyICMPEvents(esRDD1, lastCheck);


        List<Map<String, Object>> ICMPevents = (List<Map<String, Object>>) icmpAndTime[0];
        System.out.println("------------------------------------------------------------");
        System.out.println("SIZE: " + ICMPevents.size());
        for (Map<String, Object> a : ICMPevents){
            System.out.println(a.get("event").toString());
        }
        System.out.println("...........................................................");
        if (ICMPevents.size() > THRESHOLD){
            writeEventToES(ICMPevents, jsc);
        }
        updateLastCheck((Long)icmpAndTime[1]);
    }

    // writes an ICMP warning to ElasticSearch
    private static void writeEventToES(List<Map<String, Object>> ICMPevents, JavaSparkContext jsc) {
        System.out.println("ISITHERE?----------------------------------------------------------------------------");
        jsc.close();
        SparkConf confSave = new SparkConf().setAppName("myApp").setMaster("local");
        confSave.set("es.index.auto.create", "true");
        JavaSparkContext jscSave = new JavaSparkContext(confSave);

        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2, "date", getCurrentDate(false));
        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran", "date", getCurrentDate(false));

        JavaRDD<Map<String, ?>> javaRDD = jscSave.parallelize(ImmutableList.of(numbers, airports));
        JavaEsSpark.saveToEs(javaRDD, "threats/icmp");
    }

    // writes the last ICMP checked to file, to know from where to read in next iteration of running this program
    private static void updateLastCheck(Long toOut) {
        try {
            File file = new File("last_check.txt");
            file.delete();
            FileWriter fw = new FileWriter(new File("last_check.txt"), false);
            fw.write(toOut + "");
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    // called by ICMPTesting()
    public static Object[] getOnlyICMPEvents(JavaRDD<Map<String, Object>> esRDD1, long lastCheck){
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        List<Map<String, Object>> onlyICMPevents = new ArrayList<>();
        Long maxTime = 0L;
        try {
            for (Map<String, Object> a : esRDD1.collect()) {
                String time = a.get("@timestamp").toString().split(" ")[3];
                Date d = sdf.parse(time);
                if (d.getTime() > lastCheck){
                    onlyICMPevents.add(a);
                }
                if (d.getTime() > maxTime){
                    maxTime = d.getTime();
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        for (Map<String, Object> a : onlyICMPevents){
            System.out.println(a.get("@timestamp") + "");
        }
        Object[] out = new Object[2];
        out[0] = onlyICMPevents;
        out[1] = maxTime;
        return out;
    }

    // if boolean full == true; returns full name of the index, else returns @String time in miliseconds
    public static String getCurrentDate(boolean full){
        Calendar now = Calendar.getInstance();
        int day = now.get(Calendar.DAY_OF_MONTH);
        int month = now.get(Calendar.MONTH) + 1;
        int year = now.get(Calendar.YEAR);
        if (full) {
            return "syslog-" + year + "." + String.format("%02d", month) + "." + String.format("%02d", day);
        } else return now.getTimeInMillis() + "";
    }

    // reads time from file. Time = when the last iteration of this program was performed, so we dont need to check the whole index every time we run this program
    public static long getLastCheck() {
        try {
            Scanner sc = new Scanner(new File("last_check.txt"));
            Long time = sc.nextLong();
            sc.close();
            return time;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
