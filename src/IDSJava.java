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
        int THRESHOLD = 5;
        ICMPTesting(THRESHOLD);



//        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
//        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");
//        JavaRDD<Map<String, ?>> javaRDDOut = jsc.parallelize(ImmutableList.of(numbers, airports));
//        JavaEsSpark.saveToEs(javaRDDOut, "threats/try");
    }

    private static void ICMPTesting(int THRESHOLD) {
        SparkConf conf = new SparkConf().setAppName("myApp").setMaster("local");
        conf.set("es.index.auto.create", "true");
        conf.set("es.resource", getCurrentDate());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, getCurrentDate() + "/syslog");
        JavaRDD<Map<String, Object>> esRDD1 = JavaEsSpark.esRDD(jsc, getCurrentDate() + "/syslog").values().filter(doc -> doc.containsValue(" Generic ICMP event"));

        long lastCheck = getLastCheck();
        Object[] icmpAndTime = getOnlyICMPEvents(esRDD1, lastCheck);


        List<Map<String, Object>> ICMPevents = (List<Map<String, Object>>) icmpAndTime[0];
        if (ICMPevents.size() > THRESHOLD){
            writeEventToES(ICMPevents, jsc);
        }
        updateLastCheck((Long)icmpAndTime[1]);
    }

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

    private static void writeEventToES(List<Map<String, Object>> ICMPevents, JavaSparkContext jsc) {
        jsc.close();
        SparkConf confSave = new SparkConf().setAppName("myApp").setMaster("local");
        confSave.set("es.index.auto.create", "true");
        confSave.set("es.resource", "tryindex");
        JavaSparkContext jscSave = new JavaSparkContext(confSave);

        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");

        JavaRDD<Map<String, ?>> javaRDD = jscSave.parallelize(ImmutableList.of(numbers, airports));
        JavaEsSpark.saveToEs(javaRDD, "tryindex/try");

    }

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

    public static String getCurrentDate(){
        Calendar now = Calendar.getInstance();
        int day = now.get(Calendar.DAY_OF_MONTH);
        int month = now.get(Calendar.MONTH) + 1;
        int year = now.get(Calendar.YEAR);
        return "syslog-" + year + "." + String.format("%02d", month) + "." + String.format("%02d", day);
    }

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
