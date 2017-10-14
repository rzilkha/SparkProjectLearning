package example;




import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Roee Zilkha on 10/14/2017.
 */
public class RatingExampleJava {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("java");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sc.textFile("C:\\sparkProject\\src\\main\\resources\\u.data");

        JavaRDD<String> result = rdd.map(item -> item.split("\t")[2]);

        Map<String, Long> countResult = result.countByValue();
        Map<String, Long> treeMap = new TreeMap<String, Long>(countResult);

        for(String key : treeMap.keySet()){
            System.out.printf("key: %s value:%d%n", key, treeMap.get(key));
        }


    }
}
