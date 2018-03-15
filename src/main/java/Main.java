import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ibrahim on 2/22/18.
 */
public class Main {

    public static void main(String[] args){

        //create a spark context to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("word count");

        //create a java version of the spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        //load the txt into a spark RDD which is a distributed representation of each line
        JavaRDD<String> textFile = sc.textFile("hdfs://172.17.0.2:8020/ib/shakespeare.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        System.out.println("total words:" + counts.count());
        counts.saveAsTextFile("hdfs://172.17.0.2:8020/ib/shakespearWC");
    }
}
