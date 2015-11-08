import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Scanner;

/**
 * Created by hduser on 11/8/15.
 */
public class SparkOperations {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkClosestPair")
                .setMaster("spark://192.168.0.25:7077");
        //.setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        ctx.addJar("/home/vamshi/.m2/repository/com/vividsolutions/jts/1.13/jts-1.13.jar");
        ctx.addJar("target/sparkDds-1.0-SNAPSHOT.jar");

        Scanner in = new Scanner(System.in);
        int ip = in.nextInt();
        if (ip == 1) {
            // Closest Pair HDFS Path
            SparkClosestPair.closestPairFactory(ctx);
        }


    }
}
