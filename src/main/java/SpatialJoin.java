import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SpatialJoin {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SpatialJoin").setMaster("spark://127.0.0.1:7077");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.addJar("target/dds-1.0-SNAPSHOT.jar");
        sc.addJar("lib/jts-1.13.jar");
        sc.addJar("lib/guava-18.0.jar");
        JavaRDD<String> targetFile = sc.textFile("hdfs://master:54310/user/hduser/JoinQueryInput1.csv");
        JavaRDD<String> queryFile = sc.textFile("hdfs://master:54310/user/hduser/JoinQueryInput2.csv");
        final JavaRDD<GeometryWrapper> queries = queryFile.map(new JoinQueryReadInput());
        List<GeometryWrapper> queryForBroadCast = queries.collect();
        final Broadcast<List<GeometryWrapper>> broad_var = sc.broadcast(queryForBroadCast);
        JavaRDD<GeometryWrapper> targets = targetFile.map(new JoinQueryReadInput());
        int choice;
        if(targets.first().getGeometry() instanceof Point)
            choice = 1;
        else
            choice = 2;
        final Broadcast<Integer> brChoice = sc.broadcast(choice);

        JavaPairRDD<Integer, Integer> result = targets.mapPartitionsToPair(new PairFlatMapFunction<Iterator<GeometryWrapper>, Integer, Integer>() {
            public Iterable<Tuple2<Integer, Integer>> call(Iterator<GeometryWrapper> rectangleIterator) throws Exception {
                List<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();
                List<GeometryWrapper> bv = broad_var.getValue();
                Integer choice = brChoice.getValue();
                while(rectangleIterator.hasNext()){
                    GeometryWrapper target = rectangleIterator.next();
                    for(GeometryWrapper query: bv){
                        if(choice == 2 && (query.getGeometry().intersects(target.getGeometry())
                                || query.getGeometry().contains(target.getGeometry())
                                || target.getGeometry().contains(query.getGeometry())
                                || query.getGeometry().equals(target.getGeometry()))){
                            result.add(new Tuple2<Integer, Integer>(query.getId(), target.getId()));
                        }else{
                            if(choice == 1 && (query.getGeometry().contains(target.getGeometry())
                                || query.getGeometry().intersects(target.getGeometry()))){
                                result.add(new Tuple2<Integer, Integer>(query.getId(), target.getId()));
                            }
                        }
                    }
                }
                return result;
            }
        });
        JavaPairRDD<Integer, Iterable<Integer>> toPrint = result.groupByKey();//.sortByKey(true);
        JavaRDD<String> filePrint = toPrint.flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Integer>>, String>() {
            public Iterable<String> call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                List<String> res = new ArrayList<String>();
                String temp = integerIterableTuple2._1().toString();
                for (Integer id : integerIterableTuple2._2()) {
                    temp += "," + id;
                }
                res.add(temp);
                return res;
            }
        }).sortBy(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return Integer.parseInt(s.substring(0, s.indexOf(",")));
            }
        }, true, 1);
//        List<String> resFinal = filePrint.collect();
//        for(String res: resFinal){
//            System.out.println(res);
//        }
        filePrint.saveAsTextFile("hdfs://master:54310/user/hduser/JoinQueryResult.csv");
    }

    public static class JoinQueryReadInput implements Function<String, GeometryWrapper> {
        public GeometryWrapper call(String s) throws Exception {
            String vals[] = s.split(",");
            GeometryWrapper r;
            if(vals.length > 3){
                int id = Integer.parseInt(vals[0]);
                double x1 = Double.parseDouble(vals[1]);
                double y1 = Double.parseDouble(vals[2]);
                double x2 = Double.parseDouble(vals[3]);
                double y2 = Double.parseDouble(vals[4]);
                r = new GeometryWrapper(id, x1, y1, x2, y2);
            }else{
                int id = Integer.parseInt(vals[0]);
                double x1 = Double.parseDouble(vals[1]);
                double y1 = Double.parseDouble(vals[2]);
                r = new GeometryWrapper(id, x1, y1);
            }
            return r;
        }
    }


}

class GeometryWrapper implements java.io.Serializable{
    private int id;
    private double x1, y1, x2, y2;
    private Polygon rect = null;
    private Point point = null;


    public GeometryWrapper(int id, double x1, double y1, double x2, double y2) {
        this.id = id;
        this.x1 = x1;
        this.y1 = y1;
        this.x2 = x2;
        this.y2 = y2;

        Coordinate rect[] = new Coordinate[5];

        rect[0] = new Coordinate(x1, y1);
        rect[1] = new Coordinate(x2, y1);
        rect[2] = new Coordinate(x2, y2);
        rect[3] = new Coordinate(x1, y2);
        rect[4] = new Coordinate(x1, y1);

        LinearRing r = new LinearRing(new CoordinateArraySequence(rect), new GeometryFactory());
        this.rect = new Polygon(r, null, new GeometryFactory());
    }

    public GeometryWrapper(int id, double x1, double y1){
        this.id = id;
        this.x1 = x1;
        this.y1 = y1;
        GeometryFactory geometryFactory = new GeometryFactory();
        point = geometryFactory.createPoint(new Coordinate(x1,y1));
    }

    public int getId(){
        return id;
    }

    public Geometry getGeometry(){
        if(this.rect != null)
            return this.rect;
        else
            return this.point;
    }

    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("#.#####");
        if(rect != null){
            return id+":"+df.format(x1)+","+df.format(y1)+","+df.format(x2)+","+df.format(y2);
        }else{
            return id+":"+df.format(x1)+","+df.format(y1);
        }
    }

}
