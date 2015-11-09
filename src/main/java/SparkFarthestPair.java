package geospatial.operation5;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Hello world!
 *
 */

class PointPair implements Serializable{
    private Coordinate p1;
    private Coordinate p2;
    private double pointDistance;
    public PointPair(Coordinate p1, Coordinate p2) {
        this.p1 = p1;
        this.p2 = p2;
    }
    public double distance() {
        return p1.distance(p2);
    }
    
    public double getPointDistance() {
        return pointDistance;
    }
    public String getCoordinates()
    {
    	return p1.x +","+ p1.y +"\n" +p2.x+","+p2.y;
    }
    public void setPointDistance(double pointDistance) {
        this.pointDistance = pointDistance;
    }
}

public class SparkFarthestPair
{
    public static void sparkfarthestpair(JavaSparkContext sc ) 
    {

        JavaRDD<String> textFile = sc.textFile("FarthestPairTestData.csv");
        JavaRDD<Coordinate> coordinates = textFile.flatMap(
                new FlatMapFunction<String, Coordinate>() {
                    public Iterable<Coordinate> call(String s) {
                        String[] coordinate = s.split(",");
                        double xCoord = Double.parseDouble(coordinate[0]);
                        double yCoord = Double.parseDouble(coordinate[1]);
                        return Arrays.asList(new Coordinate(xCoord,yCoord));
                    }
                }
        );
        JavaRDD<Coordinate> hullPoints = coordinates.mapPartitions(new myConvexHull());
        JavaRDD<Coordinate> reducedPoints = hullPoints.repartition(1);
        JavaRDD<Coordinate> finalPoints = reducedPoints.mapPartitions(new myConvexHull());
        List<Coordinate> convexHullPoints = finalPoints.collect();
        Coordinate[] chPoints = convexHullPoints.toArray(new Coordinate[convexHullPoints.size()]);
        Coordinate p1,p2;
        List<PointPair> allPairs = new ArrayList<PointPair>();

        for (int i = 0; i < chPoints.length-1; i++) {
            for (int j = i+1; j< chPoints.length; j++) {
                PointPair obj = new PointPair(chPoints[i], chPoints[j]);
                allPairs.add(obj);
            }
        }
        JavaRDD<PointPair> pointPairs = sc.parallelize(allPairs);
        JavaRDD<PointPair> farthestPair = pointPairs.mapPartitions(new CalculateDistance());
        List<PointPair> farthestPointPairs = farthestPair.collect();
        Iterator<PointPair> itr = farthestPointPairs.iterator();
        double maxDistance = 0;
        PointPair result = null;
        while (itr.hasNext()) {
            PointPair obj = itr.next();
            if (obj.getPointDistance() > maxDistance) {
                maxDistance = obj.getPointDistance();
                result = obj;
            }
        }
        ArrayList<String> listOfPoints = new ArrayList();
        listOfPoints.add(result.getCoordinates());
        JavaRDD<String> finalresult = sc.parallelize(listOfPoints);
        
        finalresult.repartition(1).saveAsTextFile("farthest.csv");
    }

    static class myConvexHull implements FlatMapFunction<Iterator<Coordinate>, Coordinate>, Serializable
    {
        public Iterable<Coordinate> call(Iterator<Coordinate> pointIterator) throws Exception {
            List<Coordinate> currentPoints = new ArrayList<Coordinate>();
            try {
                while (pointIterator.hasNext()) {
                    currentPoints.add(pointIterator.next());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // Create a convexHull class with constructor
            GeometryFactory geomFactory = new GeometryFactory();
            ConvexHull ch = new ConvexHull(currentPoints.toArray(new Coordinate[currentPoints.size()]), geomFactory);
            // get coordinates of the convex hull using getConvexHull
            Geometry geometry = ch.getConvexHull();
            Coordinate[] c = geometry.getCoordinates();

            //Convert the coordinates array to arraylist here
            List<Coordinate> a = Arrays.asList(c);
            return a;
        }
    }

    private static class CalculateDistance implements FlatMapFunction<Iterator<PointPair>, PointPair> {
        public Iterable<PointPair> call(Iterator<PointPair> pointPairIterator) throws Exception {
            ArrayList<PointPair> farthestPair = new ArrayList<PointPair>();
            PointPair farthestPairPoint = null;
            try {
                double maxDistance = 0;
                while (pointPairIterator.hasNext()) {
                    PointPair obj = pointPairIterator.next();
                    obj.setPointDistance(obj.distance());
                    if (maxDistance < obj.getPointDistance()) {
                        maxDistance = obj.getPointDistance();
                        farthestPairPoint = obj;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            farthestPair.add(farthestPairPoint);
            return farthestPair;
        }
    }
}
