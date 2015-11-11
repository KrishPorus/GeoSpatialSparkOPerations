
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFactory;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory;
import com.vividsolutions.jts.algorithm.ConvexHull;

import java.util.*;

public class ConvexHullPoints 
{
    public static void getConvexPoints(JavaSparkContext sc )
    {
    	JavaRDD<String> lines = sc.textFile("/home/vamseedhar/Downloads/Test Case/ConvexHullTestData.csv");
    	JavaRDD<Coordinate> hullPointsRDD = lines.mapPartitions(new ConvexH());
    	List<Coordinate> hullPointsList = hullPointsRDD.collect();
    	Coordinate[] inputArray = new Coordinate[hullPointsList.size()];
    	int j = 0;
    	for(Coordinate c: hullPointsList) {
    		inputArray[j] = c;
    		System.out.println(c.toString());
    		j++;
    	}
    	GeometryFactory geoFactory1 = new GeometryFactory();
    	MultiPoint mPoint1 = geoFactory1.createMultiPoint(inputArray);
    	Geometry geo1 = mPoint1.convexHull();
    	Coordinate[] convexHullResult = geo1.getCoordinates();
    	System.out.println(convexHullResult.length);
    	for(int i = 0; i< convexHullResult.length; i++){
    		System.out.println("x: "+((Coordinate)convexHullResult[i]).x+"y: "+((Coordinate)convexHullResult[i]).y);
    	}
    	
    	JavaRDD<Coordinate> convexHullResultRDD = sc.parallelize(Arrays.asList(convexHullResult), 1);
    	JavaRDD<String> convexHullResultString = convexHullResultRDD.repartition(1).map(new Function<Coordinate, String>(){
			public String call(Coordinate hullPoint) throws Exception {
				// TODO Auto-generated method stub
				return hullPoint.x+","+hullPoint.y;
			}
    		
    	});
    	convexHullResultString.repartition(1).saveAsTextFile("/home/vamseedhar/Downloads/ConvexHullResultData.csv");
    }
}


@SuppressWarnings("serial")
class ConvexH implements FlatMapFunction<Iterator<String>, Coordinate>

{
	public Iterable<Coordinate> call(Iterator<String> coordinatesIterator) throws Exception {
		// TODO Auto-generated method stub
		List<Coordinate> coorList = new ArrayList<Coordinate>();
		while(coordinatesIterator.hasNext()){
			String[] temp = coordinatesIterator.next().split(",");
			coorList.add(new Coordinate(Double.parseDouble(temp[0]), Double.parseDouble(temp[1])));
		}
    	Coordinate[] coorArray = new Coordinate[coorList.size()];
     	int i = 0;
    	for(Coordinate c: coorList){
    		coorArray[i] = c;
    		i++;
    	}
    	
    	GeometryFactory geoFactory = new GeometryFactory();
    	MultiPoint mPoint = geoFactory.createMultiPoint(coorArray);
    	Geometry geo = mPoint.convexHull();
    	Coordinate[] convexResult = geo.getCoordinates();
    	return Arrays.asList(convexResult);
	}
    
}


