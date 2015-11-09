import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.SparkConf;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFactory;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.algorithm.ConvexHull;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class ConvexHullPoints 
{
    public static void getConvexPoints( JavaSparkContext sc )
    {
    	JavaRDD<String> lines = sc.textFile("/home/vamseedhar/Downloads/Test Case/ConvexHullTestData.csv");
    	JavaRDD<Coordinate> coordinateList = lines.repartition(1).map(new GetInput());   	
    	JavaRDD<Coordinate> hullPoints = coordinateList.mapPartitions(new ConvexH());
    	List<Coordinate> hullPointsList = hullPoints.collect();
    	JavaRDD<Coordinate> hullPointsRDD = sc.parallelize(hullPointsList, 1);
    	JavaRDD<String> hullPointsString = hullPointsRDD.repartition(1).map(new Function<Coordinate, String>(){

			public String call(Coordinate hullPoint) throws Exception {
				// TODO Auto-generated method stub
				return hullPoint.x+","+hullPoint.y;
			}
    		
    	});
    	
    	hullPointsString.saveAsTextFile("/home/vamseedhar/Downloads/ConvexHullResultData.csv");
    			 
    }
}

class GlobalConvexH implements Function2<Coordinate[], Coordinate[], Coordinate[]> {

	public Coordinate[] call(Coordinate[] pointArray1, Coordinate[] pointArray2)
			throws Exception {
		Coordinate[] inputPoints = new Coordinate[pointArray1.length + pointArray2.length];
    	ConvexHull convexHull = new ConvexHull(inputPoints, new GeometryFactory());
    	Geometry convexHullGeometry = convexHull.getConvexHull();
    	System.out.println("-------"+convexHullGeometry);
    	Coordinate[] convexResult = convexHullGeometry.getCoordinates();
		
		return convexResult;
	}
	
}

class ConvexH implements FlatMapFunction<Iterator<Coordinate>, Coordinate>

{
	public Iterable<Coordinate> call(Iterator<Coordinate> coordinatesIterator) throws Exception {
		// TODO Auto-generated method stub
		List<Coordinate> coorList = new ArrayList<Coordinate>();
		while(coordinatesIterator.hasNext()){
			coorList.add(coordinatesIterator.next());
		}
    	Coordinate[] coorArray = new Coordinate[coorList.size()];
    	int i = 0;
    	for(Coordinate c: coorList){
    		coorArray[i] = c;
    		i++;
    	}
    	ConvexHull convexHull = new ConvexHull(coorArray, new GeometryFactory());
    	Geometry convexHullGeometry = convexHull.getConvexHull();
    	Coordinate[] convexResult = convexHullGeometry.getCoordinates();
    	return Arrays.asList(convexResult);
	}
    
}


class GetInput implements Function<String, Coordinate> {

	public Coordinate call(String s) throws Exception {
		// TODO Auto-generated method stub
		return splitStringtoPoint(s);
	}
	
	private Coordinate splitStringtoPoint(String s) {
        String[] temp = s.split(",");
        return new Coordinate(Double.parseDouble(temp[0]), Double.parseDouble(temp[1]));
        
    }
	
}
