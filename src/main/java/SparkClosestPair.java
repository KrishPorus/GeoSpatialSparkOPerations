/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SparkClosestPair {

  public static void closestPairFactory(JavaSparkContext ctx) {

    JavaRDD<String> lines = ctx.textFile("hdfs://master:54310/user/closestpair/cl_1.csv");

    JavaRDD<ClosestPairPoint> points = lines.mapPartitions(new FlatMapFunction<Iterator<String>, ClosestPairPoint>() {
      public Iterable<ClosestPairPoint> call(Iterator<String> stringIterator) throws Exception {
        ArrayList<ClosestPairPoint> pointList = new ArrayList<ClosestPairPoint>();
        while (stringIterator.hasNext()) {
          String nextString = stringIterator.next();
          String[] coords = nextString.split(",");
          ClosestPairPoint p = new ClosestPairPoint(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
          pointList.add(p);
        }
        System.out.println(pointList.size());
        ClosestPair cP = ClosestPair.findClosestPair(pointList);
        ClosestPairPoint leftPoint = cP.getLeftPoint();
        ClosestPairPoint rightPoint = cP.getRightPoint();
        System.out.println("Closest Pair = (" + leftPoint.getxCoord() + "," + leftPoint.getyCoord() + ")" +
                " (" + rightPoint.getxCoord() + "," + rightPoint.getyCoord() + ")");
        double xDiff = leftPoint.getxCoord() - rightPoint.getxCoord();
        double yDiff = leftPoint.getyCoord() - rightPoint.getyCoord();
        double delta = Math.sqrt((xDiff * xDiff) + (yDiff * yDiff));
        System.out.println(delta);

        return ConvexHullBuffer.findBoundaryPoints(pointList, delta, cP);
      }
    });

    List<ClosestPairPoint> output = points.collect();
      System.out.println(output.size());
      JavaRDD<ClosestPairPoint> finalPoints = ctx.parallelize(output).repartition(1);
    JavaRDD<ClosestPairPoint> closestPair = finalPoints.mapPartitions(new FlatMapFunction<Iterator<ClosestPairPoint>, ClosestPairPoint>() {
      public Iterable<ClosestPairPoint> call(Iterator<ClosestPairPoint> pointIterator) throws Exception {
        ArrayList<ClosestPairPoint> pointList = new ArrayList<ClosestPairPoint>();
        while (pointIterator.hasNext()) {
          pointList.add(pointIterator.next());
        }
        ClosestPair cP = ClosestPair.findClosestPair(pointList);
        ClosestPairPoint leftPoint = cP.getLeftPoint();
        ClosestPairPoint rightPoint = cP.getRightPoint();
        ArrayList<ClosestPairPoint> closestPair = new ArrayList<ClosestPairPoint>();
        closestPair.add(leftPoint);
        closestPair.add(rightPoint);
        return closestPair;
      }
    });
    List<ClosestPairPoint> closesPairPoints = closestPair.collect();
    closestPair.saveAsTextFile("hdfs://master:54310/user/output/closestpair");
    for (ClosestPairPoint p : closesPairPoints) {
      System.out.println(p.getxCoord()+","+p.getyCoord());
    }
    ctx.stop();
  }
}
