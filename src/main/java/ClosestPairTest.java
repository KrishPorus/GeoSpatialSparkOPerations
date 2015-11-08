import java.io.*;
import java.util.ArrayList;

/**
 * Created by VamshiKrishna on 10/15/2015.
 */
public class ClosestPairTest {
    public static void main(String[] args) {
        ArrayList<ClosestPairPoint> pointList = new ArrayList<ClosestPairPoint>();
        try {
//            BufferedWriter bW = new BufferedWriter(new FileWriter("data.txt"));
//            Random r = new Random();
//            for (int i = 0; i < 10; i++) {
//                int x = r.nextInt(r.nextInt(10)+1);
//                int y = r.nextInt(r.nextInt(10)+1);
//                bW.write(x+","+y+"\n");
//            }
//            bW.close();
            BufferedReader bR = new BufferedReader(new FileReader("data.txt"));
            String points;
            while ((points = bR.readLine()) != null) {
                String[] coords = points.split(",");
                ClosestPairPoint p = new ClosestPairPoint(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
                pointList.add(p);
            }

            ClosestPair cP = ClosestPair.findClosestPair(pointList);
            ClosestPairPoint leftPoint = cP.getLeftPoint();
            ClosestPairPoint rightPoint = cP.getRightPoint();
            System.out.println("Closest Pair = (" + leftPoint.getxCoord() + "," + leftPoint.getyCoord() + ")" +
                    " (" + rightPoint.getxCoord() + "," + rightPoint.getyCoord() + ")");
            double xDiff = leftPoint.getxCoord() - rightPoint.getxCoord();
            double yDiff = leftPoint.getyCoord() - rightPoint.getyCoord();
            double delta = Math.sqrt((xDiff*xDiff) + (yDiff*yDiff));

            ArrayList<ClosestPairPoint> finalList = ConvexHullBuffer.findBoundaryPoints(pointList, delta, cP);

        }
        catch (FileNotFoundException e) {
            System.out.println("Unable to open file");
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
