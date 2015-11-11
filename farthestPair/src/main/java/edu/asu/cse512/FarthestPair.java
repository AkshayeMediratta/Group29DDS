package src.main.java.edu.asu.cse512;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Hello world!
 *
 */
public class FarthestPair {

	public static void geometryFarthestPair(String inputLocation,
			String outputLocation) {
		SparkConf conf = new SparkConf().setAppName("Group29-FarthestPair");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> inputData = sc.textFile(inputLocation).cache();
		JavaRDD<String> pointsData = inputData
				.filter(new Function<String, Boolean>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Boolean call(String s) {
						return !s.contains("x");
					}
				});

		JavaRDD<Point> points = pointsData.map(new Function<String, Point>() {
			public Point call(String row) {
				String[] xy = row.split(",");
				return new Point(Double.parseDouble(xy[0]), Double
						.parseDouble(xy[1]));
			}
		});
		final List<Point> listPoints = points.collect();
		sc.broadcast(listPoints);
		JavaRDD<PointPair> pairs = points.map(new Function<Point, PointPair>() {
			private static final long serialVersionUID = 1L;

			public PointPair call(Point point) {
				double maxDist = 0.0;
				PointPair maxDistPointPair = null;
				for (Point hullPoint : listPoints) {
					PointPair pointPair = new PointPair(point, hullPoint);
					double dist = pointPair.getDistance();
					if (dist > maxDist) {
						maxDist = dist;
						maxDistPointPair = pointPair;
					}
				}
				return maxDistPointPair;
			}
		});

		// JavaPairRDD<PointPair, Double> finalPoints = pairs.sortByKey(false);
		// Tuple2<PointPair, Double> input = finalPoints.first();
		PointPair finalPoints = pairs
				.reduce(new Function2<PointPair, PointPair, PointPair>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public PointPair call(PointPair point1, PointPair point2)
							throws Exception {
						if (point1.getDistance() > point2.getDistance()) {
							return point1;
						}
						return point2;
					}
				});
		List<String> listFinalPoints = new ArrayList<String>();
		String point1 = finalPoints.p1.getX().toString() + ","
				+ finalPoints.p1.getY().toString();
		String point2 = finalPoints.p2.getX().toString() + ","
				+ finalPoints.p2.getY().toString();
		listFinalPoints.add(point1);
		listFinalPoints.add(point2);
		Collections.sort(listFinalPoints);
		JavaRDD<String> finalRDD = sc.parallelize(listFinalPoints).coalesce(1);
		finalRDD.saveAsTextFile(outputLocation);
		sc.close();
	}

	/*
	 * Main function, take two parameter as input, output
	 * 
	 * @param inputLocation
	 * 
	 * @param outputLocation
	 */
	public static void main(String[] args) {
		// Initialize, need to remove existing in output file location.

		// Implement
		FarthestPair.geometryFarthestPair(args[0], args[1]);

		// Output your result, you need to sort your result!!!
		// And,Don't add a additional clean up step delete the new generated
		// file...
	}
}
