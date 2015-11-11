package edu.asu.cse512;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/**
 * 
 * Hello world!
 *
 */
public class RangeQuery {

	public static void main(String[] args) {

		//String inputLocation1 = "hdfs://192.168.139.149:54310/harsh/RangeQueryTestData.csv";
		//String inputLocation2 = "hdfs://192.168.139.149:54310/harsh/RangeQueryRectangle.csv";
		//String outputLocation = "hdfs://192.168.139.149:54310/harsh/RangeQueryResult.csv";

		spatialRangeQuery(args[0], args[1], args[2]);
	}

	private static void spatialRangeQuery(String inputLocation1, String inputLocation2, String outputLocation) {
		SparkConf conf = new SparkConf().setAppName("SpatialRangeQuery Application");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Read the input csv file holding set of polygons in a rdd of string objects
		JavaRDD<String> inputPoints = sc.textFile(inputLocation1);

		// Map the above rdd of strings to a rdd of points
		// This is done by splitting the rows of file by ‘,’ and then converting them to individual points.

		JavaRDD<PointRange> pointRDD = inputPoints.map(mapInputStringToPointRDD());

		// Repeat the above process but now for initializing the rdd for query window
		JavaRDD<String> queryRect = sc.textFile(inputLocation2);
		
		// Map the query window to RDD object
		JavaRDD<RectangleRange> queryRDD = queryRect.map(new Function<String, RectangleRange>() {
			public RectangleRange call(String inputString) {
				String[] points = inputString.split(",");

				Double leftMostUpperXCoord = Math.min(Double.parseDouble(points[0]), Double.parseDouble(points[2]));
				Double leftMostUpperYCoord = Math.max(Double.parseDouble(points[1]), Double.parseDouble(points[3]));

				PointRange upperLeftPoint = new PointRange(leftMostUpperXCoord, leftMostUpperYCoord);
				Double rightMostLowerXCoord = Math.max(Double.parseDouble(points[0]), Double.parseDouble(points[2]));
				Double rightMostLowerYCoord = Math.min(Double.parseDouble(points[1]), Double.parseDouble(points[3]));
				PointRange lowerRightPoint = new PointRange(rightMostLowerXCoord, rightMostLowerYCoord);
				return new RectangleRange(0, upperLeftPoint, lowerRightPoint);
			}
		});

		// Broadcast the query window to each of the worker
		final Broadcast<RectangleRange> queryWindow = sc.broadcast(queryRDD.first());
		
		// Filter the RDD for the input rectangles formed earlier based upon the query window
		// by utilizing the isRectangleinsideQueryWindow() method as described previously
		// while creating the Rectangle class.

		JavaRDD<PointRange> rangeQueryRDD = pointRDD.filter(new Function<PointRange, Boolean>() {
			public Boolean call(PointRange point) throws Exception {
				return point.isPointinsideQueryWindow(queryWindow.value());
			}
		});

		// Save the result RDD object as a file on HDFS

		JavaRDD<String> result = rangeQueryRDD.map(new Function<PointRange, String>() {
			public String call(PointRange inputPoint) {
				return inputPoint.getPointID().toString();
			}
		});

		result.coalesce(1).saveAsTextFile(outputLocation);
		sc.close();
	}

	public static Function<String, PointRange> mapInputStringToPointRDD() {
		return new Function<String, PointRange>() {
			public PointRange call(String inputString) {
				String[] points = inputString.split(",");

				// Initialize the point by the above pair
				return new PointRange(Integer.parseInt(points[0]), Double.parseDouble(points[1]),
						Double.parseDouble(points[2]));

			}
		};
	}
}
