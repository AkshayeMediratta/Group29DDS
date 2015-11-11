package edu.asu.cse512;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class Join {

	public static void main(String[] args) {

		// String inputLocation1 =
		// "hdfs://192.168.139.149:54310/harsh/JoinQueryInput1.csv";
		// String inputLocation1 =
		// "hdfs://192.168.139.149:54310/harsh/JoinQueryInput3.csv";
		// String inputLocation2 =
		// "hdfs://192.168.139.149:54310/harsh/JoinQueryInput2.csv";
		// String outputLocation =
		// "hdfs://192.168.139.149:54310/harsh/JoinQueryResult.csv";
		// String inputType = "point";

		// Call the spatial join function passing the above parameters
		spatialJoinQuery(args[0], args[1], args[2], args[3]);
	}

	private static void spatialJoinQuery(String inputLocation1,
			String inputLocation2, String outputLocation, String inputType) {

		SparkConf conf = new SparkConf()
				.setAppName("SpatialJoinQuery Application");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the input csv file holding set of polygons in a rdd of string
		// objects
		JavaRDD<String> firstInputPoints = sc.textFile(inputLocation1);

		// Map the above rdd of strings to a rdd of rectangles for the first
		// input

		// Repeat the above process but now for initializing the rdd for query
		// window
		JavaRDD<String> secondInputPoints = sc.textFile(inputLocation2);

		JavaRDD<Tuple2<Integer, ArrayList<Integer>>> joinQueryRDD = new JavaRDD<Tuple2<Integer, ArrayList<Integer>>>(
				null, null);

		// Process this if condition if the first input holds the rectangle
		// initialization points
		if (inputType.equalsIgnoreCase("rectangle")) {

			// Map the first set of inputs to a RDD (holds the initialization
			// points for rectangle)
			final JavaRDD<RectangleJoin> firstInputRDD = firstInputPoints
					.map(mapInputStringToRectRDD());

			// Map the query window to RDD object
			final JavaRDD<RectangleJoin> secondInputRDD = secondInputPoints
					.map(mapInputStringToRectRDD());

			// broadcast the second set of rectangles (query windows) to each of
			// the workers
			final Broadcast<List<RectangleJoin>> firstInput = sc
					.broadcast(firstInputRDD.collect());

			// map the id of second input to the multiple id’s of the first
			// input if they contain the
			// second rectangle
			joinQueryRDD = secondInputRDD
					.map(new Function<RectangleJoin, Tuple2<Integer, ArrayList<Integer>>>() {
						public Tuple2<Integer, ArrayList<Integer>> call(
								RectangleJoin rectangle) throws Exception {

							// Get the list of rectangles from the second RDD
							// input.
							List<RectangleJoin> firstInputCollection = firstInput
									.value();

							ArrayList<Integer> firstInputIds = new ArrayList<Integer>();

							// Iterate the second input and check for the second
							// set of
							// rectangle id’s
							// that hold the rectangle from first set obtained
							// from the
							// mapped RDD
							for (RectangleJoin firstRects : firstInputCollection) {
								if (rectangle
										.isRectangleinsideQueryWindow(firstRects)
										|| rectangle.isOverlap(firstRects)) {
									firstInputIds.add(firstRects
											.getRectangleId());
								}
							}

							// Create a new tuple of the mapped values and
							// return back
							// the mapped transformation.
							Tuple2<Integer, ArrayList<Integer>> resultList = new Tuple2<Integer, ArrayList<Integer>>(
									rectangle.getRectangleId(), firstInputIds);
							return resultList;
						}
					});

		} else if (inputType.equalsIgnoreCase("point")) {

			// Map the first set of input points to a point RDD
			final JavaRDD<PointJoin> firstInputRDD = firstInputPoints
					.map(mapInputStringToPointRDD());

			// broadcast the first input to each of the workers
			final Broadcast<List<PointJoin>> firstInput = sc
					.broadcast(firstInputRDD.collect());

			// Map the query window to RDD object
			final JavaRDD<RectangleJoin> secondInputRDD = secondInputPoints
					.map(mapInputStringToRectRDD());

			joinQueryRDD = secondInputRDD
					.map(new Function<RectangleJoin, Tuple2<Integer, ArrayList<Integer>>>() {
						public Tuple2<Integer, ArrayList<Integer>> call(
								RectangleJoin rectangle) throws Exception {

							// Get the list of rectangles from the second RDD
							// input.
							List<PointJoin> firstInputCollection = firstInput
									.getValue();
							ArrayList<Integer> secondInputIds = new ArrayList<Integer>();

							// Iterate the first input and check for the second
							// set of
							// rectangle id’s
							// that hold the points from first set obtained from
							// the
							// mapped RDD
							for (PointJoin point : firstInputCollection) {
								if (point.isPointinsideQueryWindow(rectangle)) {
									secondInputIds.add(point.getPointID());
								}
							}
							// Create a new tuple of the mapped values and
							// return back
							// the mapped transformation.
							Tuple2<Integer, ArrayList<Integer>> resultList = new Tuple2<Integer, ArrayList<Integer>>(
									rectangle.getRectangleId(), secondInputIds);
							return resultList;
						}
					});

		}

		// map the result of the join query to a list of strings holding the
		// mapped values
		JavaRDD<String> result = joinQueryRDD
				.map(new Function<Tuple2<Integer, ArrayList<Integer>>, String>() {
					public String call(
							Tuple2<Integer, ArrayList<Integer>> inputPoint) {

						Integer containingRect = inputPoint._1();
						ArrayList<Integer> containedRects = inputPoint._2();

						StringBuffer intermediateBuffer = new StringBuffer();

						intermediateBuffer.append(containingRect);

						for (Integer rects : containedRects) {
							if (rects != -1) {
								intermediateBuffer.append("," + rects);
							} else {
								intermediateBuffer.append(",");
							}
						}

						return intermediateBuffer.toString();
					}
				});

		// save the result to a text file
		result.coalesce(1).saveAsTextFile(outputLocation);

		sc.close();
	}

	private static Function<String, RectangleJoin> mapInputStringToRectRDD() {
		return new Function<String, RectangleJoin>() {
			public RectangleJoin call(String inputString) {
				// Read the file in an array of string object indicating each
				// point.
				String[] points = inputString.split(",");

				// Initialize the leftmost x and y coordinate
				Double leftMostUpperXCoord = Math.min(
						Double.parseDouble(points[1]),
						Double.parseDouble(points[3]));
				Double leftMostUpperYCoord = Math.max(
						Double.parseDouble(points[2]),
						Double.parseDouble(points[4]));

				// holds the upper left point for the rectangle
				PointJoin upperLeftPoint = new PointJoin(leftMostUpperXCoord,
						leftMostUpperYCoord);
				Double rightMostLowerXCoord = Math.max(
						Double.parseDouble(points[1]),
						Double.parseDouble(points[3]));
				Double rightMostLowerYCoord = Math.min(
						Double.parseDouble(points[2]),
						Double.parseDouble(points[4]));

				// holds the lower right point for the rectangle
				PointJoin lowerRightPoint = new PointJoin(rightMostLowerXCoord,
						rightMostLowerYCoord);
				return new RectangleJoin(Integer.parseInt(points[0]),
						upperLeftPoint, lowerRightPoint);
			}
		};
	}

	public static Function<String, PointJoin> mapInputStringToPointRDD() {
		return new Function<String, PointJoin>() {
			public PointJoin call(String inputString) {
				String[] points = inputString.split(",");

				// Initialize the point by the above pair
				return new PointJoin(Integer.parseInt(points[0]),
						Double.parseDouble(points[1]),
						Double.parseDouble(points[2]));

			}
		};
	}
}
