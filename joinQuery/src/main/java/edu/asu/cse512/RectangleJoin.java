package edu.asu.cse512;

import java.io.Serializable;

public class RectangleJoin implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8525368746044775440L;
	// holds unique id assigned to each rectangleint rectangleId;
	int rectangleId;
	// upper left point for the rectangle
	PointJoin upperLeftPoint;
	// lower right point for the rectangle
	PointJoin lowerRightPoint;

	public int getRectangleId() {
		return rectangleId;
	}

	public void setRectangleId(int rectangleId) {
		this.rectangleId = rectangleId;
	}

	public PointJoin getUpperLeftPoint() {
		return upperLeftPoint;
	}

	public void setUpperLeftPoint(PointJoin upperLeftPoint) {
		this.upperLeftPoint = upperLeftPoint;
	}

	public PointJoin getLowerRightPoint() {
		return lowerRightPoint;
	}

	public void setLowerRightPoint(PointJoin lowerRightPoint) {
		this.lowerRightPoint = lowerRightPoint;
	}

	// Constructor for initializing the rectangle object
	RectangleJoin(int inputId, PointJoin inUpperLeftPoint,
			PointJoin inLowerRightPoint) {
		this.rectangleId = inputId;
		this.upperLeftPoint = inUpperLeftPoint;
		this.lowerRightPoint = inLowerRightPoint;
	}

	// This method is used for determining whether the query window holds the
	// given rectangle or not.

	public Boolean isRectangleinsideQueryWindow(RectangleJoin rect) {
		Boolean isInside;
		// Check if the query window x and y coordinate for the upper left point
		// and
		// the lower right point enclose the given rectangle or not.
		if (upperLeftPoint.x <= rect.upperLeftPoint.x
				&& upperLeftPoint.y >= rect.upperLeftPoint.y
				&& lowerRightPoint.x >= rect.lowerRightPoint.x
				&& lowerRightPoint.y <= rect.lowerRightPoint.y) {
			isInside = true;
		} else {
			isInside = false;
		}
		return isInside;
	}

	public Boolean isOverlap(RectangleJoin rect) {

		// If one rectangle is on left side of other
		if (upperLeftPoint.x > rect.lowerRightPoint.x
				|| rect.upperLeftPoint.x > lowerRightPoint.x)
			return false;

		// If one rectangle is above other
		if (upperLeftPoint.y < rect.lowerRightPoint.y
				|| rect.upperLeftPoint.y < lowerRightPoint.y)
			return false;

		return true;
	}
}
