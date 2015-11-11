package edu.asu.cse512;

import java.io.Serializable;

public class PointJoin implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5501181546920101976L;
	public Double x;
	public Double y;
	private Integer pointID;

	public PointJoin(Double x1, Double y1) {
		x = x1;
		y = y1;
	}

	public PointJoin(Integer pointID, Double x1, Double y1) {
		this.pointID = pointID;
		x = x1;
		y = y1;
	}

	public Double getY() {
		return y;
	}

	public Double getX() {
		return x;
	}

	public void setY(Double y) {
		this.y = y;
	}

	public void setX(Double x) {
		this.x = x;
	}

	public Integer getPointID() {
		return pointID;
	}

	public void setPointID(Integer pointID) {
		this.pointID = pointID;
	}

	// This method is used for determining whether the point lies inside
	// the rectangle or not.

	public Boolean isPointinsideQueryWindow(RectangleJoin rect) {
		Boolean isInside;
		// Check if the point is inside the given rectangle or not

		if (this.x >= rect.getUpperLeftPoint().getX()
				&& this.y <= rect.getUpperLeftPoint().getY()
				&& this.x <= rect.getLowerRightPoint().getX()
				&& this.y >= rect.getLowerRightPoint().getY()) {
			isInside = true;
		} else {
			isInside = false;
		}
		return isInside;
	}
}