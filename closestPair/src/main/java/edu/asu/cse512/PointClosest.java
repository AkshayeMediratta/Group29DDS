package edu.asu.cse512;

import java.io.Serializable;

public class PointClosest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5501181546920101976L;
	public Double x;
	public Double y;

	public PointClosest(Double x1, Double y1) {
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
}
