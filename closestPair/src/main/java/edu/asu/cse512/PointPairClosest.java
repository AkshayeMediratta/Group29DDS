package edu.asu.cse512;

import java.io.Serializable;

public class PointPairClosest implements Comparable<PointPairClosest>,
		Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	PointClosest p1;
	PointClosest p2;

	public PointPairClosest(PointClosest point1, PointClosest point2) {
		this.p1 = point1;
		this.p2 = point2;
	}

	public PointClosest getP1() {
		return p1;
	}

	public void setP1(PointClosest p1) {
		this.p1 = p1;
	}

	public PointClosest getP2() {
		return p2;
	}

	public void setP2(PointClosest p2) {
		this.p2 = p2;
	}

	public double getDistance() {
		return Math.sqrt(Math.pow(p2.getY() - p1.getY(), 2)
				+ Math.pow(p2.getX() - p1.getX(), 2));
	}

	public boolean isSamePoint() {
		if (this.p1.getX() == this.p2.getX()
				&& this.p1.getY() == this.p2.getY()) {
			return true;
		}
		return false;
	}

	public int compareTo(PointPairClosest o) {
		// TODO Auto-generated method stub
		if (this.getDistance() - o.getDistance() > 0.0) {
			return 1;
		}
		return -1;
	}
}
