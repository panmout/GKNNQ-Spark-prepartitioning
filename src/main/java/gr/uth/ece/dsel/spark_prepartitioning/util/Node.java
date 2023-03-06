package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.io.Serializable;
//import java.util.HashSet;

public final class Node implements Serializable
{
	private int low, high; // lower, higher index of sample array
	private Node nw, ne, sw, se; // children
	private double xmin, xmax, ymin, ymax; // node boundaries
	//private HashSet<Integer> contPoints = new HashSet<Integer>(); // points contained
	
	// constructor
	public Node (double xmin, double ymin, double xmax, double ymax)
	{
		this.xmin = xmin;
		this.xmax = xmax;
		this.ymin = ymin;
		this.ymax = ymax;
	}
	
	public int getLow()
	{
		return this.low;
	}

	public void setLow(int low)
	{
		this.low = low;
	}

	public int getHigh()
	{
		return this.high;
	}

	public void setHigh(int high)
	{
		this.high = high;
	}
	
	public double getXmin() {
		return this.xmin;
	}

	public void setXmin(double xmin) {
		this.xmin = xmin;
	}

	public double getXmax() {
		return this.xmax;
	}

	public void setXmax(double xmax) {
		this.xmax = xmax;
	}

	public double getYmin() {
		return this.ymin;
	}

	public void setYmin(double ymin) {
		this.ymin = ymin;
	}

	public double getYmax() {
		return this.ymax;
	}

	public void setYmax(double ymax) {
		this.ymax = ymax;
	}

	public Node getNW() {
		return this.nw;
	}

	public void setNW(Node nW) {
		this.nw = nW;
	}

	public Node getNE() {
		return this.ne;
	}

	public void setNE(Node nE) {
		this.ne = nE;
	}

	public Node getSW() {
		return this.sw;
	}

	public void setSW(Node sW) {
		this.sw = sW;
	}

	public Node getSE() {
		return this.se;
	}

	public void setSE(Node sE) {
		this.se = sE;
	}
	
	/*
	public final void addPoints(int i)
	{
		this.contPoints.add(i);
	}
	
	public final void removePoints()
	{
		if (this.nw != null)
		{
			this.nw.removePoints();
			this.nw.contPoints.clear();
		}
		if (this.ne != null)
		{
			this.ne.removePoints();
			this.ne.contPoints.clear();
		}
		if (this.sw != null)
		{
			this.sw.removePoints();
			this.sw.contPoints.clear();
		}
		if (this.se != null)
		{
			this.se.removePoints();
			this.se.contPoints.clear();
		}
	}

	public final HashSet<Integer> getContPoints() {
		return this.contPoints;
	}

	public final void setContPoints(HashSet<Integer> contPoints) {
		this.contPoints = new HashSet<Integer>(contPoints);
	}
	
	public final void removePoint(int i) {
		this.contPoints.remove(i);
	}
	*/
}
