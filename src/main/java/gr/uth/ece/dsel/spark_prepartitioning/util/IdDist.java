package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.io.Serializable;

// class IdDist (int pid, double distance) with constructor and set-get methods

public final class IdDist implements Serializable
{
	private int pid;
	private double dist;
	
	public IdDist(int pid1, double dist1)
	{
		setId(pid1);
		setDist(dist1);
	}
	
	public void setId(int pid1)
	{
		this.pid = pid1;
	}
	
	public void setDist(double dist1)
	{
		this.dist = dist1;
	}
	
	public int getId()
	{
		return this.pid;
	}
	
	public double getDist()
	{
		return this.dist;
	}
}
