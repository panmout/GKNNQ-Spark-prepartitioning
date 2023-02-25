/*
package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

public class NeighborsAccumulatorV2 extends AccumulatorV2<Iterable<Point>, PriorityQueue<IdDist>>
{
	private int k;
	private ArrayList<Point> qpoints;
	private double[] mbrCentroid;
	private PriorityQueue<IdDist> neighbors;
	private ArrayList<Point> tpoints;
	private boolean fastsums;
	private String method;
	private BfNeighbors bfn;
	private PsNeighbors psn;
	private LongAccumulator dpc_count;
	private LongAccumulator sumdist;
	private LongAccumulator sumdx;
	private LongAccumulator sumdx_success;
	private LongAccumulator skipped_tpoints;
	private LongAccumulator total_tpoints;
	
	public NeighborsAccumulatorV2(int k, double[] mbrC, ArrayList<Point> qp, PriorityQueue<IdDist> pq, boolean fs, String method, LongAccumulator dpc_count, LongAccumulator sumdist, LongAccumulator sumdx, LongAccumulator sumdx_success, LongAccumulator skipped_tpoints, LongAccumulator total_tpoints)
	{
		this.k = k;
		this.qpoints = new ArrayList<Point>(qp);
		this.mbrCentroid = Arrays.copyOf(mbrC, mbrC.length);
		this.neighbors = new PriorityQueue<IdDist>(pq);
		this.fastsums = fs;
		this.method = method;
		this.dpc_count = dpc_count;
		this.sumdist = sumdist;
		this.sumdx = sumdx;
		this.sumdx_success = sumdx_success;
		this.skipped_tpoints = skipped_tpoints;
		this.total_tpoints = total_tpoints;
	}

	@Override
	public void add(Iterable<Point> it)
	{
		Iterator<Point> iterator = it.iterator();
	    
	    this.tpoints = new ArrayList<Point>();
	    
	    // fill tpoints list
	    while (iterator.hasNext())
	    {
	    	this.tpoints.add(iterator.next());
	    }
	    
	    PriorityQueue<IdDist> otherPQ = new PriorityQueue<IdDist>(this.k, new IdDistComparator("max"));
	    
	    if (method.equals("bf"))
	    {
	    	this.bfn = new BfNeighbors(this.k, this.mbrCentroid, this.qpoints, this.tpoints, this.fastsums, this.neighbors, this.dpc_count, this.total_tpoints);
		    
		    otherPQ.addAll(this.bfn.getNeighbors());
	    }
	    else if (method.equals("ps"))
	    {
	    	this.psn = new PsNeighbors(this.k, this.mbrCentroid, this.qpoints, this.tpoints, this.fastsums, this.neighbors, this.dpc_count, this.sumdist, this.sumdx, this.sumdx_success, this.skipped_tpoints, this.total_tpoints);
			
		    otherPQ.addAll(this.psn.getNeighbors());
	    }
		
		while (!otherPQ.isEmpty())
		{
			IdDist newNeighbor = otherPQ.poll();
			
			if (!GnnFunctions.isDuplicate(this.neighbors, newNeighbor))
			{
				// if PriorityQueue not full, add new tpoint (IdDist)
		    	if (this.neighbors.size() < this.k)
					this.neighbors.offer(newNeighbor);
		    	else // if queue is full, run some checks and replace elements
		    	{
		    		double dm = this.neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
					if (newNeighbor.getDist() < dm) // compare distance
					{
						this.neighbors.poll(); // remove top element
						this.neighbors.offer(newNeighbor); // insert to queue
					}
		    	}
			}
		}
	}

	@Override
	public AccumulatorV2<Iterable<Point>, PriorityQueue<IdDist>> copy()
	{
		return new NeighborsAccumulatorV2(this.k, this.mbrCentroid, this.qpoints, this.neighbors, this.fastsums, this.method, this.dpc_count, this.sumdist, this.sumdx, this.sumdx_success, this.skipped_tpoints, this.total_tpoints);
	}
	
	@Override
	public boolean isZero()
	{
		return (this.neighbors.isEmpty()); // check if priority queue is empty
	}

	@Override
	public void merge(AccumulatorV2<Iterable<Point>, PriorityQueue<IdDist>> other)
	{
		PriorityQueue<IdDist> otherPQ = new PriorityQueue<IdDist>(other.value());
		
		Iterator<IdDist> it = otherPQ.iterator();
		
		while (it.hasNext())
		{
			IdDist neighbor = it.next();
			if (!GnnFunctions.isDuplicate(this.neighbors, neighbor))
				this.neighbors.offer(neighbor);
		}
	}

	@Override
	public void reset()
	{
		this.neighbors.clear(); // clear priority queue
	}

	@Override
	public PriorityQueue<IdDist> value()
	{
		return this.neighbors;
	}
}
*/