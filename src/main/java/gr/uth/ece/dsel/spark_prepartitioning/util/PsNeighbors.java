package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.FormatterClosedException;
import java.util.PriorityQueue;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;

public final class PsNeighbors implements Function<Iterable<Point>, PriorityQueue<IdDist>>
{
	private int k;
	private ArrayList<Point> qpoints;
	private double[] mbrCentroid;
	private PriorityQueue<IdDist> neighbors;
	private PriorityQueue<IdDist> oldNeighbors;
	private ArrayList<Point> tpoints;
	private boolean fastsums;
	private boolean changed = false; // priority queue will be returned only if changed
	private LongAccumulator dpc_count;
	private LongAccumulator sumdist;
	private LongAccumulator sumdx;
	private LongAccumulator sumdx_success;
	private LongAccumulator skipped_tpoints;
	
	//private String messages = "";
	
	public PsNeighbors(int K, double[] mbrC, ArrayList<Point> qp, PriorityQueue<IdDist> pq, boolean fs, LongAccumulator dpc_count, LongAccumulator sumdist, LongAccumulator sumdx, LongAccumulator sumdx_success, LongAccumulator skipped_tpoints)
	{
		this.k = K;
		this.qpoints = new ArrayList<Point>(qp);
		this.mbrCentroid = Arrays.copyOf(mbrC, mbrC.length);
		this.neighbors = new PriorityQueue<IdDist>(pq);
		this.oldNeighbors = new PriorityQueue<IdDist>(pq);
		this.fastsums = fs;
		this.dpc_count = dpc_count;
		this.sumdist = sumdist;
		this.sumdx = sumdx;
		this.sumdx_success = sumdx_success;
		this.skipped_tpoints = skipped_tpoints;
	}
	
	public final PriorityQueue<IdDist> call(Iterable<Point> iter)
	{
		// read MBR coordinates
		final double xmin = this.mbrCentroid[0];
		final double xmax = this.mbrCentroid[1];
	    
	    this.tpoints = new ArrayList<Point>();
	    
	    // fill tpoints list
	    for (Point tpoint: iter)
	    {
	    	this.tpoints.add(tpoint);
	    }
	    
	    //messages = messages.concat(String.format("tpoints = %d\n", this.tpoints.size()));
	    
	    // sort list by x ascending
	 	Collections.sort(this.tpoints, new PointXComparator("min"));
	 	
	 	if (!this.tpoints.isEmpty()) // if this cell has any tpoints
		{
			// get median point of Q
			int m = this.qpoints.size() / 2; // median index of Q
			double xm = this.qpoints.get(m).getX(); // x of median qpoint
			
			double x_left = this.tpoints.get(0).getX(); // leftmost tpoint's x
			double x_right = this.tpoints.get(this.tpoints.size() - 1).getX(); // rightmost tpoint's x
			
			boolean check_right = false;
			boolean check_left = false;
			
			int right_limit = 0;
			int left_limit = 0;
			
			if (xm < x_left) // median qpoint is at left of all tpoints
			{
				check_right = true;
				right_limit = 0;
			}
			else if (x_right < xm) // median qpoint is at right of all tpoints
			{
				check_left = true;
				left_limit = this.tpoints.size() - 1;
			}
			else // median qpoint is among tpoints
			{
				check_left = true;
				check_right = true;
				
				int tindex = GnnFunctions.binarySearchTpoints(xm, this.tpoints); // get tpoints array index for median qpoint interpolation
				right_limit = tindex + 1;
				left_limit = tindex;
			}
			
			boolean cont_search = true; // set flag to true
			
			if (check_left)
			{
				while ((left_limit > -1) && (xt(left_limit) > xmin))  // if tpoint's x is inside MBR
				{
					if (calc_sum_dist_in(left_limit--) == false)
					{
						cont_search = false;
						break;
					}
				}
				if (cont_search == true) // if tpoint's x is outside MBR
				{
					while (left_limit > -1)
					{
						if (calc_sum_dist_out(left_limit--) == false)
						{
							break;
						}
					}
				}
				// x-check success, add remaining tpoints
				if (left_limit > 0) // could be left_limit = -1
				{
					this.skipped_tpoints.add(left_limit);
					//messages = messages.concat(String.format("left_limit = %d\n", left_limit));
				}
			}
			
			cont_search = true; // set flag to true
			
			if (check_right)
			{
				while (right_limit < this.tpoints.size() && (xt(right_limit) < xmax)) // if tpoint's x is inside MBR
				{
					if (calc_sum_dist_in(right_limit++) == false)
					{
						cont_search = false;
						break;
					}
				}
				if (cont_search == true) // if tpoint's x is outside MBR
				{
					while (right_limit < this.tpoints.size())
					{
						if (calc_sum_dist_out(right_limit++) == false)
						{
							break;
						}
					}
				}
				// x-check success, add remaining tpoints
				if (this.tpoints.size() - right_limit > 0) // could be right_limit = tpoints.size()
				{
					this.skipped_tpoints.add(this.tpoints.size() - right_limit);
					//messages = messages.concat(String.format("tpoints.size - right_limit = %d\n", this.tpoints.size() - right_limit));
				}
			}
		}
	 	//writeFile("messages.txt", messages);
	 	if (this.changed == true)
	 	{
	    	if (this.oldNeighbors.isEmpty()) // phase 2
	    		return this.neighbors;
	    	else // phase 3, remove elements from phase 2
	    		return GnnFunctions.pqDifference(this.neighbors, this.oldNeighbors);
	    }
	    else
	    	return new PriorityQueue<IdDist>(this.k, new IdDistComparator("max"));
	 	// end PsNeighbors
	}
	
	private final boolean calc_sum_dist_in(int i) // if tpoint's x is inside MBR
	{
		// read centroid coordinates
		final double xc = this.mbrCentroid[4];
		final double yc = this.mbrCentroid[5];
	    // read sumDistCQ
		final double sumDistCQ = this.mbrCentroid[6];
	    
		final Point tpoint = this.tpoints.get(i); // get tpoint
		final double xt = tpoint.getX(); // tpoint's x
		final double yt = tpoint.getY(); // tpoint's y
		
		//messages = messages.concat(String.format("checking tpoint %d\n", tpoint.getId()));
		
		if (this.neighbors.size() < this.k) // if queue is not full, add new tpoints 
		{
			final double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, false, 0); // distance calculation
			final IdDist neighbor = new IdDist(tpoint.getId(), sumdist);
			if (!GnnFunctions.isDuplicate(this.neighbors, neighbor))
			{
				this.neighbors.offer(neighbor); // insert to queue
				this.changed = true;
				//increment SUMDIST metrics variable
				this.sumdist.add(1);
				//messages = messages.concat(pqToString());
			}
			return true; // check next point
		}
		else  // if queue is full, run some checks and replace elements
		{
			final double dm = this.neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
			
			final double dpc = GnnFunctions.distance(xc, yc, xt, yt); // tpoint-centroid distance
			
			if (!GnnFunctions.heuristic4(this.qpoints.size(), dpc, dm, sumDistCQ)) // if |Q|*dist(p,c) >= MaxHeap.root.dist + dist(centroid, Q) then prune point
			{
				//messages = messages.concat(String.format("tpoint: %d\tdpc success: qsize = %d\tdpc = %f\tqsize*dpc = %f\tdm = %f\tsumDistCQ = %f\tdm + sumDistCQ = %f\n", tpoint.getId(), this.qpoints.size(), dpc, this.qpoints.size()*dpc, dm, sumDistCQ, dm + sumDistCQ));
				//increment DPC_COUNT metrics variable
				this.dpc_count.add(1);
				return true; // check next point
			}
			
			final double sumdx = GnnFunctions.calcSumDistQx(tpoint, this.qpoints, this.fastsums, dm); // calculate sumdx of tpoint from Q
			//increment SUMDX metrics variable
			this.sumdx.add(1);
			
			//if (sumdx <= dm)
				//messages = messages.concat(String.format("tpoint: %d\tsumdx_fail: sumdx = %f <= dm = %f\n", tpoint.getId(), sumdx, dm));
			
			if (sumdx > dm)  // if sumdx bigger than MaxHeap.root.dist
			{
				//messages = messages.concat(String.format("tpoint: %d\tsumdx_success: sumdx = %f > dm = %f\n", tpoint.getId(),sumdx, dm));
				//increment SUMDX_SUCCESS metrics variable
				this.sumdx_success.add(1);
				return false; // end while loop
			}
			
			final double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, this.fastsums, dm); // distance calculation
			
			//increment SUMDIST metrics variable
			this.sumdist.add(1);
			
			if (sumdist < dm) // finally compare distances
			{
				final IdDist neighbor = new IdDist(tpoint.getId(), sumdist);
				if (!GnnFunctions.isDuplicate(this.neighbors, neighbor))
				{
					this.neighbors.poll(); // remove top element
					this.neighbors.offer(neighbor); // insert to queue
					this.changed = true;
					//messages = messages.concat(pqToString());
				}
			}
			return true; // check next point
		}
	}
	
	private final boolean calc_sum_dist_out(int i) // if tpoint's x is outside MBR
	{
		// read centroid coordinates
		final double xc = this.mbrCentroid[4];
		final double yc = this.mbrCentroid[5];
	    // read sumDistCQ
		final double sumDistCQ = this.mbrCentroid[6];
	    
		final Point tpoint = this.tpoints.get(i); // get tpoint
		final double xt = tpoint.getX(); // tpoint's x
		final double yt = tpoint.getY(); // tpoint's y
		
		//messages = messages.concat(String.format("checking tpoint %d\n", tpoint.getId()));
		
		if (this.neighbors.size() < this.k) // if queue is not full, add new tpoints 
		{
			final double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, false, 0); // distance calculation
			final IdDist neighbor = new IdDist(tpoint.getId(), sumdist);
			if (!GnnFunctions.isDuplicate(this.neighbors, neighbor))
			{
				this.neighbors.offer(neighbor); // insert to queue
				this.changed = true;
				//increment SUMDIST metrics variable
				this.sumdist.add(1);
				//messages = messages.concat(pqToString());
			}
			return true; // check next point
		}
		else  // if queue is full, run some checks and replace elements
		{
			final double dpcx = Math.abs(xt - xc); // calculate (tpoint, centroid) x-dist
			
			final double dm = this.neighbors.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
			
			final double sumdx = this.qpoints.size()*dpcx; // calculate sumdx(tpoint, Q) = |Q|*dpcx
			
			//if (sumdx <= dm)
				//messages = messages.concat(String.format("tpoint: %d\tsumdx_fail: sumdx = %f <= dm = %f\n", tpoint.getId(), sumdx, dm));
			
			if (sumdx > dm) // if |Q|*dx(p,c) >= MaxHeap.root.dist then terminate search
			{
				//messages = messages.concat(String.format("tpoint: %d\tsumdx_success: sumdx = %f > dm = %f\n", tpoint.getId(), sumdx, dm));
				//increment SUMDX_SUCCESS metrics variable
				this.sumdx_success.add(1);
				return false; // end while loop
			}
						
			final double dpc = GnnFunctions.distance(xc, yc, xt, yt); // calculate (tpoint, centroid) dist
			
			if (!GnnFunctions.heuristic4(this.qpoints.size(), dpc, dm, sumDistCQ)) // if |Q|*dist(p,c) >= MaxHeap.root.dist + dist(centroid, Q) then prune point
			{
				//messages = messages.concat(String.format("tpoint: %d\tdpc success: qsize = %d\tdpc = %f\tqsize*dpc = %f\tdm = %f\tsumDistCQ = %f\tdm + sumDistCQ = %f\n", tpoint.getId(), this.qpoints.size(), dpc, this.qpoints.size()*dpc, dm, sumDistCQ, dm + sumDistCQ));
				//increment DPC_COUNT metrics variable
				this.dpc_count.add(1);
				return true; // check next point
			}
			
			final double sumdist = GnnFunctions.calcSumDistQ(tpoint, this.qpoints, this.fastsums, dm); // distance calculation
			
			//increment SUMDIST metrics variable
			this.sumdist.add(1);
			
			if (sumdist < dm) // finally compare distances
			{
				final IdDist neighbor = new IdDist(tpoint.getId(), sumdist);
				if (!GnnFunctions.isDuplicate(this.neighbors, neighbor))
				{
					this.neighbors.poll(); // remove top element
					this.neighbors.offer(neighbor); // insert to queue
					this.changed = true;
					//messages = messages.concat(pqToString());
				}
			}
			return true; // check next point	
		}
	}
	
	private final double xt(int i)
	{
		final Point tpoint = this.tpoints.get(i); // get tpoint
		return tpoint.getX(); // tpoint's x
	}
	
	public final void writeFile(String file, String content)
	{
		try
		{
			Formatter outputTextFile = new Formatter(new FileWriter(file, true));
			//Formatter outputTextFile = new Formatter(file);
			outputTextFile.format(content);
			outputTextFile.close();
		}
		catch (FormatterClosedException formatterException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(2);
		}
		catch (IOException ioException)
		{
			System.err.println("Error writing to file, exiting");
			System.exit(3);
		}
	}
	
	public final String pqToString()
	{
		PriorityQueue<IdDist> newPQ = new PriorityQueue<IdDist>(k, new IdDistComparator("max"));
		
		newPQ.addAll(this.neighbors);
		
		String output = "";
		
		int counter = 0;
		
		while (!newPQ.isEmpty() && counter < k) // add neighbors to output
	    {
			IdDist elem = newPQ.poll();
			int pid = elem.getId();
			double dist = elem.getDist();
			output = output.concat(String.format("(%d\t%.10f)", pid, dist));
			counter++;
		}
		output = output.concat("\n");
		
		return output;
	}
}
