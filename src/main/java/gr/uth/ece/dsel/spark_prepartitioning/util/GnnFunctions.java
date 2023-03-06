package gr.uth.ece.dsel.spark_prepartitioning.util;

import org.apache.spark.util.LongAccumulator;

import java.util.ArrayList;
import java.util.PriorityQueue;

public final class GnnFunctions
{
	// String to point
	public static Point newPoint(String line, String sep)
	{
		final String[] data = line.trim().split(sep);
		final int id = Integer.parseInt(data[0]);
		final double x = Double.parseDouble(data[1]);
		final double y = Double.parseDouble(data[2]);
		return new Point(id, x, y);
	}
	
    /*
     **************************************************************************
     *                             PHASE 1                                    *
     *                                                                        *
     *             find number of training points per cell                    *
     **************************************************************************                                 
     */
    
    /*
    Cell array (numbers inside cells are cell_id)
      
        n*ds |---------|----------|----------|----------|----------|----------|--------------|
             | (n-1)n  | (n-1)n+1 | (n-1)n+2 |          | (n-1)n+i |          | (n-1)n+(n-1) |
    (n-1)*ds |---------|----------|----------|----------|----------|----------|--------------|
             |         |          |          |          |          |          |              |
             |---------|----------|----------|----------|----------|----------|--------------|
             |   j*n   |  j*n+1   |  j*n+2   |          |  j*n+i   |          |   j*n+(n-1)  |
        j*ds |---------|----------|----------|----------|----------|----------|--------------|
             |         |          |          |          |          |          |              |
             |---------|----------|----------|----------|----------|----------|--------------|
             |   2n    |   2n+1   |   2n+2   |          |   2n+i   |          |     3n-1     |
        2*ds |---------|----------|----------|----------|----------|----------|--------------|
             |    n    |    n+1   |    n+2   |          |    n+i   |          |     2n-1     |
          ds |---------|----------|----------|----------|----------|----------|--------------|
             |    0    |     1    |     2    |          |     i    |          |      n-1     |
             |---------|----------|----------|----------|----------|----------|--------------|       
           0          ds         2*ds                  i*ds               (n-1)*ds          n*ds
      
      
     So, cell_id(i,j) = j*n+i
  	*/
	
	// point to GD cell
	public static String pointToCellGD(Point p, int n)
	{
		final double ds = 1.0 / n; // interval ds (cell width)
		final double x = p.getX();  // p.x
		final double y = p.getY();  // p.y
		final int i = (int) (x / ds); // i = (int) x/ds
		final int j = (int) (y / ds); // j = (int) y/ds
		final int cellId = j * n + i;
		return String.valueOf(cellId); // return cellId
	}
	
	// node to cell
	public static String nodeToCell(Node node)
	{
		return pointToCellQT((node.getXmin() + node.getXmax()) / 2, (node.getYmin() + node.getYmax()) / 2, node);
	}
	
	// point to QT cell
	public static String pointToCellQT(double x, double y, Node node)
	{
		if (node.getNW() != null)
		{
			if (x >= node.getXmin() && x < (node.getXmin() + node.getXmax()) / 2) // point inside SW or NW
			{
				if (y >= node.getYmin() && y < (node.getYmin() + node.getYmax()) / 2) // point inside SW
					return "2" + pointToCellQT(x, y, node.getSW());
				else if (y >= (node.getYmin() + node.getYmax()) / 2 && y < node.getYmax()) // point inside NW
					return "0" + pointToCellQT(x, y, node.getNW());
			}
			else if (x >= (node.getXmin() + node.getXmax()) / 2 && x < node.getXmax()) // point inside SE or NE
			{
				if (y >= node.getYmin() && y < (node.getYmin() + node.getYmax()) / 2) // point inside SE
					return "3" + pointToCellQT(x, y, node.getSE());
				else if (y >= (node.getYmin() + node.getYmax()) / 2 && y < node.getYmax()) // point inside NE
					return "1" + pointToCellQT(x, y, node.getNE());
			}
		}
		return "";
	}
	
	// cell pruning heuristics
	public static boolean heuristics123(double x0, double y0, double ds, double[] mbrCentroid, ArrayList<Point> qpoints, double bestDist, boolean fastSums, boolean heuristics, LongAccumulator heur1success, LongAccumulator heur1fail, LongAccumulator heur2success, LongAccumulator heur2fail, LongAccumulator heur3success, LongAccumulator heur3fail)
	{
		// read MBR coordinates
		final double xmin = mbrCentroid[0];
		final double xmax = mbrCentroid[1];
		final double ymin = mbrCentroid[2];
		final double ymax = mbrCentroid[3];
	    // read centroid coordinates
		final double xc = mbrCentroid[4];
		final double yc = mbrCentroid[5];
	    // read sumDistCQ
		final double sumDistCQ = mbrCentroid[6];
		
		final int qsize = qpoints.size();
 		
		boolean bool = true; // pruning flag (true --> pass, false --> prune)
		
		// heuristic 1 (single point method), prune if: minDist(cell, centroid) >= [bestDist + sumDist(centroid, Q)] / |Q|
		if (bool && heuristics)
		{
			if (pointSquareDistance(xc, yc, x0, y0, x0 + ds, y0 + ds) * qsize >= bestDist + sumDistCQ)
			{
				bool = false;
				heur1success.add(1); // this cell was successfully pruned by heuristic 1
			}
		}
		
		// heuristic 2 (minimum bounding method), prune if: minDist(cell, MBR) >= bestDist / |Q|
		if (bool && heuristics)
		{
			heur1fail.add(1); // this cell was NOT pruned by heuristic 1
			
			if (squaresDistance(xmin, ymin, xmax, ymax, x0, y0, x0 + ds, y0 + ds) * qsize >= bestDist)
			{
				bool = false;
				heur2success.add(1); // this cell was successfully pruned by heuristic 2
			}
		}
			
		// heuristic 3 (2nd minimum bounding method), prune if: minDist(cell, Q) >= bestDist
		if (bool && heuristics)
		{
			heur2fail.add(1); // this cell was NOT pruned by heuristic 2
			
			double sumDistCellQ = 0; // sum of distances of cell to each point of Q
			
			for (Point q : qpoints)
			{
				sumDistCellQ += pointSquareDistance(q.getX(), q.getY(), x0, y0, x0 + ds, y0 + ds);
				
				if (fastSums && sumDistCellQ >= bestDist) // fast sums
					break;
			}
			if (sumDistCellQ >= bestDist)
			{
				bool = false;
				heur3success.add(1); // this cell was successfully pruned by heuristic 3
			}
			else
				heur3fail.add(1); // this cell was NOT pruned by heuristic 3
		}
		return bool;
	}
	
	// heuristic 4 - true => pass
	public static boolean heuristic4(int qsize, double dpc, double dm, double sumDistcQ)
	{
		return (qsize * dpc < dm + sumDistcQ);
	}
	
	// return euclidean distance between two points (x1, y1) and (x2, y2)
	public static double distance (double x1, double y1, double x2, double y2)
	{
		return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
	}// end euclidean distance
	
	// calculate sum of distances from a tpoint to all qpoints
	public static double calcSumDistQ(Point tpoint, ArrayList<Point> queryPoints, boolean fastSums, double dist)
	{
		double sumdist = 0;
		for (Point qpoint : queryPoints)
		{
			sumdist += distance(qpoint.getX(), qpoint.getY(), tpoint.getX(), tpoint.getY());
			
			if (fastSums && dist > 0 && sumdist > dist) // if fastSums == true break loop
				break;
		}
		/*
		double xt = tpoint.getX(); // tpoint's x
	    double yt = tpoint.getY(); // tpoint's y
	    
	    // similar to Scala's foldLeft or Spark's aggregate
	    double sumdist = queryPoints.stream().reduce(0.0, // initial value
	    		(acc, qp) -> acc + distance(qp.getX(), qp.getY(), xt, yt), // accumulator (acc = double)
	    		(acc1, acc2) -> acc1 + acc2); // combiner
	    */
	    return sumdist; // return
	}
	
	// calculate sum of x-distances from a tpoint to all qpoints
	public static double calcSumDistQx(Point tpoint, ArrayList<Point> queryPoints, boolean fastSums, double dist)
	{
		double sumdistDx = 0;
		for (Point qpoint : queryPoints)
		{
			sumdistDx += Math.abs(qpoint.getX() - tpoint.getX());
			
			if (fastSums && dist > 0 && sumdistDx > dist) // if fastSums == true break loop
				break;
		}
		/*
		double xt = tpoint.getX(); // tpoint's x
	    
	    // similar to Scala's foldLeft or Spark's aggregate
	    double sumdistDx = queryPoints.stream().reduce(0.0, // initial value
	    		(acc, qp) -> acc + Math.abs(qp.getX() - xt), // accumulator (acc = double)
	    		(acc1, acc2) -> acc1 + acc2); // combiner
	    */
	    return sumdistDx; // return
	}
	
	public static PriorityQueue<IdDist> joinPQ(PriorityQueue<IdDist> pq1, PriorityQueue<IdDist> pq2, int k)
	{
		PriorityQueue<IdDist> pq = new PriorityQueue<>(k, new IdDistComparator("max"));
		
		while (!pq1.isEmpty())
		{
			IdDist n1 = pq1.poll();
			if (!isDuplicate(pq, n1))
				pq.offer(n1);
		}
		
		while (!pq2.isEmpty())
		{
			IdDist n2 = pq2.poll();
			if (!isDuplicate(pq, n2))
				pq.offer(n2);
		}
		
		while (pq.size() > k)
			pq.poll();
		
		return pq;
	}
	
	public static PriorityQueue<IdDist> joinPQ1(PriorityQueue<IdDist> pq1, PriorityQueue<IdDist> pq2, int k)
	{
		PriorityQueue<IdDist> pq = new PriorityQueue<IdDist>(k, new IdDistComparator("max"));
		
		pq.addAll(pqDifference(pq1, pq2));
		
		pq.addAll(pqDifference(pq2, pq1));
		
		pq1.clear();
		pq2.clear();
		
		while (pq.size() > k)
			pq.poll();
		
		return pq;
	}
	
	public static PriorityQueue<IdDist> pqDifference(PriorityQueue<IdDist> pq1, PriorityQueue<IdDist> pq2)
	{
		// returns the elements of pq1 that are not contained in pq2
		pq1.removeIf(idDist -> isDuplicate(pq2, idDist));
		
		return pq1;
	}
	
	// check for duplicates in PriorityQueue
	public static boolean isDuplicate(PriorityQueue<IdDist> pq, IdDist neighbor)
	{
		for (IdDist elem : pq)
			if (elem.getId() == neighbor.getId())
				return true;
		return false;
	}
	
	// PriorityQueue<IdDist> to String
	public static String pqToString(PriorityQueue<IdDist> pq, int k, String comp)
	{
		PriorityQueue<IdDist> newPQ = new PriorityQueue<>(k, new IdDistComparator(comp));
		
		newPQ.addAll(pq);
		
		StringBuilder output = new StringBuilder();
		
		int counter = 0;
		
		while (!newPQ.isEmpty() && counter < k) // add neighbors to output
	    {
			IdDist elem = newPQ.poll();
			int pid = elem.getId();
			double dist = elem.getDist();
			output.append(String.format("%d\t%.10f\n", pid, dist));
			counter++;
		}
		
		return output.toString();
	}
	
	// print PQ
	public static void printPQ(PriorityQueue<IdDist> pq, int k, String comp)
	{
		System.out.println(pqToString(pq, k, comp));
	}
	
	public static double pointSquareDistance(double xp, double yp, double x1, double y1, double x2, double y2)
	{
		final double dx = Math.max(Math.max(x1 - xp, 0.0), xp - x2);
		final double dy = Math.max(Math.max(y1 - yp, 0.0), yp - y2);
		
		return Math.sqrt(dx * dx + dy * dy);
	}
	
	public static double squaresDistance(double x1, double y1, double x2, double y2, double x3, double y3, double x4, double y4)
	{
		final boolean left = x4 < x1;
		final boolean right = x2 < x3;
		final boolean bottom = y4 < y1;
		final boolean top = y2 < y3;
		
		double minDist = 0;
		
		if (top && left)
			minDist = distance(x1, y2, x4, y3);
		else if (left && bottom)
			minDist = distance(x1, y1, x4, y4);
		else if (bottom && right)
			minDist = distance(x2, y1, x3, y4);
		else if (right && top)
			minDist = distance(x2, y2, x3, y3);
		else if (left)
			minDist = x1 - x4;
		else if (right)
			minDist = x3 - x2;
		else if (bottom)
			minDist = y1 - y4;
		else if (top)
			minDist = y3 - y2;
		
		return minDist;
	}
	
	public static int binarySearchTpoints(double x, ArrayList<Point> points)
	{
		int low = 0;
		int high = points.size() - 1;
		int middle = (low + high + 1) / 2;
		int location = -1;
		
		do
		{
			if (x >= points.get(middle).getX())
			{
				if (middle == points.size() - 1) // middle = array length
					location = middle;
				else if (x < points.get(middle + 1).getX()) // x between middle and high
					location = middle;
				else // x greater than middle but not smaller than middle+1
					low = middle + 1;
			}
			else // x smaller than middle
				high = middle - 1;
			
			middle = (low + high + 1) / 2; // recalculate middle
			
		} while ((low < high) && (location == -1));
		
		return location;
	}
}