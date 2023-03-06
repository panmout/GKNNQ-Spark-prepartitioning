package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public final class GetOverlaps
{
	private final HashMap<String, Integer> cell_tpoints; // hashmap of training points per cell list from MR1 {[cell_id, number of training points]}
	private final HashSet<String> overlaps; // set of overlapping cells
	private final double[] mbrC; // mbrCentroid array
	private int N; // N*N cells
	private final int K; // GNN K
	private Node root; // create root node
	private final String phase15; // mbr or centroid
	private String partitioning;
	
	public GetOverlaps(int k, double[] mbrCentroid, HashMap<String, Integer> phase1output, String method)
	{
		this.cell_tpoints = phase1output;
		this.mbrC = Arrays.copyOf(mbrCentroid, mbrCentroid.length);
		this.K = k;
		this.phase15 = method;
		this.overlaps = new HashSet<>();
	}
	
	public void setPartition(String partition)
	{
		this.partitioning = partition;
	}
	
	public void setN(int n)
	{
		this.N = n;
	}
	
	public void setRoot(Node node)
	{
		this.root = node;
	}
	
	public HashSet<String> getOverlaps()
	{
		if(this.phase15.equals("mbr"))
		{
			if(this.partitioning.equals("gd"))
				mbrOverlapsGD();
			else if(this.partitioning.equals("qt"))
				mbrOverlapsQT();
		}
		else if(this.phase15.equals("centroid"))
		{
			if(this.partitioning.equals("gd"))
				centroidOverlapsGD();
			else if(this.partitioning.equals("qt"))
				centroidOverlapsQT();
		}
		return this.overlaps;
	}
	
	/*
	 **********	Overlaps functions (GD, QT, MBR, centroid) ************
	 */
	
	// find grid MBR overlaps
	private void mbrOverlapsGD()
	{
		final double ds = 1.0/this.N; // interval ds (cell width)
		
		// read MBR coordinates
		double xmin = this.mbrC[0];
		double xmax = this.mbrC[1];
		double ymin = this.mbrC[2];
		double ymax = this.mbrC[3];
    	
		int overlaps_points = 0; // total number of training points in overlaps
		
		while (overlaps_points <= this.K)
	    {
			// loop until find at least K training points in intersected cells
	        overlaps_points = 0; // reset
	        
	        for (String cell : this.cell_tpoints.keySet())
			{
	        	final int intCell = Integer.parseInt(cell);
	        	final int i = intCell % this.N;
	        	final int j = (intCell - i) / this.N;
	    	    
	    	    /* cell's lower left corner: x0, y0
	    			 *        upper left corner: x0, y0 + ds
	    			 *        upper right corner: x0 + ds, y0 + ds
	    			 *        lower right corner: x0 + ds, y0
	    			 */
	    	    
	        	final double x0 = i * ds;
	        	final double y0 = j * ds;
	    	  
	    	    // check intersection of grid cell and MBR
	    		if ((xmin <= x0 + ds) && (xmax >= x0) && (ymin <= y0 + ds) && (ymax >= y0))
	    			this.overlaps.add(cell);
			}
	        
	        // count total training points in overlaps list
			for (String cell : this.overlaps)
				overlaps_points += this.cell_tpoints.get(cell); // add this overlap's training points
	        
	        // increase size of MBR for next loop (each side by 10%)
	        xmin = 0.9 * xmin;
	        xmax = (1.1 * xmax <= 1.0) ? 1.1 * xmax : xmax;
	        ymin = 0.9 * ymin;
	        ymax = (1.1 * ymax <= 1.0) ? 1.1 * ymax : ymax;
	    }
	}
	
	// find quadtree MBR overlaps
	private void mbrOverlapsQT()
	{
		double xmin = this.mbrC[0]; // get MBR borders from array
		double xmax = this.mbrC[1];
		double ymin = this.mbrC[2];
		double ymax = this.mbrC[3];
		
		int overlaps_points = 0; // total number of training points in overlaps
		
		while (overlaps_points <= this.K) // loop until find at least K training points in intersected cells
		{
			overlaps_points = 0; // reset
			
			for (String cell : this.cell_tpoints.keySet())
			{
				// get each training cell's coords from its cellname
				double x0 = 0; // cell's lower left corner coords initialization
				double y0 = 0;
				
				for (int i = 0; i < cell.length(); i++) // check cellname's digits
				{
					switch(cell.charAt(i))
					{
						case '0':
							y0 += 1.0 / Math.pow(2, i + 1); // if digit = 0 increase y0
							break;
						case '1':
							x0 += 1.0 / Math.pow(2, i + 1); // if digit = 1 increase x0
							y0 += 1.0 / Math.pow(2, i + 1); // and y0
							break;
						case '3':
							x0 += 1.0 / Math.pow(2, i + 1); // if digit = 3 increase x0
							break;
					}
				}
				
				final double ds = 1.0 / Math.pow(2, cell.length()); // cell side length
				/* cell's lower left corner: x0, y0
				 *        upper left corner: x0, y0 + s
				 *        upper right corner: x0 + s, y0 + s
				 *        lower right corner: x0 + s, y0
				 */
				 
				// check intersection of quadtree cell and MBR
				if ((xmin <= x0 + ds) && (xmax >= x0) && (ymin <= y0 + ds) && (ymax >= y0))
					this.overlaps.add(cell);
			}
			
			// count total training points in overlaps list
			for (String cell : this.overlaps)
				overlaps_points += this.cell_tpoints.get(cell); // add this overlap's training points
				
			// increase size of MBR for next loop (each side by 10%)
			xmin = 0.9 * xmin;
			xmax = (1.1 * xmax <= 1.0) ? 1.1 * xmax : xmax;
			ymin = 0.9 * ymin;
			ymax = (1.1 * ymax <= 1.0) ? 1.1 * ymax : ymax;
		}
	}
	
	// find grid centroid overlaps
	private void centroidOverlapsGD()
	{
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
		
		ds = 1.0/N;
		i = (int) (x/ds)
		j = (int) (y/ds)
		cell = j*N+i
		
		How to find neighboring cells:

		If current is cell_id:
		 W is (cell_id - 1)
		 E is (cell_id + 1)
		 N is (cell_id + n)
		 S is (cell_id - n)
		NE is (cell_id + n + 1)
		NW is (cell_id + n - 1)
		SE is (cell_id - n + 1)
		SW is (cell_id - n - 1)
		
		south row cells ( 0, 1,..., n-1 ) don't have S, SE, SW neighbors
		north row cell's ( (n-1)n, (n-1)n+1,..., (n-1)n+(n-1) ) don't have N, NE, NW neighbors
		west column cells ( 0, n,..., (n-1)n ) don't have W, NW, SW neighbors
		east column cells ( n-1, 2n-1,..., (n-1)n+(n-1) ) don't have E, NE, SE neighbors
		
		
		                           xi mod ds (part of xi inside cell)
		     |----------xi------------->.(xi, yi)
		     |---------|----------|-----^--
	         |         |          |     |   yi mod ds (part of yi inside cell)
	    2*ds |---------|----------|-----|--
	         |         |          |     yi
	      ds |---------|----------|-----|--
	         |         |          |     |
	         |---------|----------|--------
	       0          ds         2*ds                  
	       
		 */
		
    	// read centroid coordinates
    	final double xc = this.mbrC[4];
    	final double yc = this.mbrC[5];
    	
		// find centroid cell
    	final double ds = 1.0 / this.N; // interval ds (cell width)
    	final int ic = (int) (xc / ds); // get i
    	final int jc = (int) (yc / ds); // get j
    	final int intCentroidCell = jc * this.N + ic; // calculate cell_id
		
		// circle radius initialized as half the cell width
		double R = 0.5 * ds;
				
		// radius increase step = 50% of radius
		final double dr = 0.5 * R;
		
		// total number of training points in overlaps
		int overlaps_points = 0;
		
		final String centroidCell = String.valueOf(intCentroidCell);
		
		if (this.cell_tpoints.containsKey(centroidCell))
		{
			overlaps_points += this.cell_tpoints.get(centroidCell);
			this.overlaps.add(centroidCell);
		}
		
		// top-bottom rows, far left-right columns
		final HashSet<Integer> south_row = new HashSet<>(); // no S, SE, SW for cells in this set
		final HashSet<Integer> north_row = new HashSet<>(); // no N, NE, NW for cells in this set
		final HashSet<Integer> west_column = new HashSet<>(); // no W, NW, SW for cells in this set
		final HashSet<Integer> east_column = new HashSet<>(); // no E, NE, SE for cells in this set
		
		for (int i = 0; i < this.N; i++) // filling sets
		{
			south_row.add(i);
			north_row.add((this.N - 1) * this.N + i);
			west_column.add(i * this.N);
			east_column.add(i * this.N + this.N - 1);
		}
		
		// set of surrounding cells
		final HashSet<Integer> surrounding_cells = new HashSet<>();
		
		// dummy set of cells to be added (throws ConcurrentModificationException if trying to modify set while traversing it)
		final HashSet<Integer> addSquaresList = new HashSet<>();
		
		// first element is centroid cell
		surrounding_cells.add(intCentroidCell);
		
		// trying to find overlaps to fill k-nn
		while (overlaps_points < this.K)
		{
			// getting all surrounding cells of centroid cell
			
			boolean runAgain = true;
			
			// keep filling set until it contains circle R inside it
			while (runAgain)
			{
				for (int square : surrounding_cells)
				{
					if (!west_column.contains(square)) // W (excluding west column)
						addSquaresList.add(square - 1);
					
					if (!east_column.contains(square)) // E (excluding east column)
						addSquaresList.add(square + 1);
					
					if (!north_row.contains(square)) // N (excluding north_row)
						addSquaresList.add(square + this.N);
					
					if (!south_row.contains(square)) // S (excluding south_row)
						addSquaresList.add(square - this.N);
					
					if (!south_row.contains(square) && !west_column.contains(square)) // SW (excluding south row and west column)
						addSquaresList.add(square - this.N - 1);
					
					if (!south_row.contains(square) && !east_column.contains(square)) // SE (excluding south row and east column)
						addSquaresList.add(square - this.N + 1);
					
					if (!north_row.contains(square) && !west_column.contains(square)) // NW (excluding north row and west column)
						addSquaresList.add(square + this.N - 1);
					
					if (!north_row.contains(square) && !east_column.contains(square)) // NE (excluding north row and east column)
						addSquaresList.add(square + this.N + 1);
				}
				
				surrounding_cells.addAll(addSquaresList); // add new squares to original set
				
				// boolean variables to check if surrounding cells include the circle with radius R
				boolean stopRunX = false;
				boolean stopRunY = false;
				
				int maxI = ic; // min & max column index of surrounding cells at centroid's cell row
				int minI = ic;
				
				for (int i = 0; i < this.N; i++) // running through columns 0 to N
				{
					if (surrounding_cells.contains(jc * this.N + i)) // getting cells at centroid's cell row (jc)
					{
						maxI = Math.max(i, maxI);
						
						minI = Math.min(i, minI);
					}
				}
				
				if ((maxI - minI) * ds > 2 * R) // if surrounding cells width is more than 2*R, set stop var to 'true'
					stopRunX = true;
				
				int maxJ = jc; // min & max row index of surrounding cells at centroid's cell column
				int minJ = jc;
				
				for (int j = 0; j < this.N; j++) // running through columns 0 to N
				{
					if (surrounding_cells.contains(j * this.N + ic)) // getting cells at centroid's cell column (ic)
					{
						maxJ = Math.max(j, maxJ);
						
						minJ = Math.min(j, minJ);
					}
				}
				
				if ((maxJ - minJ) * ds > 2 * R) // if surrounding cells width is more than 2*R, set stop var to 'true'
					stopRunY = true;
				
				// if all stop vars are set to 'true', stop loop
				if (stopRunX && stopRunY)
					runAgain = false;
			}
			
			// checking for overlaps in surroundings
			final Iterator<Integer> iterator = surrounding_cells.iterator(); // creating iterator to traverse set
			
			while (iterator.hasNext()) // while set has elements
			{
				final int square = iterator.next();
				// if cell does not contains any training points, remove it from list
				if (!this.cell_tpoints.containsKey(String.valueOf(square)))
					iterator.remove();
				else
				{
					// cell_id = j*n + i
					final int i = square % this.N;
					final int j = (square - i) / this.N;
					// get cell center coordinates
					final double cx = i*ds + ds/2;
					final double cy = j*ds + ds/2;
					// circle center to cell center distance
					final double centers_dist_x = Math.abs(xc - cx);
					final double centers_dist_y = Math.abs(yc - cy);
					
					final String sq = String.valueOf(square);
					
					// check circle - cell collision
					if (i > ic && j == jc) // to the east of centroid's cell, same row
					{
						if (xc + R > i * ds) // checking collision with cell's west wall
							this.overlaps.add(sq); // there is collision, add cell to overlaps
					}
					else if (i > ic && j > jc) // to the north-east of centroid's cell
					{
						if (centers_dist_x < R + ds / 2 && centers_dist_y < R + ds / 2) // if centers are close enough
							if (Math.pow(xc - i * ds, 2) + Math.pow(yc - j * ds, 2) < R * R) // if also SW corner is inside circle
								this.overlaps.add(sq); // there is collision, add cell to overlaps
					}
					else if (i == ic && j > jc) // to the north of centroid's cell, same column
					{
						if (yc + R > j * ds) // checking collision with cell's south wall
							this.overlaps.add(sq); // there is collision, add cell to overlaps
					}
					else if (i < ic && j > jc) // to the north-west of centroid's cell
					{
						if (centers_dist_x < R + ds / 2 && centers_dist_y < R + ds / 2) // if centers are close enough
							if (Math.pow(xc - (i + 1) * ds, 2) + Math.pow(yc - j * ds, 2) < R * R) // if also SE corner is inside circle
								this.overlaps.add(sq); // there is collision, add cell to overlaps
					}
					else if (i < ic && j == jc) // to the west of centroid's cell, same row
					{
						if (xc - R < (i + 1)*ds) // checking collision with cell's east wall
							this.overlaps.add(sq); // there is collision, add cell to overlaps
					}
					else if (i < ic && j < jc) // to the south-west of centroid's cell
					{
						if (centers_dist_x < R + ds / 2 && centers_dist_y < R + ds / 2) // if centers are close enough
							if (Math.pow(xc - (i + 1) * ds, 2) + Math.pow(yc - (j + 1) * ds, 2) < R * R) // if also NE corner is inside circle
								this.overlaps.add(sq); // there is collision, add cell to overlaps
					}
					else if (i == ic && j < jc) // to the south of centroid's cell, same column
					{
						if (yc - R < (j + 1) * ds) // checking collision with cell's north wall
							this.overlaps.add(sq); // there is collision, add cell to overlaps
					}
					else if (i > ic && j < jc) // to the south-east of centroid's cell
					{
						if (centers_dist_x < R + ds / 2 && centers_dist_y < R + ds / 2) // if centers are close enough
							if (Math.pow(xc - i * ds, 2) + Math.pow(yc - (j + 1) * ds, 2) < R * R) // if also NE corner is inside circle
								this.overlaps.add(sq); // there is collision, add cell to overlaps
					}
				}
			} // end while
			
			overlaps_points = 0; // reset value
			
			// now find total training points from overlaps
			for (String s : this.overlaps)
				overlaps_points += this.cell_tpoints.get(s); // add this overlap's training points
			
			R += dr; // increase radius
		}
	}
	
	// find quadtree centroid overlaps
	private void centroidOverlapsQT()
	{
		// centroid coords
    	final double xc = this.mbrC[4];
    	final double yc = this.mbrC[5];
		
    	final String centroidCell = GnnFunctions.pointToCellQT(xc, yc, this.root); // find centroid's cell
		
    	System.out.println("centroid cell: " + centroidCell);
		/* If
		 * root cell side length = L
		 * and for example
		 * cell id = 3012 (4 digits)
		 * then cell's length = L / (2 ^ 4)
		 */
    	
    	int overlaps_points = 0; // total number of training points in overlaps
    	
    	if (this.cell_tpoints.containsKey(centroidCell))
    	{
			overlaps_points += this.cell_tpoints.get(centroidCell);
			this.overlaps.add(centroidCell);
		}
    	
    	final double ds = 1.0 / Math.pow(2, centroidCell.length()); // ds = centroid's cell width
		
		// circle radius initialized as half the cell width
		double R = 0.5 * ds;
				
		// radius increase step = 50% of radius
		final double dr = 0.5 * R;
		
		while (overlaps_points <= this.K) // trying to find overlaps to fill k-nn
		{
			// reset value
			overlaps_points = 0;
			this.overlaps.clear();
			
			// draw circle and check for overlaps
			rangeQuery(xc, yc, R, this.root, "");
			
			Iterator<String> it = this.overlaps.iterator();
			
			while (it.hasNext())
			{
				final String cell = it.next();
				
				if (this.cell_tpoints.containsKey(cell)) // count points from non-empty cells
					overlaps_points += this.cell_tpoints.get(cell); // add this overlap's training points
				else
					it.remove();
			}
			
			R += dr; // increase radius
		}
	}
	
	private void rangeQuery(double x, double y, double r, Node node, String address)
	{
		if (node.getNW() == null) // leaf node
			this.overlaps.add(address);
		
		// internal node
		else
		{
			if (intersect(x, y, r, node.getNW()))
				rangeQuery(x, y, r, node.getNW(), address + "0");
			
			if (intersect(x, y, r, node.getNE()))
				rangeQuery(x, y, r, node.getNE(), address + "1");
			
			if (intersect(x, y, r, node.getSW()))
				rangeQuery(x, y, r, node.getSW(), address + "2");
			
			if (intersect(x, y, r, node.getSE()))
				rangeQuery(x, y, r, node.getSE(), address + "3");
		}
	}
	
	private boolean intersect(double x, double y, double r, Node node)
	{
		// if point is inside cell return true
		if (x >= node.getXmin() && x <= node.getXmax() && y >= node.getYmin() && y <= node.getYmax())
			return true;
		
		// check circle - cell collision
		final double ds = node.getXmax() - node.getXmin(); // cell's width
		
		// get cell center coordinates
		final double xc = (node.getXmin() + node.getXmax()) / 2;
		final double yc = (node.getYmin() + node.getYmax()) / 2;
		
		// circle center to cell center distance
		final double centers_dist_x = Math.abs(x - xc);
		final double centers_dist_y = Math.abs(y - yc);
		
		// if centers are far in either direction, return false
		if (centers_dist_x > r + ds / 2)
			return false;
		if (centers_dist_y > r + ds / 2)
			return false;
		
		// if control reaches here, centers are close enough
		
		// the next two cases mean that circle center is within a stripe of width r around the square 
		if (centers_dist_x < ds / 2)
			return true;
		if (centers_dist_y < ds / 2)
			return true;
		
		// else check the corner distance
		final double corner_dist_sq = (centers_dist_x - ds / 2) * (centers_dist_x - ds / 2) + (centers_dist_y - ds / 2) * (centers_dist_y - ds / 2);
		
		return corner_dist_sq <= r * r;
	}
}
