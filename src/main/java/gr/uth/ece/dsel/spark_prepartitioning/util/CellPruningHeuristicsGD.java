package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;

public final class CellPruningHeuristicsGD implements Function<String, Boolean>
{
	private final int N;
	private final double bestDist;
	private final HashSet<String> overlaps;
	private final ArrayList<Point> qpoints;
	private final double[] mbrCentroid;
	private final boolean fastsums;
	private final boolean heuristics;
	private final LongAccumulator num_cells;
	private final LongAccumulator heur1success;
	private final LongAccumulator heur1fail;
	private final LongAccumulator heur2success;
	private final LongAccumulator heur2fail;
	private final LongAccumulator heur3success;
	private final LongAccumulator heur3fail;
	
	public CellPruningHeuristicsGD(int n, double bd, HashSet<String> overlaps, ArrayList<Point> qp, double[] mbrC, boolean fs, boolean hs, LongAccumulator num_cells, LongAccumulator heur1success, LongAccumulator heur1fail, LongAccumulator heur2success, LongAccumulator heur2fail, LongAccumulator heur3success, LongAccumulator heur3fail)
	{
		this.N = n;
		this.bestDist = bd;
		this.overlaps = overlaps;
		this.qpoints = new ArrayList<Point>(qp);
		this.mbrCentroid = Arrays.copyOf(mbrC, mbrC.length);
		this.fastsums = fs;
		this.heuristics = hs;
		this.num_cells = num_cells;
		this.heur1success = heur1success;
		this.heur1fail = heur1fail;
		this.heur2success = heur2success;
		this.heur2fail = heur2fail;
		this.heur3success = heur3success;
		this.heur3fail = heur3fail;
	}
	
	@Override
	public Boolean call(String cell)
	{		
		boolean bool = true; // pruning flag (true --> pass, false --> prune)
	    
		// proceed only if cell is in Phase 1 output but not in overlaps
		if (!this.overlaps.contains(cell))
		{
		    // calculate cell's coords
			final int intCell = Integer.parseInt(cell);
		    
		    final double ds = 1.0 / N; // interval ds
	 		final int i = intCell % N; // get i
	 		final int j = (intCell - i) / N; // get j
	 		
	 		/* cell's lower left corner: x0, y0
	 		 *        upper left corner: x0, y0 + ds
	 		 *        upper right corner: x0 + ds, y0 + ds
	 		 *        lower right corner: x0 + ds, y0
	 		 */
	 		
	 		final double x0 = i * ds;
	 		final double y0 = j * ds;
			
			// check heuristics 1, 2, 3
			bool = GnnFunctions.heuristics123(x0, y0, ds, this.mbrCentroid, this.qpoints, this.bestDist, this.fastsums, this.heuristics, this.heur1success, this.heur1fail, this.heur2success, this.heur2fail, this.heur3success, this.heur3fail);
			
			if (bool)
				this.num_cells.add(1);
		}
		return bool;
	}
}
