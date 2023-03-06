package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;

public final class CellPruningHeuristicsQT implements Function<String, Boolean>
{
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
	
	public CellPruningHeuristicsQT(double bd, HashSet<String> overlaps, ArrayList<Point> qp, double[] mbrC, boolean fs, boolean hs, LongAccumulator num_cells, LongAccumulator heur1success, LongAccumulator heur1fail, LongAccumulator heur2success, LongAccumulator heur2fail, LongAccumulator heur3success, LongAccumulator heur3fail)
	{
		this.bestDist = bd;
		this.overlaps = overlaps;
		this.qpoints = new ArrayList<>(qp);
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
		    double x0, y0 = 0; // cell's lower left corner coords, cell width
		    
		    // calculate cell's coords
		    x0 = 0; // cell's lower left corner coords initialization
			y0 = 0;
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
			
			// check heuristics 1, 2, 3
			bool = GnnFunctions.heuristics123(x0, y0, ds, this.mbrCentroid, this.qpoints, this.bestDist, this.fastsums, this.heuristics, this.heur1success, this.heur1fail, this.heur2success, this.heur2fail, this.heur3success, this.heur3fail);
			
			if (bool)
				this.num_cells.add(1);
		}
		return bool;
	}
}
