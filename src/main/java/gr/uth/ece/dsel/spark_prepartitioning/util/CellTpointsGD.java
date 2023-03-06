package gr.uth.ece.dsel.spark_prepartitioning.util;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public final class CellTpointsGD implements PairFunction<String, String, Point>
{
	private final String sep;
	private final int n;
	
	public CellTpointsGD (String sep, int n)
	{
		this.sep = sep;
		this.n = n;
	}
	
	@Override
	public Tuple2<String, Point> call(String line)
	{
		final Point p = GnnFunctions.newPoint(line, sep);
		final String cell = GnnFunctions.pointToCellGD(p, n);
		return new Tuple2<>(cell, p);
	}
}
