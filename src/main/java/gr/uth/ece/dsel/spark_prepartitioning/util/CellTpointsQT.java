package gr.uth.ece.dsel.spark_prepartitioning.util;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public final class CellTpointsQT implements PairFunction<String, String, Point>
{
	private final String sep;
	private final Node node;
	
	public CellTpointsQT (String sep, Node node)
	{
		this.sep = sep;
		this.node = node;
	}
	
	@Override
	public Tuple2<String, Point> call(String line)
	{
		final Point p = GnnFunctions.newPoint(line, sep);
		final String cell = GnnFunctions.pointToCellQT(p.getX(), p.getY(), this.node);
		return new Tuple2<>(cell, p);
	}
}
