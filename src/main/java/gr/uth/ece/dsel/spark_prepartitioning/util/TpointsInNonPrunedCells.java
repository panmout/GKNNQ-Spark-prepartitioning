package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.util.HashSet;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public final class TpointsInNonPrunedCells implements Function<Tuple2<String, Iterable<Point>>, Boolean>
{
	private final HashSet<String> npCells;
	
	public TpointsInNonPrunedCells(HashSet<String> cells)
	{
		this.npCells = cells;
	}
	
	@Override
	public Boolean call(Tuple2<String, Iterable<Point>> tuple)
	{
		return this.npCells.contains(tuple._1);
	}
}