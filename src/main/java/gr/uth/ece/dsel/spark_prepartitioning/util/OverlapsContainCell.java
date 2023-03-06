package gr.uth.ece.dsel.spark_prepartitioning.util;

import java.util.HashSet;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public final class OverlapsContainCell implements Function<Tuple2<String, Iterable<Point>>, Boolean>
{
	private final HashSet<String> overlaps;
	
	public OverlapsContainCell(HashSet<String> overlaps)
	{
		this.overlaps = overlaps;
	}
	
	@Override
	public Boolean call(Tuple2<String, Iterable<Point>> pair)
	{
		return this.overlaps.contains(pair._1);
	}
}
