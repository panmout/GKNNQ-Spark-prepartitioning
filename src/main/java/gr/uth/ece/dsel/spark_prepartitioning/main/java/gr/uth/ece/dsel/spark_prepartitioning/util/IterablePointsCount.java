package gr.uth.ece.dsel.spark_prepartitioning.util;

import org.apache.spark.api.java.function.Function;

public final class IterablePointsCount implements Function<Iterable<Point>, Integer>
{
	@Override
	public final Integer call(Iterable<Point> it)
	{
		int counter = 0;
		
		for (Point p: it)
			counter++;
		
		return counter;
	}
}
