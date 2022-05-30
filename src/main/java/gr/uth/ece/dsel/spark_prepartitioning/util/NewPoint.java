package gr.uth.ece.dsel.spark_prepartitioning.util;

import org.apache.spark.api.java.function.Function;

//string to point
public final class NewPoint implements Function<String, Point>
{
	private String sep;
	
	public NewPoint (String sep)
	{
		this.sep = sep;
	}
	
	@Override
	public final Point call(String line)
	{
		return GnnFunctions.newPoint(line, this.sep);
	}
}