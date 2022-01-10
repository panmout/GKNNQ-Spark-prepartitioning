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
		String[] data = line.trim().split(sep);
		int id = Integer.parseInt(data[0]);
		double x = Double.parseDouble(data[1]);
		double y = Double.parseDouble(data[2]);
		return new Point(id, x, y);
	}
}