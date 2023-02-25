package gr.uth.ece.dsel.spark_prepartitioning.main;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.HashSet;
import java.io.BufferedReader;
import gr.uth.ece.dsel.spark_prepartitioning.util.*;

public class Main
{	
	public static void main(String[] args)
	{
		final long tStart = System.currentTimeMillis();
		
		/*
	     **************************************************************************
	     *                                                                        *
	     *                         Class variables,                               *
	     *                      command line arguments,                           *
	     *                         open HDFS files                                *
	     *                                                                        *
	     **************************************************************************                                 
	     */
		
		// args
		final String partitioning = args[0];
		final String method = args[1];
		final int K = Integer.parseInt(args[2]);
		final int N = Integer.parseInt(args[3]);
		final String nameNode = args[4];
		final String queryDir = args[5];
		final String queryDataset = args[6];
		final String trainingDir = args[7];
		final String trainingDataset = args[8];
		final String gnnDir = args[9];
		final String treeDir = args[10]; // HDFS dir containing tree file
		final String treeFileName = args[11]; // tree file name in HDFS
		final String phase15 = args[12];
		final boolean heuristics = Boolean.parseBoolean(args[13]);
		final boolean fastsums = Boolean.parseBoolean(args[14]);
		final char systemType = args[15].charAt(0); // "L" for local or "D" for distributed
	    
		final String username = System.getProperty("user.name"); // get user name
	    ArrayList<Point> qpoints = new ArrayList<Point>();
	    double[] mbrCentroid = new double[7];
	    Node root = null;
	    
	    if (!partitioning.equals("qt") && !partitioning.equals("gd"))
			throw new IllegalArgumentException("partitoning arg must be 'qt' or 'gd'");
		
		if (!phase15.equals("mbr") && !phase15.equals("centroid"))
			throw new IllegalArgumentException("phase15 args must be 'mbr' or 'centroid'");
		
		if (!method.equals("bf") && !method.equals("ps"))
			throw new IllegalArgumentException("method arg must be 'bf' or 'ps'");
	    
	    System.out.printf("GKNN %s-%s starting...\n", partitioning.toUpperCase(), method.toUpperCase());
	    
	    // create HDFS files paths
	    final String queryFile = String.format("hdfs://%s:9000/user/%s/%s/%s", nameNode, username, queryDir, queryDataset);
	    final String trainingFile = String.format("hdfs://%s:9000/user/%s/%s/%s", nameNode, username, trainingDir, trainingDataset);
	    final String mbrCentroidFile = String.format("hdfs://%s:9000/user/%s/%s/mbrcentroid.txt", nameNode, username, gnnDir);
	    final String treeFile = String.format("hdfs://%s:9000/user/%s/%s/%s", nameNode, username, treeDir, treeFileName); // full HDFS path to tree file
		
	  	// display arguments to the console
	    String arguments = String.format("partitioning=%s\nmethod=%s\nK=%d\nQuery Dataset=%s\nTraining Dataset=%s\nPhase 1.5=%s\nheuristics=%s\nfastsums=%s\n", partitioning, method, K, queryFile, trainingFile, phase15, heuristics, fastsums);
	  	if (partitioning.equals("gd"))
	  		arguments += "N=" + N + "\n";
	  	else if (partitioning.equals("qt"))
	  		arguments += "sampletree=" + treeFileName + "\n";
	    System.out.println("Input arguments: \n" + arguments);
	  	
		// Spark conf
		// check system type (local or distributed)
		String master = "";
		if (systemType == 'L')
			master = "local[*]";
		else if (systemType == 'D')
			master = String.format("spark://%s:7077", nameNode);
		else
			throw new IllegalArgumentException("systemType arg must be 'L' for local or 'D' for distributed");

		SparkConf sparkConf = new SparkConf().setAppName("gknn-prepartitioning-spark").setMaster(master);
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		// Hadoop FS
		Configuration hadoopConf = new Configuration();
	    hadoopConf.set("fs.defaultFS", String.format("hdfs://%s:9000", nameNode));
	    FileSystem fs;
	  	
	  	// open HDFS files
	  	try
	  	{
	  		fs = FileSystem.get(hadoopConf);
	  		
		  	// read query dataset file and convert to Point array
		    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(queryFile))));
		    String line;
			while ((line = reader.readLine())!= null) // while input has more lines
				qpoints.add(GnnFunctions.newPoint(line, "\t")); // add point to list
			reader.close(); // close file
			
			// read mbrCentroid file from hdfs
		    reader = new BufferedReader(new InputStreamReader(fs.open(new Path(mbrCentroidFile))));
			line = reader.readLine();
			String[] data = line.trim().split("\t"); // split it at '\t'
			for (int i = 0; i < data.length; i++)
				mbrCentroid[i] = Double.parseDouble(data[i]); // read (xmin, xmax, ymin, ymax, xc, yc, sumDistCQ) and put into array
			reader.close(); // close file
			
			// read treefile from hdfs
			if (partitioning.equals("qt"))
			{
				Path pt = new Path(treeFile); // create path object from path string
				ObjectInputStream input = new ObjectInputStream(fs.open(pt)); // open HDFS tree file
				root = (Node) input.readObject(); // assign quad tree binary form to root node
			}
	  	}
	  	catch (ClassNotFoundException classNotFoundException)
		{
			System.err.println("Invalid object type");
		}
	  	catch (IOException e)
		{
			System.err.println("hdfs file does not exist");
			e.printStackTrace();
		}
	    
	  	// if PS sort query points by x-ascending
	  	if (method.equals("ps"))
	  		Collections.sort(qpoints, new PointXComparator("min"));
	  	
	  	// broadcast query and mbrcentroid array
	    Broadcast<ArrayList<Point>> qpointsBC = jsc.broadcast(qpoints);
	    Broadcast<double[]> mbrCentroidBC = jsc.broadcast(mbrCentroid);
	    
	    // accumulators	    
	    LongAccumulator total_tpoints = jsc.sc().longAccumulator();
	    LongAccumulator num_cells = jsc.sc().longAccumulator();
	    LongAccumulator sumdist = jsc.sc().longAccumulator();
	    LongAccumulator sumdx = jsc.sc().longAccumulator();
	    LongAccumulator sumdx_success = jsc.sc().longAccumulator();
	    LongAccumulator dpc_count = jsc.sc().longAccumulator();
	    LongAccumulator skipped_tpoints = jsc.sc().longAccumulator();
	    LongAccumulator heur1success = jsc.sc().longAccumulator();
	    LongAccumulator heur1fail = jsc.sc().longAccumulator();
	    LongAccumulator heur2success = jsc.sc().longAccumulator();
	    LongAccumulator heur2fail = jsc.sc().longAccumulator();
	    LongAccumulator heur3success = jsc.sc().longAccumulator();
	    LongAccumulator heur3fail = jsc.sc().longAccumulator();
	    
	    /*
	     **************************************************************************
	     *                             PHASE 0                                    *
	     *                                                                        *
	     *             prepartitioning - create RDD[cell, tpoints]                *
	     **************************************************************************                                 
	     */
	    
	    final long t0 = System.currentTimeMillis();
	    System.out.println("PHASE 0 starting...");
	    
	    int partitions = 2; // initially set partitions to total number of cores
	    
	    // create RDD[cell, tpoints]
	    JavaPairRDD<String, Iterable<Point>> cellTpointsRDD = JavaPairRDD.fromJavaRDD(jsc.emptyRDD());
	    
	    if (partitioning.equals("gd"))
	    	cellTpointsRDD = jsc.textFile(trainingFile)
	    					 	.mapToPair(new CellTpointsGD("\t", N))
	    					 	.groupByKey(partitions)
	    					 	.persist(StorageLevel.MEMORY_AND_DISK());
	    else if (partitioning.equals("qt"))
	    	cellTpointsRDD = jsc.textFile(trainingFile)
			 				 	.mapToPair(new CellTpointsQT("\t", root))
			 				 	.groupByKey(partitions)
			 				 	.persist(StorageLevel.MEMORY_AND_DISK());
	    
	    // save RDD to HDFS
	    //final String trainingCellTpointsDir = trainingDataset.split("\\.")[0];
	    //cellTpointsRDD.saveAsObjectFile(String.format("hdfs://%s:9000/user/%s/%sCellTpoints", nameNode, username, trainingCellTpointsDir));
	    
	    System.out.printf("number of cells = %d\n", cellTpointsRDD.keys().count());
	    
	    System.out.printf("cellTpointsRDD numPartitions: %d%n", cellTpointsRDD.getNumPartitions());
	    
	    System.out.printf("PHASE 0 finished in %d millis\n", System.currentTimeMillis() - t0);
	    
	    /*
	     **************************************************************************
	     *                             PHASE 1                                    *
	     *                                                                        *
	     *             find number of training points per cell                    *
	     **************************************************************************                                 
	     */
	    
	    final long t1 = System.currentTimeMillis();
	    System.out.println("PHASE 1 starting...");
	    
	    // get map(cell_id, contained training points)
	    JavaPairRDD<String, Integer> tPointsPerCellRDD = cellTpointsRDD.mapValues(new IterablePointsCount())
	    												 			   .persist(StorageLevel.MEMORY_AND_DISK());
	    
	    System.out.printf("number of cells = %d\n", tPointsPerCellRDD.count());
	    
	    // collect Phase 1 output: HashMap(cell_id, num tpoints)
	    final HashMap<String, Integer> cell_tpoints = new HashMap<String, Integer>(tPointsPerCellRDD.collectAsMap());
	    
	    // broadcast Phase 1 output: HashMap(cell_id, num tpoints)
	    Broadcast<HashMap<String, Integer>> cell_tpointsBC = jsc.broadcast(cell_tpoints);
	    
	    tPointsPerCellRDD.unpersist();  // remove RDD from cache
	    
	    System.out.printf("PHASE 1 finished in %d millis\n", System.currentTimeMillis() - t1);
	    
	    /*
	     **************************************************************************
	     *                               PHASE 1.5                                *
	     *                                                                        *
	     *         find cells that overlap with MBR or circle around centroid     *
	     **************************************************************************                                 
	     */
	    
	    final long t15 = System.currentTimeMillis();
	    System.out.println("PHASE 1.5 starting...");
	    
	    // initialize empty int set to put overlaps
	    HashSet<String> overlaps = new HashSet<String>();
	    
	    final GetOverlaps getOverlaps = new GetOverlaps(K, mbrCentroidBC.getValue(), cell_tpointsBC.getValue(), phase15);
	    
	    if (partitioning.equals("gd"))
	    {
	    	getOverlaps.setN(N);
	    	getOverlaps.setPartition("gd");
	    }
	    else if (partitioning.equals("qt"))
	    {
	    	getOverlaps.setRoot(root);
	    	getOverlaps.setPartition("qt");
	    }
	    
	    overlaps.addAll(getOverlaps.getOverlaps());
	
	    // broadcast overlaps
	    Broadcast<HashSet<String>> overlapsBC = jsc.broadcast(overlaps);
	    
	    String overlapsString = "";
	    for (String cell: overlaps)
	    	overlapsString = overlapsString.concat(cell + "\n");
	    WriteLocalFiles.writeFile("overlaps.txt", overlapsString);
	    
	    System.out.println();
	    
	    System.out.printf("PHASE 1.5 finished in %d millis\n", System.currentTimeMillis() - t15);
	    
	    /*
	     **************************************************************************
	     *                             PHASE 2                                    *
	     *                                                                        *
	     *                 discover neighbors from overlaps                       *
	     **************************************************************************
	     */
	    
	    final long t2 = System.currentTimeMillis();
	    System.out.println("PHASE 2 starting...");
	    
	    int par = (int) Math.ceil(overlaps.size() / 4); // set partitions to number of overlaps cells / 4
	    
	    if (par == 0)
	    	partitions = 1;
	    else
	    {
	    	if (par % 2 != 0)
	    		par++;
	    	partitions = par;
	    }
	    
	    // filter cell-tpoints RDD with overlapping cells only
	    JavaPairRDD<String, Iterable<Point>> overlapsCellTpointsRDD = cellTpointsRDD.filter(new OverlapsContainCell(overlapsBC.getValue()))
	    																			.repartition(partitions)
	    																			.persist(StorageLevel.MEMORY_AND_DISK());
	    
	    System.out.printf("overlap cells: %d%n", overlapsCellTpointsRDD.keys().count());
	    
	    System.out.printf("overlapsCellTpointsRDD numPartitions: %d%n", overlapsCellTpointsRDD.getNumPartitions());
	    
	    int overlap_tpoints = 0;
	    
	    for (String cell: overlapsCellTpointsRDD.keys().collect())
	    {
	    	if (cell_tpoints.containsKey(cell))
	    	{
	    		//System.out.printf("cell: %s contains %d tpoints%n", cell, cell_tpoints.get(cell));
	    		overlap_tpoints += cell_tpoints.get(cell);
	    	}
	    }
	    
	    System.out.printf("overlap tpoints = %d%n", overlap_tpoints);
	    
	    // max heap of K neighbors (IdDist)
	    PriorityQueue<IdDist> neighbors2 = new PriorityQueue<IdDist>(K, new IdDistComparator("max"));
	    
	    // RDD of best neighbors (as PriorityQueue<IdDist> objects)
	    JavaRDD<PriorityQueue<IdDist>> neighbors2RDD = jsc.emptyRDD();
	    
	    // find neighbors in overlaps
	    if (method.equals("bf"))
	    	neighbors2RDD = overlapsCellTpointsRDD
	    					.mapValues(new BfNeighbors(K, mbrCentroidBC.getValue(), qpointsBC.getValue(), neighbors2, fastsums, dpc_count))
	    					.values();
	    else if (method.equals("ps"))
	    	neighbors2RDD = overlapsCellTpointsRDD
	    					.mapValues(new PsNeighbors(K, mbrCentroidBC.getValue(), qpointsBC.getValue(), neighbors2, fastsums, dpc_count, sumdist, sumdx, sumdx_success, skipped_tpoints))
	    					.values();
	    
	    //NeighborsAccumulatorV2 neighborsAcc2 = new NeighborsAccumulatorV2(K, mbrCentroid, qpoints, neighbors2, fastsums, method, dpc_count, sumdist, sumdx, sumdx_success, skipped_tpoints, total_tpoints);
	    //jsc.sc().register(neighborsAcc2, "NeighborsAccumulatorV2-2");
	    //overlapsTpointsRDD.foreach(it -> neighborsAcc2.add(it));
	    //neighbors2.addAll(neighborsAcc2.value());
	    
	    neighbors2 = neighbors2RDD.reduce((pq1, pq2) -> GnnFunctions.joinPQ(pq1, pq2, K));
	    
	    System.out.println("Phase 2 neighbors");
	    
	    GnnFunctions.printPQ(neighbors2, K, "min");
	    
	    WriteLocalFiles.writeFile("neighbors2.txt", GnnFunctions.pqToString(neighbors2, K, "min"));
	    
	    System.out.printf("PHASE 2 finished in %d millis\n", System.currentTimeMillis() - t2);
	    /*
	    // print accumulators
	    System.out.printf("num_cells = %d\n", overlapsCellTpointsRDD.count());
	    System.out.printf("dpc_count = %d\n", dpc_count.value());
	    System.out.printf("sumdist = %d\n", sumdist.value());
	    System.out.printf("sumdx = %d\n", sumdx.value());
	    System.out.printf("sumdx_success = %d\n", sumdx_success.value());
	    System.out.printf("skipped_tpoints = %d\n", skipped_tpoints.value());
	    System.out.printf("total_tpoints = %d\n", total_tpoints.value());
	    
	    // reset accumulators
	    dpc_count.reset();
	    sumdist.reset();
	    sumdx.reset();
	    sumdx_success.reset();
	    skipped_tpoints.reset();
	    total_tpoints.reset();
	    */
	    overlapsCellTpointsRDD.unpersist(); // remove RDD from cache
	    
	    /*
	     **************************************************************************
	     *                               PHASE 3                                  *
	     *                                                                        *
	     *    discover neighbors from cells excluding overlaps, apply heuristics  *
	     **************************************************************************                                 
	     */
	    
	    final long t3 = System.currentTimeMillis();
	    System.out.println("PHASE 3 starting...");
	    
	    final double dm = neighbors2.peek().getDist(); // get (not remove) distance of neighbor with maximum distance
    	
	    // create RDD with cells that pass heuristics
	    JavaRDD<String> nonPrunedCellsRDD = jsc.emptyRDD();
	    
	    // apply heuristics to prune distant cells
	    if (partitioning.equals("gd"))
	    	nonPrunedCellsRDD = tPointsPerCellRDD
	    						.map(pair -> pair._1)
	    						.filter(new CellPruningHeuristicsGD(N, dm, overlapsBC.getValue(), qpointsBC.getValue(), mbrCentroidBC.getValue(), fastsums, heuristics, num_cells, heur1success, heur1fail, heur2success, heur2fail, heur3success, heur3fail))
	    						.persist(StorageLevel.MEMORY_AND_DISK());
	    else if (partitioning.equals("qt"))
	    	nonPrunedCellsRDD = tPointsPerCellRDD
	    						.map(pair -> pair._1)
	    						.filter(new CellPruningHeuristicsQT(dm, overlapsBC.getValue(), qpointsBC.getValue(), mbrCentroidBC.getValue(), fastsums, heuristics, num_cells, heur1success, heur1fail, heur2success, heur2fail, heur3success, heur3fail))
	    						.persist(StorageLevel.MEMORY_AND_DISK());
	    
	    tPointsPerCellRDD.unpersist(); // remove RDD from cache
	    
	    // collect nonPrunedCellsRDD as local set
	    HashSet<String> nonPrunedCells = new HashSet<String>(nonPrunedCellsRDD.collect());
	    
	    par = (int) Math.ceil(nonPrunedCells.size() / 4); // set partitions to number of non-overlaps cells / 4
	    
	    if (par == 0)
	    	partitions = 1;
	    else
	    {
	    	if (par % 2 != 0)
	    		par++;
	    	partitions = par;
	    }
	    	    
	    // broadcast local set nonPrunedCells
	    Broadcast<HashSet<String>> nonPrunedCellsBC = jsc.broadcast(nonPrunedCells);
	    
	    // filter cellTpointsRDD with non-pruned cells
	    JavaPairRDD<String, Iterable<Point>> nonOverlapsCellTpointsRDD = JavaPairRDD.fromJavaRDD(jsc.emptyRDD());
	    
	    if (partitioning.equals("gd"))
	    	nonOverlapsCellTpointsRDD = cellTpointsRDD.filter(new TpointsInNonPrunedCells(nonPrunedCellsBC.getValue()))
	    											  .repartition(partitions)
	    											  .persist(StorageLevel.MEMORY_AND_DISK());
    	else if (partitioning.equals("qt"))
    		nonOverlapsCellTpointsRDD = cellTpointsRDD.filter(new TpointsInNonPrunedCells(nonPrunedCellsBC.getValue()))
										    		  .repartition(partitions)										  
										    		  .persist(StorageLevel.MEMORY_AND_DISK());
	    
	    int non_overlap_tpoints = 0;
	    
	    long nonOverlapsCells = nonOverlapsCellTpointsRDD.keys().count();
	    
	    System.out.printf("nonOverlapsCellTpointsRDD numPartitions: %d%n", nonOverlapsCellTpointsRDD.getNumPartitions());
	    
	    System.out.printf("non-overlap cells: %d%n", nonOverlapsCells);
	    
	    for (String cell: nonOverlapsCellTpointsRDD.keys().collect())
	    {
	    	if (cell_tpoints.containsKey(cell))
	    	{
	    		//System.out.printf("cell: %s contains %d tpoints%n", cell, cell_tpoints.get(cell));
	    		non_overlap_tpoints += cell_tpoints.get(cell);
	    	}
	    }
	    
	    System.out.printf("non overlap tpoints = %d%n", non_overlap_tpoints);
	    
	    // RDD of best neighbors (as PriorityQueue<IdDist> objects)
	    JavaRDD<PriorityQueue<IdDist>> neighbors3RDD = jsc.emptyRDD();
	    
	    if (!nonOverlapsCellTpointsRDD.isEmpty())
	    {
		    // find neighbors in non-overlaps
		    if (method.equals("bf"))
		    	neighbors3RDD = nonOverlapsCellTpointsRDD
		    					.mapValues(new BfNeighbors(K, mbrCentroidBC.getValue(), qpointsBC.getValue(), neighbors2, fastsums, dpc_count))
	    						.values()
	    						.persist(StorageLevel.MEMORY_AND_DISK());
		    else if (method.equals("ps"))
		    	neighbors3RDD = nonOverlapsCellTpointsRDD
		    					.mapValues(new PsNeighbors(K, mbrCentroidBC.getValue(), qpointsBC.getValue(), neighbors2, fastsums, dpc_count, sumdist, sumdx, sumdx_success, skipped_tpoints))
	    						.values()
	    						.persist(StorageLevel.MEMORY_AND_DISK());
	    }
	    
	    PriorityQueue<IdDist> neighbors3 = new PriorityQueue<IdDist>(K, new IdDistComparator("max"));
	    
	    if (!neighbors3RDD.isEmpty())
	    	neighbors3 = neighbors3RDD.reduce((pq1, pq2) -> GnnFunctions.joinPQ(pq1, pq2, K));
	    
	    neighbors3RDD.unpersist(); // remove RDD from cache
	    
	    //NeighborsAccumulatorV2 neighborsAcc3 = new NeighborsAccumulatorV2(K, mbrCentroid, qpoints, neighbors2, fastsums, method, dpc_count, sumdist, sumdx, sumdx_success, skipped_tpoints, total_tpoints);
	    //jsc.sc().register(neighborsAcc3, "NeighborsAccumulatorV2-3");
	    //nonOverlapsTpointsRDD.foreach(it -> neighborsAcc3.add(it));
	    //neighbors3.addAll(neighborsAcc3.value());
	    /*
	    // print accumulators
	    System.out.printf("num_cells = %d\n", nonOverlapsCellTpointsRDD.count());
	    System.out.printf("dpc_count = %d\n", dpc_count.value());
	    System.out.printf("sumdist = %d\n", sumdist.value());
	    System.out.printf("sumdx = %d\n", sumdx.value());
	    System.out.printf("sumdx_success = %d\n", sumdx_success.value());
	    System.out.printf("skipped_tpoints = %d\n", skipped_tpoints.value());
	    System.out.printf("total_tpoints = %d\n", total_tpoints.value());
	    */
	    System.out.printf("heuristic 1 successes = %d\n", heur1success.value());
	    System.out.printf("heuristic 1 fails = %d\n", heur1fail.value());
	    System.out.printf("heuristic 2 successes = %d\n", heur2success.value());
	    System.out.printf("heuristic 2 fails = %d\n", heur2fail.value());
	    System.out.printf("heuristic 3 successes = %d\n", heur3success.value());
	    System.out.printf("heuristic 3 fails = %d\n", heur3fail.value());
	    
	    nonOverlapsCellTpointsRDD.unpersist(); // remove RDD from cache
    	
	    System.out.printf("PHASE 3 finished in %d millis\n", System.currentTimeMillis() - t3);
	    
	    /*
	     **************************************************************************
	     *                             PHASE 3.5                                  *
	     *                                                                        *
	     *           merge lists from Phases 2 & 3 into the final one             *
	     **************************************************************************                                 
	     */
	    
	    if (!neighbors3.isEmpty() && !neighbors3.containsAll(neighbors2))
	    {
	    	System.out.println("Phase 3 neighbors");
	    	GnnFunctions.printPQ(neighbors3, K, "min");
	    	
	    	System.out.println("Final neighbors list:");
	    	PriorityQueue<IdDist> finalNeighbors = new PriorityQueue<IdDist>(K, new IdDistComparator("max"));
	    	finalNeighbors = GnnFunctions.joinPQ(neighbors2, neighbors3, K);
	    	GnnFunctions.printPQ(finalNeighbors, K, "min");
	    	
	    	WriteLocalFiles.writeFile("finalNeighbors.txt", GnnFunctions.pqToString(finalNeighbors, K, "min"));
	    }
	    else
	    	System.out.println("No new neighbors from Phase 3");
		
	    System.out.printf("%s-%s finished in %d millis\n", partitioning.toUpperCase(), method.toUpperCase(), System.currentTimeMillis() - tStart);
	    
		jsc.close();
	}
}
