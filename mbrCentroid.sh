# execute (<namenode name> <hdfs dir name> <query dataset> <hdfs GNN dir name> <step> <mindist> <counter_limit> <pointdist>)

hadoop jar ./target/gknn-spark-prepartitioning-0.0.1-SNAPSHOT.jar gr.uth.ece.dsel.spark_prepartitioning.preliminary.MbrCentroid \
<<<<<<< HEAD
nameNode=panagiotis-lubuntu \
=======
nameNode=Hadoopmaster \
>>>>>>> b674c9bdf016ef50da742ce80edd3513d74403b5
queryDir=input \
queryDataset=linearwaterNNew_sub_2.8M.txt \
gnnDir=gnn \
step=0.000001 \
minDist=10 \
counter_limit=100 \
diff=0.000001
