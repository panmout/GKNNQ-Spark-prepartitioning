partitioning=gd # gd or qt
method=bf # bf or ps
K=10
N=100
NameNode=panagiotis-lubuntu
queryDir=input
trainingDir=input
queryDataset=query-dataset.txt
trainingDataset=NApppointNNew.txt
gnnDir=gnn
treeDir=sampletree
treeFileName=qtree.ser
phase15=centroid # mbr or centroid
heuristics=true
fastsums=true

spark-submit \
--class gr.uth.ece.dsel.spark_prepartitioning.main.Main \
./target/spark-prepartitioning-0.0.1-SNAPSHOT.jar \
$partitioning $method $K $N $NameNode $queryDir $queryDataset $trainingDir $trainingDataset $gnnDir $treeDir $treeFileName $phase15 $heuristics $fastsums
