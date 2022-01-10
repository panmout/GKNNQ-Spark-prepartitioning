###########################################################################
#                             PARAMETERS                                  #
###########################################################################

nameNode=panagiotis-lubuntu
trainingDir=input
treeDir=sampletree
trainingDataset=NApppointNNew.txt
samplerate=10
capacity=100
type=1 # 1 for simple capacity based quadtree, 2 for all children split method, 3 for average width method

###########################################################################
#                                    EXECUTE                              # ###########################################################################

spark-submit \
--class gr.uth.ece.dsel.spark_prepartitioning.preliminary.Qtree \
./target/spark-prepartitioning-0.0.1-SNAPSHOT.jar \
nameNode=$nameNode \
trainingDir=$trainingDir \
treeDir=$treeDir \
trainingDataset=$trainingDataset \
samplerate=$samplerate \
capacity=$capacity \
type=$type
