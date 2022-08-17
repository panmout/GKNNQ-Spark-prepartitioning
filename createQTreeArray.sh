###########################################################################
#                             PARAMETERS                                  #
###########################################################################

nameNode=panagiotis-lubuntu
trainingDir=input
treeDir=sampletree
trainingDataset=NApppointNNew.txt
samplerate=100
capacity=10

###########################################################################
#                                    EXECUTE                              # ###########################################################################

hadoop jar ./target/gknn-spark-prepartitioning-0.0.1-SNAPSHOT.jar \
gr.uth.ece.dsel.spark_prepartitioning.preliminary.QuadtreeArray \
nameNode=$nameNode \
trainingDir=$trainingDir \
treeDir=$treeDir \
trainingDataset=$trainingDataset \
samplerate=$samplerate \
capacity=$capacity
