#!/bin/sh
inputdir="/home/rstm2013/20groups-small"
gluster="/mnt/root"
prefix1="tfidf-"
prefix2="output-"
lastnum=$(ls $gluster$HOME |
sed -rne 's:tfidf-([0-9]+):\1:p' |
sort -n |
tail -n1)
newnum=$(expr $lastnum \+ 1)
outputdir1="$HOME/$prefix1$newnum"
outputdir2="$HOME/$prefix2$newnum"
echo "Input dir: $inputdir"
echo "Output dir: $outputdir1"
echo "Output dir: $outputdir2"
hadoop-submit --jar target/hadoop-clustering-1.0-SNAPSHOT.jar -- \
	$inputdir $outputdir1 $outputdir2