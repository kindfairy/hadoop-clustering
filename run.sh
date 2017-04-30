#!/bin/sh
inputdir="/home/rstm2013/20groups-small"
gluster="/mnt/root"
prefix="output-"
lastnum=$(ls $gluster$HOME |
sed -rne 's:output-([0-9]+):\1:p' |
sort -n |
tail -n1)
newnum=$(expr $lastnum \+ 1)
outputdir="$HOME/$prefix$newnum"
echo "Input dir: $inputdir"
echo "Output dir: $outputdir"
hadoop-submit --jar target/hadoop-clustering-1.0-SNAPSHOT.jar -- \
	$inputdir $outputdir