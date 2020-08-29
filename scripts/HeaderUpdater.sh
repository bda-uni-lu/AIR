#!/bin/bash
for subdir in `ls $1`; do
	for files in `ls $1/$subdir`; do
    		echo "writing to $files"
		#cat LHeader.txt $1/$subdir/$files > temp.cpp && cat temp.cpp > $1/$subdir/$files && rm temp.cpp
#break;	
done
#break;
done

