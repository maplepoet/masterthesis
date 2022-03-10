#!/bin/bash

######
# This script allows to compile and run the code. You need to specify IP addresses of your servers and clients. The scripts expect three arguments and the result is stored in a folder named "results". Do create a folder with the name "results" before running this script.
######

i=$1   # Argument 1 to script --> Number of replicas
cli=$2 # Argument 2 to script --> Number of clients
name=$3
runs=$4
bsize=$5 # Argumnet 3 to script --> Batch Size
if [ -z $bsize ]; then
	bsize=100
fi

SNODES=(
	# Frankfurt
	"172.24.5.121"
	"172.24.5.183"
	"172.24.3.200"
	"172.24.3.70"
	# Ohio
	"172.25.128.28"
	"172.25.201.153"
	"172.25.193.119"
	"172.25.206.255"
	# "10.0.0.20"
	# Virginia
	"172.26.229.8"
	"172.26.226.93"
	"172.26.149.169"
	"172.26.78.36"
)

CNODES=(
	# California
	"172.31.7.2"
)

rm ifconfig.txt hostnames.py

# Building file ifconfig.txt
#
count=0
while (($count < $i)); do
	echo ${SNODES[$count]} >>ifconfig.txt
	count=$((count + 1))
done

count=0
while (($count < $cli)); do
	echo ${CNODES[$count]} >>ifconfig.txt
	count=$((count + 1))
done

# Building file hostnames
#
echo "hostip = [" >>hostnames.py
count=0
while (($count < $i)); do
	echo -e "\""${SNODES[$count]}"\"," >>hostnames.py
	count=$((count + 1))
done

count=0
while (($count < $cli)); do
	echo -e "\""${CNODES[$count]}"\"," >>hostnames.py
	count=$((count + 1))
done
echo "]" >>hostnames.py

echo "hostmach = [" >>hostnames.py
count=0
while (($count < $i)); do
	echo "\""${SNODES[$count]}"\"," >>hostnames.py
	count=$((count + 1))
done

count=0
while (($count < $cli)); do
	echo "\""${CNODES[$count]}"\"," >>hostnames.py
	count=$((count + 1))
done
echo "]" >>hostnames.py

# Compiling the Code
make clean; make -j8

tm=0

# Copy to scripts
cp run* scripts/
cp ifconfig.txt scripts/
cp config.h scripts/
cp hostnames.py scripts/
cd scripts

# Number of times you want to run the code (default 1)
while [ $tm -lt $runs ]; do
	python3 simRun.py $i s${i}_c${cli}_results_${name}_b${bsize}_run${tm}_node $tm

	tm=$((tm + 1))
done

# Go back
cd ..
