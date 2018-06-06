#!/bin/bash

echo -e ""
echo -e "\t+----------"
echo -e "\t| Jordi Lladós Segura"
echo -e "\t| Scalable Consistency in T-Coffee through Apache Spark and Cassandra database"
echo -e "\t+----------"
echo -e ""
echo -e "this script launch PPCAS and t_coffee."

function usage {
	echo "Usage: $0 -i [sequence_file] -m [master_spark_ip] -s [seed_cassandra]"
	echo "Optional arguments:"
	echo "	-c (chunk, default: no chunk)"
	echo "	-p (number of partitions, default: number of sequences)"
	exit 1
}

while getopts “i:m:p:s:c:” OPTION
do
	case $OPTION in
		i)
			i=$OPTARG
			;;
		m)
			master_ip=$OPTARG
			;;
		s)
			cassandra_ip=$OPTARG
			;;
		c)
			chunk=$OPTARG
			;;
		p)
			n_partitions=$OPTARG
			;;
	esac
done

if [ -z "${i}" ] || [ -z "${master_ip}" ] || [ -z "${cassandra_ip}" ]; then
    usage
fi

if [ -z "${chunk}" ]; then
    chunk=1
fi

if [ -z "${n_partitions}" ]; then
    n_partitions=0
fi

PPCAS=../PPCASv2/
TC=../src

fullpath=${i}
filename="${fullpath##*/}"                      # Strip longest match of */ from start
dir="${fullpath:0:${#fullpath} - ${#filename}}" # Substring from 0 thru pos of filename
base="${filename%.[^.]*}"                       # Strip shortest match of . plus at least one non-dot char from end
ext="${filename:${#base} + 1}"                  # Substring from len of base thru end
if [[ -z "$base" && -n "$ext" ]]; then          # If we have an extension and no base, it's really the base
	base=".$ext"
	ext=""
fi

out_name=${base}_bdtc_${chunk}

ABS_PATH=`cd "$dir"; pwd` # double quotes for paths that contain spaces etc...
file=${ABS_PATH}/${filename}

cd $PPCAS
sh run.sh $file $n_partitions $cassandra_ip $chunk $out_name

cd $TC
./t_coffee $file -extend_mode very_fast_triplet -dp_mode myers_miller_pair_wise -table_name $out_name -cassandra_seed $cassandra_ip -chunk_size $chunk