#!/bin/bash

#title           : air_slurm_exp.sh
#description     : This script will iteratively deploy slurm tasks for a given range of throughput (tp), No.of nodes (N) and message per sec (mps)
#author		 	 : Vinu Venugopal, University of Luxembourg
#date            : 15-02-2020
#version         : 0.1    
#usage		     : bash air_slurm_exp.sh <trial_no>
#===================================================================

if [[ $# -eq 0 ]] ; then
    echo 'Pass a TRIAL number! A trial number is a unique id for the result directory of the current run. E.g., ./xxx.sh 123'
    exit 1
fi

export AIR_INNER_LAUNCHER=./air-inner-launcher-xx.sh

export AIR_LOG=./airlog

mkdir -p EXP && cd EXP && mkdir AIR-PLOT-3_$1  && cd AIR-PLOT-3_$1 && mkdir -p {1,2,3,4,5,6,7,8}NODES && cd ../..

for N in $(seq 3 1 3)
do
counter=0
for tp in $(seq 30000000 1000000 39000000)
do
for mps in $(seq 1 1 2)
do

counter=$((counter+1))
echo "$counter  #CPUs: $nC   throughput $tp   mps: $mps"

echo "#!/bin/bash -l" > $AIR_INNER_LAUNCHER
echo "#SBATCH -J BDA-${N}-${counter}" >> $AIR_INNER_LAUNCHER
echo "#SBATCH -p batch" >> $AIR_INNER_LAUNCHER
echo "#SBATCH --nodes=$N" >> $AIR_INNER_LAUNCHER
echo "#SBATCH -C skylake" >> $AIR_INNER_LAUNCHER
echo "#SBATCH --ntasks-per-node=1" >> $AIR_INNER_LAUNCHER
echo "#SBATCH --cpus-per-task=28" >> $AIR_INNER_LAUNCHER
echo "#SBATCH --time=00:05:00" >> $AIR_INNER_LAUNCHER
echo "#SBATCH --qos qos-batch" >> $AIR_INNER_LAUNCHER
echo "#SBATCH -o $AIR_LOG/out_AirJob_${N}_%j" >> $AIR_INNER_LAUNCHER
echo "#SBATCH -e $AIR_LOG/err_AirJob_${N}_%j" >> $AIR_INNER_LAUNCHER

echo "export MSG_PER_SEC=$mps" >> $AIR_INNER_LAUNCHER

echo "export ITERATION_NO=$counter" >> $AIR_INNER_LAUNCHER

echo "export TRIAL=$1" >> $AIR_INNER_LAUNCHER


cat << 'EOF' >> ${AIR_INNER_LAUNCHER}

echo "Input throughput: $1   Nodes: $SLURM_NNODES   CPUs per task: $SLURM_CPUS_PER_TASK   No. of parallel slurm tasks (= parallel dataflows): $SLURM_NTASKS" >> ${SLURM_SUBMIT_DIR}/EXP/AIR-PLOT-3_${TRIAL}/${SLURM_NNODES}NODES/result-air_${ITERATION_NO}_${SLURM_JOB_ID}-$1-${MSG_PER_SEC}m.log

cd  $SLURM_SUBMIT_DIR/AIR1.1/Release_iris && module load toolchain/intel && srun ./AIRNP-${MSG_PER_SEC}m YSB $1 >> ${SLURM_SUBMIT_DIR}/EXP/AIR-PLOT-3_${TRIAL}/${SLURM_NNODES}NODES/result-air_${ITERATION_NO}_${SLURM_JOB_ID}-$1-${MSG_PER_SEC}m.log

EOF

##echo "terminated.. do: cat $SLURM_SUBMIT_DIR/RESULTS/AIR/${SLURM_NNODES}NODES/${SLURM_CPUS_PER_TASK}CORES/result-air_${SLURM_NNODES}-nodes_${SLURM_CPUS_ON_NODE}-cores_${SLURM_JOB_ID}-$1.log"


chmod +x ${AIR_INNER_LAUNCHER}

sbatch --test-only ${AIR_INNER_LAUNCHER} $tp

#sbatch ${AIR_INNER_LAUNCHER} $tp


done
done
done


