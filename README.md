# AIR - Distributed Dataflow Processing with Asynchronous Iterative Routing

AIR engine is designed from scratch in C++ using the Message Passing Interface (MPI), pthreads for multithreading, and is directly deployed on top of a common HPC workload manager such as SLURM. AIR implements a light-weight, dynamic sharding protocol (referred to as "Asynchronous Iterative Routing"), which facilitates a direct and asynchronous communication among all client nodes and thereby completely avoids the overhead induced by the control flow with a master node that may otherwise form a performance bottleneck. 

A preprint of the paper based on this work can be found here: https://arxiv.org/pdf/2001.00164.pdf

AIR can be download also from our GitLab repository: https://gitlab.uni.lu/mtheobald/AIR

More details about the architecture of AIR and detailed experimental results (including a comparison with the modern scale-up SPEs) can be found in [1]. Our initial efforts that paved the way to developing this framework is detailed in [2]. A short video presentation depicting the motivation, architecture, and installation guidelines of AIR can be found at: https://www.youtube.com/watch?v=4A6jlSgvZPE&t


Build & Run:
------------

- Dependencies

An MPI distribution; preferably OpenMPI/MPICH2 

On Mac: 
```sh
  $brew install mpich
```
On Linux: 
```sh
  $sudo apt-get install mpich

Note: If the package mpich has no installation candidate. Do the following:
  $sudo apt-get install build-essential
  $sudo apt-get install gfortran
  $cd /tmp
  $wget http://www.mpich.org/static/downloads/1.4.1/mpich2-1.4.1.tar.gz
  $tar xzvf mpich2-1.4.1.tar.gz  
  $cd mpich2-1.4.1/
  $./configure
  $make
  $sudo make install
```

- Set compilation parameters
```sh
  Edit /path/to/AIR/CMakeLists.txt
```

- Build the project
```sh
  $cd /path/to/AIR/
  $mkdir Release
  $cd Release
  $cmake -DCMAKE_BUILD_TYPE=Release -G "CodeBlocks - Unix Makefiles" ../
  $make all
```

- Run a use-case
```sh
  $mpirun -np <no.of dataflows> ./AIR <use-case abbreviation> <input throughput>
  (E.g., mpirun -np 4 ./AIR YSB 100)
  ```  
  or
```sh
  $mpiexec -np <no.of dataflows> ./AIR <use-case abbreviation> <input throughput>
```

Running AIR on an HPC Cluster:
------------------------------

The following guidelines are based on the IRIS cluster (https://hpc.uni.lu/systems/iris/) of the University of Luxembourg. 

- Changes in C compiler settings (based on Intel MPI v18.0.1):
```sh
Edit CMakeLists.txt, change "/path/to/bin/mpic++"  to "mpiicpc"
```
>Via Eclipse: Go to the properties>C/C++Build>Settings>[GCCC++Complier>Command & GCCC++Complier>Command], make the above change (and then, clean and build -- you may get mpiicpc missing exception -- and ignore exceptions).
>In case of any change in the src code, replace the remote "src" directory with the new "src" and repeat the compilation/building steps.

- Perform resource allocation -- refer to SLURM documentation (https://hpc.uni.lu/users/docs/slurm.html) for advanced node allocation options.
```sh
  $si
```

- Load the MPI and CMake modules and build the project
```sh
  $module load toolchain/intel
  $module load devel/CMake/3.13.3-GCCcore-8.2.0  
  $mkdir Release_iris
  $cd Release_iris
  $cmake -DCMAKE_BUILD_TYPE=Release -G "CodeBlocks - Unix Makefiles" ../  
  $make clean
  $make all
```

- Run a use-case:
```sh
  $cd ../AIR/Release_iris/
  $srun -np <no. of dataflows> ./AIR <use-case abbreviation> <throughput>
  (E.g., srun -np 2 ./AIR WA 10000)
```

To run under *OVERCOMMIT* option
```sh
  $srun --overcommit -n <no. of dataflows> ./AIR <use-case abbreviation> <throughput>
```

Available Use-Cases:
--------------------

1. Yahoo! Streaming Benchmark (YSB)

2. Extended Yahoo! Streaming Benchmark (YSBM)

3. Simple Windowed Aggregation (WA)

4. Simple MapReduce (MR)

5. TCP-H Benchmark (TPCH) -- in progress

6. Yet Another Benchmark (YAB) -- in progress


Sanity Check Routines:
----------------------

- Postgres SQL queries for sanity checks can be found in Postgres-SanityCheck.sql

- SANITY_CHECK flag should be set as true to enable this functionality


Note: 
-----

- AGG_WIND_SPAN is by default set as 10sec, you may change that in *AIR/src/communication/Window.hpp* file. 

- By default PER_SEC_MSG_COUNT = 1 (If PER_SEC_MSG_COUNT = n >1, a per second input data -- based on the throughput value -- would be generated as n messages. For very high values of throughput, it is advisable to use n>1 to avoid MPI message size limit exceptions. 

- *AIR/src/dataflow/Vertex.hpp* has options for setting PIPELINE and SANITY_CHECK flags


References:
-----------

[1] VinuE.Venugopal,MartinTheobald,SamiraChaychi,andAmalTawakuli.2020. AIR: A Light-Weight Yet High-Performance Dataflow Engine based on Asynchro- nous Iterative Routing. In SBAC-PAD 2020: IEEE 32nd International Symposium on Computer Architecture and High Performance Computing Porto, Portugal, Sep- tember 8-11, 2020. (accepted).

[2] Vinu E. Venugopal and Martin Theobald. 2020. Benchmarking Synchronous and Asynchronous Stream Processing Systems. In CoDS-COMAD 2020: 7th ACM IKDD CoDS and 25th COMAD, Hyderabad India, January 5-7, 2020. ACM, 322–323. https://doi.org/10.1145/3371158.3371206

