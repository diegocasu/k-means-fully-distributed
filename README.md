# K-means fully distributed

## Description

The repository contains the Hadoop and Spark implementations of the k-means algorithm, 
designed assuming that the means cannot be saved in memory and without the use of In-Mapper combiners or broadcast variables.
In particular, the Hadoop version exploits the secondary sort pattern.

It was born as a side project of [K-means](https://github.com/diegocasu/k-means.git).
