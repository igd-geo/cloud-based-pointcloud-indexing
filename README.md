**Progressive Point Cloud Indexing with Big Data Technologies**

This application enables highly parallelised and linearly scalable point cloud preprocessing capabilities through the use of Apache Spark and Apache Cassandra. The progressive mode offers further functionality possibilities through the use of successive indexing in individual regions of the point cloud.

This repository contains the source code and the libraries that were utilised (in the form of pom.xml and jar files). A generated jar file can be executed from the command line of the master node called from within the Spark Cassandra cluster. If called using the command line, the following parameters must be defined in the given order.
- Master-URL
- Cassandra Host IP
- Generate new Cassandra Table?: <true/false><br/>
      -> true = a new Cassandra table is generated <br/>
      -> false = the parameters are input into an exisiting table

- Keyspace Name: if the previous parameter is false, the current keyspace name is searched for and the parameter is written into it
- Table Name
- Cassandra Replication Factor
- Maximum number of points per node
- Spacing Value: The size of the virtual cells used for the sampling is calculated using (2^spacing) * scale_of_lastFilePoints 
- Sampling Strategy: <0,1,2><br/>
 	-> 0 = first point of the virtual cell<br/>
 	-> 1 = middle point of the virtual cell<br/>
 	-> 2 = random points of the node
- Number of levels per nested octree
- Folder location of the LAS files
- Add points to indexed regions? <true,false>:<br/> 
 	-> true = the new points are inserted into exisiting indexed regions
 	-> false = the new points replace the exisiting indexed points of the same region
- Spark max. split size <in bytes><br/>
    -> 1: Uses the default value of the Spark application


For example, running the application in the Apache Spark standalone cluster mode would look like this:<br/>
./spark-submit --class process.Main "<name_of_jarfile>.jar" spark://192.163.0.97:7077 192.163.0.56 true indexed pc1 1 32768 11 2 8 
/home/ubuntu/Documents/Pointclouds/ false false -1

Instructions of how a Spark Cassandra cluster can be deployed can be found in https://opencredo.com/blogs/deploy-spark-apache-cassandra/.

Note: To run the application in progressive mode, the current view frustrum of a visualisation application must first be bound to the indexing application. The interface that was implemented for this purpose is in the class "Main" in line 274 and is given by the variable "Frustum".
