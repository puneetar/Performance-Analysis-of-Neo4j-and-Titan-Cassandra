Performance-Analysis-of-Neo4j-and-Titan-Cassandra
=================================================
ABSTRACT:
----------
Social Network Graph have been among the prominent Graph types in Software Industry for a long time. Keeping in mind the requirements of terabytes of Data storage and milliseconds of performance, we would like to compare different types of Databases and determine the best one suited for Social Network Graphs. 

In this myriad of different type of Databases options such as Relational DB, NO-SQL DB, Graph DB, Column oriented DB and many more, we would like to concentrate our study over Graph DB and Column oriented DB as they tend to be the top contenders of fulfilling all requirements of Social Network Graphs. 
1.Graph DB: Specially designed and optimized for storing Graph type data models.
2. Column Oriented DB : Used by companies like Facebook, Google to store Graph Data.

For our study we chose Neo4j (Graph DB)  and Titan + Cassandra (Graph API wrapper over Column Oriented DB).



PROCEDURE OF ANALYSIS:
----------------------
In this project we will run a series of experiments (benchmarks) on both the DBs and analyse their performance in terms of speed and memory footprint. We will be using 2 Datasets, one with 50 Million nodes data and other with 5 Million nodes and run the following experiments.

Micro-Benchmarks:
Insert / Get / Delete / Update Node and Node Properties
Insert / Get / Delete / Update Edge and Edge Properties
Macro Benchmarks (combining multiple micro-benchmarks):
K- hop neighbors
Get selection of Nodes from given filter expression
Getting neighbors (with edge filter) of a set of nodes 
Set edge between set of nodes (selected by a filter)
Aggregate functions like count() , sum(), avg(), min() , max()
Algorithms:
Shortest path Algorithm
