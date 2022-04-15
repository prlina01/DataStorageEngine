# Data Storage Engine 
A database engine (or storage engine) is the underlying software component that a database management system (DBMS) uses to create, read, update and delete (CRUD) data from a database.

Information in a database is stored as bits laid out as data structures in storage that can be efficiently read from and written to given the properties of hardware. Typically the storage itself is designed to meet requirements of various areas that extensively utilize storage, including databases. A DBMS in operation always simultaneously utilizes several storage types (e.g., memory, and external storage), with respective layout methods. 

This data storage engine written fully in **Golang**, with it's advanced data structures, is designed to be able to process huge amount of incoming streaming and batch data efficiently. 

**Data structures** used in this project: Write Ahead Log, Skip list, memtable, SStable along with Index and Summary tables, Merkle tree, LRU cache, Bloom Filter, HyperLogLog, Count-min Sketch 

![Screenshot from 2022-04-15 19-03-23](https://user-images.githubusercontent.com/36077702/163627645-d8b59a88-d408-4a94-b664-49313c493906.png)

![Screenshot from 2022-04-15 19-03-34](https://user-images.githubusercontent.com/36077702/163627714-77146a98-45ea-4bad-a610-2fa62478732a.png)

## Default project configuration  

```yaml  
wal_size: 10  
memtable_size: 10  
low_water_mark: 1  
cache_size: 5  
max_lsm_tree_level: 4  
max_lsm_nodes_first_level: 4  
max_lsm_nodes_other_levels: 2  
false_positive_rate: 0.05  
hll_precision: 4  
max_tokens: 10 
token_bucket_interval: 30  
cms_delta: 0.01  
cms_epsilon: 0.01  
