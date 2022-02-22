# Data Storage Engine  
  
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
