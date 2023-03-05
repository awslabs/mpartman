### How the package functions work under the hood.

---
#### Adding partitions
 1. The partitioned table does not have neither default partition nor MINMAXVALUE partitions.
    - This is a simplest way to add the partitions.
    - Partition is adding as `create table` and then `alter table ... attach partition`.
    - Share Update Exclusive lock is acquired in this case, so DML (select, insert, update, delete) is not blocked.
 2. The partitioned table has either default partition or MINMAXVALUE partitions.
    - Access Exclusive lock is acquired in this case because we have to detach default partition and/or MINMAXVALUE partition before.
    - Such a lock blocks the parallel DML and vice versa.

---
#### Auto interval partitions
 - PostgreSQL does not support automatic partitioning, so you have to use some scheduler like pg_cron.

---
#### Merge/Split partitions
 - The algorithm is as follows:
   - Desired partition(s) is/are detached
   - New one(s) is/are created
   - The data is inserted back into the table from detached partition(s)
 - Due to some partitions are detached the Access Exclusive lock is acquired, thus the parallel DML is blocked.

---
#### Drop/Detach (retention) partitions
 - This activity causes the Access Exclusive lock acquisition, so it blocks the DML too.

---
#### Known exceptions behaviour
 - Mostly in the functions `p_raiseexception boolean` parameter is used.
 - If set to false, a warning is issued in the following cases:
   - The table is unavailable
   - The table is not partitioned
   - Wrong partition strategy
   - The table is a partition when we need main table
   - The table is partitioned by more than one column
   - Wrong partitioning column data type
   - Only one partition for values FROM (MINVALUE) TO (MAXVALUE) is used
   - Future partition overlaps existing one
   - Some others merge/split specific warnings
 - If set to true, an exception will be raised

---
#### Table triggers
Not to fire table triggers, when the records are inserted back into the partitioned table,  
parameter `session_replication_role` is set to `replica` at the begin of function  
and is set to the previous value at the end of function.  
That parameter can be set by superuser only, so the function `f_set_config`  
with `security definer` is used to set this parameter for current transaction.  
Only `session_replication_role` configuration parameter can be set by this function.  

---
#### Partition name conventions
 - In general Mpartman functions don't depend on the partition name, so feel free to name your partitions as you want. When you create the partitions by Mpartman functions the partition name looks as follows:
 - First level partition name consists of:
```
   partitioned-table-name_p_first-level-partitioning-column-value_4-characters-suffix
```
 - Subpartition name consists of:
```
   partitioned-table-name_p_first-level-partitioning-column-value_subp_inidex_4-characters-suffix
```
 - If the final partition name is longer than 63 characters, then the name looks like this:
```
   npt_set_of_random_characters
```

