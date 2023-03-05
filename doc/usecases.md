## Typical use cases

You can use Mpartman to manage partitions. This includes such operations as creating, deleting, or finding partitions.  

### To add RANGE partitions with Mpartman
- You already have a partitioned table with set of partitions
  - It's necessary to append partitions until August 1st 2022
```sql
select * from mpartman.f_range_add_interval_partitions(
    'myschema.mytable',  -- Partitioned table name
    '1 day',             -- Interval (if it's positive, then partitions are appended)
    '2022-08-01'::date,  -- Add partitions until this value (the right value in the last partition aligned by "Interval")
                         --   You can use e.g. (current_date + '5 days'::interval)::date instead
                         --   or you can find "last partitioning column value" and use it to append precisely 5 partitions
                         --   (select (b + '5 days'::interval)::date from mpartman.f_get_min_max_value_range('myschema.mytable'::regclass::oid, null::date, 'max') as (a text, b date))
    false,               -- We don't need default partition
    false,               -- We don't need MAXVALUE partition
    false                -- Raise a warning for known errors
);
```
  - It's necessary to prepend partitions starting from January 1st 2019
```sql
select * from mpartman.f_range_add_interval_partitions(
    'myschema.mytable',  -- Partitioned table name
    '-1 month',          -- Interval (if it's negative, then partitions are prepended)
    '2019-01-01'::date,  -- The left value in the first partition aligned by "Interval"
    false,               -- We don't need default partition
    false,               -- We don't need MINVALUE partition
    true                 -- Raise an exception for all errors
);
```

---
### To get rid of unnecessary RANGE partitions with Mpartman
- Suppose you do retention for the old "date" partitions (function f_do_retention is for RANGE partitioning strategy only)
```sql
select * from mpartman.f_do_retention(
    'myschema.mytable',  -- Partitioned table name
    'old',               -- Detach partitions starting from the oldest one
    'detach',            -- What to do "detach" or "drop"
    '2019-12-01'::date,  -- Detach partitions including this value
    false,               -- Detach MINVALUE partition too ("false" means not to keep)
    true,                -- Raise an exception for all errors
    'histschema'         -- The schema name to move detached partitions to (optional)
);
```
- You have a table with RANGE partitioning strategy and column type integer. Let's suppose you need to drop "newest" partitions
```sql
select * from mpartman.f_do_retention(
    'myschema.mytable',  -- Partitioned table name
    'new',               -- Drop partitions including the newest one starting from the "value"
    'drop',              -- What to do "detach" or "drop"
    '20000'::integer,    -- Drop partitions starting from this value
    true,                -- Keep MAXVALUE partition (is not a good idea)
    true                 -- Raise an exception for all errors
);
```

You can drop/detach a single RANGE/LIST partition using `f_drop_part` and `f_detach_part` functions.  

---
### To find out a RANGE/LIST partition name by partitioning column value with Mpartman
- Partitioned table does not have subpartitions
```sql
select * from mpartman.f_find_part_by_value(
    'myschema.mytable',  -- Partitioned table name
    false,               -- Whether return default partition name or not
    '2022-10-01'         -- Partitioning column value as a text (first level partition)
);
```
- Partitioned table has subpartitions
```sql
select * from mpartman.f_find_part_by_value(
    'myschema.mytable',  -- Partitioned table name
    false,               -- Whether return default partition name or not
    '2022-10-01',        -- Partitioning column value as a text (first level partition)
    'AA'                 -- Partitioning column value as a text (subpartition)
);
```

---
### To create a LIST partition for each user account with Mpartman
```sql
select * from mpartman.f_list_add_partitions(
    'myschema.mytable',  -- Partitioned table name
    (select array_to_json(array(select account_id from user_accounts))::jsonb),
    false,               -- We don't need default partition
    true                 -- Raise an exception for all errors
);
```

There is no error if the corresponding partitions already exist.  

---
### To create arbitrary LIST partitions with Mpartman
```sql
select * from mpartman.f_list_add_partitions(
    'myschema.mytable',  -- Partitioned table name
    '[["AA","bb","Rr"], "WE", ["UA","US"]]'::jsonb,
    false,               -- We don't need default partition
    true                 -- Raise an exception for all errors
);
```
As a result 3 partitions will be added.  

---
### To create LIST partitions with sequential partitioning column values with Mpartman
```sql
select * from mpartman.f_list_add_interval_partitions(
    'myschema.mytable',  -- Partitioned table name
    '10',                -- Interval between partitioning column values (Can be negative. The logic is the same as for the RANGE partition functions.)
    '1800'::integer,     -- Last partitioning column value
    false,               -- We don't need default partition
    true                 -- Raise an exception for all errors
);
```
The partitions will be append if "last value" < 1800

---
To get more information please read [function descriptions](./functions.md)  

