### Table of Contents
* Public functions
  - [Parameters description](#parameters-description)
  - LIST partition functions
    * [Add interval partitions](#add-list-interval-partitions)
    * [Add partitions](#add-list-partitions)
    * [Merge partitions](#merge-list-partitions)
    * [Split partition](#split-list-partition)
  - RANGE partition functions
    * [Add interval partitions (overloaded 1)](#add-range-interval-partitions-1)
    * [Add interval partitions (overloaded 2)](#add-range-interval-partitions-2)
    * [Add partitions](#add-range-partitions)
    * [Merge partitions](#merge-range-partitions)
    * [Split partition](#split-range-partition)
    * [Check the gaps](#check-the-gaps)
  - General functions
    * [Do retention by value](#do-retention-by-value)
    * [Detach partition by value](#detach-partition-by-value)
    * [Drop partition by value](#drop-partition-by-value)
    * [Find partition by value](#find-partition-by-value)
    * [Get the partitions information for a table](#get-the-partitions-information-for-a-table)
* [DEFAULT partition support](#default-partition-support)
* [Support of MAXVALUE and MINVALUE for range partitions](#support-of-maxvalue-and-minvalue-for-range-partitions)
* [The subpartitions support](#the-subpartitions-support)
* [Locks](#locks)
* [Exceptions vs Warnings](#exceptions-vs-warnings)
* [Schema qualified table name](#schema-qualified-table-name)
* [Supplementary views](#supplementary-views)

## Public functions

### Parameters description

*The table or partitions names. See [Schema qualified table name](#schema-qualified-table-name) for details.*  
*Check the [the subpartitions support](#the-subpartitions-support) for details about subpartition templates.*  

    p_table_name text  -  Main partitioned table name
    p_part_name text   -  First level partition name
    p_part_arr text[]  -  Text array of the first level partition names
    p_subp_templ text  -  First level partition name to use as a subpartition template.
                          It even can be different table, but the section column must have
                          the same name and data type as a main partitioned table.
  

*Partitioning column values with explicit data type definition*  

    p_start_value anyelement
    p_end_value anyelement
  

*Array with range boundaries*  

    p_bond_arr anyarray  -  range boundaries with explicit data type definition
  
  
Interval definition  

    p_interval text  -  The text representation of the interval to create the interval partitions.
                        For example: '1 day' or '1.5' or '-1 week'.
                        See the corresponding functions descriptions for details.
  
 
*Partitioning column values to find corresponding partition* 

    p_value text
    p_value2 text
  

*The list(s) of values to create or split list partition(s)*  

    p_values jsonb  -  The JSONB data type is choosen because it can contain
                       the arrays with different dimensions as well as the single values.
  

*Boolean parameters to define the corresponding behavior.*  
*See [default partition support](#default-partition-support), [support of MAXVALUE and MINVALUE for range partitions](#support-of-maxvalue-and-minvalue-for-range-partitions),*  
*and [Exceptions vs Warnings](#exceptions-vs-warnings) for details.*  

    p_keepdefault boolean      -  Create if not exists (true)/Drop if exists (false) the default partition.
    p_keepminmaxvalue boolean  -  Use (true)/Rid of (false) MINVALUE and/or MAXVALUE as a range boundary
                                  if it was used before.
    p_raiseexception boolean   -  Raise exception (true)/Raise warning (false) in well known cases.
    p_count_defpart boolean    -  When you find a prtition by value this parameter defines
                                  should the DEFAULT partition be returned as a result.
                                  If false and the DEFAULT partition contains the value, then NULL is returned.


### LIST partition functions

#### Add list interval partitions
> **Note:** To use this function there must be at least one partition.
```sql
f_list_add_interval_partitions(
    p_table_name text,
    p_interval text,
    p_end_value anyelement,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Creates an interval partition set until **p_end_value** either starting from the very end  
or from the very beginning, depending on the **p_interval** sign.  
If **p_interval** is positive the partitions are appended,  
if **p_interval** is negative the partitions are prepended.  
Returns a set of created partition names.

Examples:  
```sql
select * from mpartman.f_list_add_interval_partitions(
    'myschema.mytable',
    '10',
    '1800'::integer,
    false,
    false,
    'myschema.mytable_p_50'
);
select * from mpartman.f_list_add_interval_partitions(
    'myschema.mytable',
    '-1.5',
    '80'::numeric,
    false,
    false,
    'myschema.mytable_p_250_0000'
);
```

#### Add list partitions
```sql
f_list_add_partitions(
    p_table_name text,
    p_values jsonb,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Creates a partition set with the partitioning column values specified in **p_values**.  
Returns a set of created partition names.

Examples:
```sql
select * from mpartman.f_list_add_partitions(
    'myschema.mytable',
    '[["AA","bb","Rr"], "WE", ["UA","US"]]'::jsonb,
    false,
    true
);
select * from mpartman.f_list_add_partitions(
    'myschema.mytable',
    (select array_to_json(array(select some_column from some_table))::jsonb),
    false,
    false
);
```

#### Merge list partitions
```sql
f_list_merge_partitions(
    p_table_name text,
    p_part_arr text[],
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Merges partition set into one partition. Partition names are specified in **p_part_arr**.  
Returns created partition name.

Examples:
```sql
select * from mpartman.f_list_merge_partitions(
    'myschema.mytable',
    array['myschema.mytable_p_a','myschema.mytable_p_b','myschema.mytable_p_c'],
    true
);
```

#### Split list partition
```sql
f_list_split_partition(
    p_part_name text,
    p_values jsonb,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Splits a partition into partition set depending on  
the partitioning column values specified in **p_values**.  
Returns a set of created partition names.  

Examples:
```sql
select * from mpartman.f_list_split_partition(
    'myschema.mytable_p_aa',
    '[["AA","BB"], "CC"]'::jsonb,
    false,
    true
);
```
> **Note**:   
> The set of values specified in the **p_values** must be equal to
> the set of partitioning column values within a partition specified in the **p_part_name**.

### Range partition functions

#### Add range interval partitions 1
> **Note:** To use this function there must be at least one partition.
```sql
f_range_add_interval_partitions(
    p_table_name text,
    p_interval text,
    p_end_value anyelement,
    p_keepdefault boolean,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Creates an interval partition set until **p_end_value** either starting from the very end  
or from the very beginning, depending on the **p_interval** sign.  
If **p_interval** is positive the partitions are appended,  
if **p_interval** is negative the partitions are prepended. 
Returns a set of created partition names.
  
Examples:
```sql
select * from mpartman.f_range_add_interval_partitions(
    'myschema.mytable',
    '1 day',
    '2022-08-01'::date,
    false,
    true,
    false
);
select * from mpartman.f_range_add_interval_partitions(
    'myschema.mytable',
    '-1 month',
    '2019-01-01'::date,
    false,
    true,
    false
);
select * from mpartman.f_range_add_interval_partitions(
    'myschema.mytable',
    '10',
    '1800'::integer,
    false,
    true,
    false,
    'myschema.mytable_p_50'
);
```

#### Add range interval partitions 2
```sql
f_range_add_interval_partitions(
    p_table_name text,
    p_interval text,
    p_start_value anyelement,
    p_end_value anyelement,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Creates an interval partition set starting from **p_start_value** until **p_end_value**.  
Parameter **p_interval** must be positive.  
Returns a set of created partition names.
  

Examples:
```sql
select * from mpartman.f_range_add_interval_partitions(
    'myschema.mytable',
    '1 month',
    '2022-01-01'::date,
    '2023-01-01'::date,
    false,
    false
);
```

#### Add range partitions
```sql
f_range_add_partitions(
    p_table_name text,
    p_bond_arr anyarray,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Creates a partition set with arbitrary interval. The boundaries are specified in the **p_bond_arr**.  
Returns a set of created partition names.  

Examples:
```sql
select * from mpartman.f_range_add_partitions(
    'myschema.mytable',
    array['2022-09-03', '2022-09-02', '2022-09-01']::date[],
    false,
    true
);
```
> **Note:** The boundaries array can be unsorted.

#### Merge range partitions
```sql
f_range_merge_partitions(
    p_table_name text,
    p_part_arr text[],
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Merges partition set into one partition. Partition names are specified in **p_part_arr**.  
Returns created partition name.

Examples:
```sql
select * from mpartman.f_range_merge_partitions(
    'myschema.mytable',
    array['myschema.mytable_p_1','myschema.mytable_p_2','myschema.mytable_p_3'],
    true
);
```

#### Split range partition
```sql
f_range_split_partition(
    p_part_name text,
    p_bond_arr anyarray,
    p_keepdefault boolean,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
```
Splits a partition into partition set depending on the boundaries specified in the **p_bond_arr**.  
Returns a set of created partition names.  
  

Examples:
```sql
select * from mpartman.f_range_split_partition(
    'myschema.mytable_p_2022-10-18',
    array['2022-10-18', '2022-10-23', '2022-10-25']::date[],
    false,
    false,
    true
);
```
> **Note:** The boundaries array can be unsorted.

#### Check the gaps
```sql
f_check_gap_range(
    p_table_name text
)
 returns table (
        partition_name text,
        left_boundary text,
        right_boundary text,
        previous_left_boundary text,
        previous_right_boundary text
 )
```
Checks whether the set of range boundaries is continuous.  
If there is at least one record returned the range has a gap.  
  
Examples:
```sql
select * from mpartman.f_check_gap_range(
    'myschema.mytable'
);
```

### General functions
#### Do retention by value
```sql
f_do_retention(
    p_table_name text,
    p_direction text, -- Either "old" or "new"
    p_action text,    -- Either "drop" or "detach"
    p_value anyelement,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean,
    p_retentionschema text default null::text
)
 returns setof text
```
Detaches/Drops set of partition by partitioning column value.  
Returns detached/dropped partition names.  
Moves (optional) detached partitions and its subpartitions into p_retentionschema schema.  
  
Examples:  
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

#### Detach partition by value
```sql
f_detach_part(
    p_table_name text,
    p_value text,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean,
    p_retentionschema text default null::text
)
 returns text
```
Detaches partition by partitioning column value.  
Returns detached partition name.  
Moves (optional) detached partition and its subpartitions into p_retentionschema schema.  
  
Examples:  
```sql
select * from mpartman.f_detach_part(
    'myschema.mytable',
    '2022-10-01'::date,
    true,
    false,
    'archschema'
);
```

#### Drop partition by value
```sql
f_drop_part(
    p_table_name text,
    p_value text,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean
)
 returns text
```
Drops partition by partitioning column value.  
Returns dropped partition name.  
  
Examples:
```sql
select * from mpartman.f_drop_part(
    'myschema.mytable',
    '2022-10-01'::date,
    false,
    true
);
```

#### Find partition by value
```sql
f_find_part_by_value(
    p_table_name text,
    p_count_defpart boolean,
    p_value text,
    p_value2 text default null::text
)
 returns text
```
Finds partition by partitioning column value.  
If **p_value2** is not null, the subpartition name will be finding.
  
Examples:
```sql
select * from mpartman.f_find_part_by_value(
    'myschema.mytable',
    false,
    '2022-10-01',
    'AA'
);
```

#### Get the partitions information for a table
> **Note:** Table name for this function must be schema qualified.
```sql
f_get_all_part_info(
    p_table_name text
)
returns table(
    main_table_name     text,
    partition_name      text,
    partition_boundary  text,
    partition_strategy  text
)
```
Returns brief information set about the partitioned table.  

### DEFAULT partition support
The presence of a default partition is controlled by the parameter **p_keepdefault** as described [above](#parameters-description).  
Default partition can not be dropped if there is any data inside. To be able to drop the default partition
you need to create new partitions that cover the partitioning column values from the records in the default partition.  
  
When you create new partitions and default partition exists the following algorithm is used:
- If default partition contains any records, then default partition is being detached
- New partitions are created
- New default partition is created
- The records from detached default partition inserted back into the main table
> **Note:** Keep in mind that default partition impacts the locking process when DDL is applied.

### Support of MAXVALUE and MINVALUE for range partitions
The range partitions with MAXVALUE or MINVALUE as a boundary are supported and controlled  
by the parameter **p_keepminmaxvalue** as described [above](#parameters-description).  
If you set parameter **p_keepminmaxvalue** to true, the algorithm is the same as for default partition.
> **Note:**  
> The presence of such partitions most likely leads to the copying a lot of records  
> when you create new partitions. It impacts the locking proces as well.  

### The subpartitions support
You can not manage the subpartitions directly by functions from this set.  
The subpartitions are supported as a part of new partition creation.  
If you don't define parameter **p_subp_templ** then "last" partition is used as a template  
for interval partition strategy, and random partition for list partition strategy.  
As a value of parameter **p_subp_templ** you can specify either the subpartition from the table  
you are dialing with or other partitioned table with partitioning column with same name and data type. 

### Locks
**ACCESS SHARE** lock is taken by the following functions:
- f_check_gap_range
- f_find_part_by_value
- f_get_all_part_info
  
If neither default partition nor MinMaxValue partitions exist,  
and you are not going to have them, then **Share Update Exclusive** lock is taken when you create new partitions.  

For all other cases **ACCESS EXCLUSIVE** lock is taken and as a consequence the DML operators with table you use  
are blocked or the the functions from this set are blocked by DML.

### Exceptions vs Warnings
The parameter **p_raiseexception** controls whether you get either an error or just warning for "well known" cases  
such as wrong table name, wrong partition strategy, wrong column data type, tables overlapping, etc.  
For all other cases the exception raising is possible.  

### Schema qualified table name
To be sure you don't have any issues with table/partiton naming use the schema qualified table name everywhere.  
As an axception you can use the table/partition name without the schema when that schema is in the *search_path*.  

### Supplementary views
For internal purposes, two views are used:
- v_partitioned_tables
- v_pt_tree  
  
***

[back to the table of contents](#table-of-contents)  

