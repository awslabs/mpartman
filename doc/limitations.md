### Potential issues

 - If partitioned table is referenced by foreign key, an exception "foreign key violation" can be raised when some partitions with data are detached.
 - A regular user must be an owner of the partitioned table in order to manage the table partitions. As a **VERY** **INSECURE** **APPROACH** you can add `security definer` into the corresponding Mpartman function definition to allow regular user manage any partitioned table, and then reinstall that function(s).

