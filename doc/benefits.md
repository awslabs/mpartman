### Mpartman benefits

  - This package contains stateless functions only.
    * All the functions use _*pg_catalog*_ only and do not store any information about partitions.
    * No the common configuration. The functions parameters only.
    * So this package does not prevent you from manually interfering with partition management.
    * You do not need to set up any configuration for existing partitions.
    * You can improve your existing scripts with this package.
  - The package supports a wide set of partitioning column data types:
    - RANGE partition strategy
      * date, timestamp without time zone, timestamp with time zone
      * smallint, integer, bigint, decimal, numeric, real, float, double precision
    - LIST partition strategy
      * date, timestamp without time zone, timestamp with time zone
      * smallint, integer, bigint, decimal, numeric, real, float, double precision
      * text, character varying
  - The LIST partition strategy is supported.
  - MINVALUE and MAXVALUE are supported for range partitions.
    * The use of such partitions is configurable.
  - The subpartitions are supported.
  - The exclusive table lock is no longer an issue when you add new partitions if default partition is not used.
    * The use of the default partition is configurable.
  - This package provides DBA with wide functionality.
    * Few ways to create RANGE and LIST partitions:
      - Prepend
      - Append
      - Using array or JSON values
      - Using query result
    * Merge partitions
    * Split partitions
    * Detach partitions by partitioning column value
    * Drop partitions by partitioning column value
    * Partition retention function
    * The set of partition information functions and views
  - PostgreSQL versions from 11 to 15 are supported.
  - No binary library dependencies.
  - This package can be installed either as an extension or as a set of functions.
    * Since this package is just a set of SQL functions, you can modify the code to suit your requirements.

