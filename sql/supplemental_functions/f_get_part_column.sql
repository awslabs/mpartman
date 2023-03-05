create or replace function mpartman.f_get_part_column(
    p_table_oid oid
)
 returns text
 language plpgsql
as $function$
declare
    l_col_name text;
begin
    -- Get the partition column name.
    -- Only single column is supported.
    select
        col.attname into l_col_name
    from
        (select
             partrelid,
             partnatts,
             case partstrat
                  when 'l' then 'list'
                  when 'r' then 'range' end as partition_strategy,
             unnest(partattrs) column_index
         from
             pg_partitioned_table
         where partrelid = p_table_oid
        ) pt
    join
        pg_attribute col
    on
        col.attrelid = p_table_oid
        and col.attnum = pt.column_index
    ;

    return l_col_name;
end;
$function$
;
