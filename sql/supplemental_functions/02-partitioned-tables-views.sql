create or replace view mpartman.v_partitioned_tables as
select
    par.oid,
    par.relnamespace::regnamespace::text as schema,
    par.relname as table_name,
    pt.partnatts as part_num_columns,
    pt.column_index,
    col.attname as part_col_name,
    pt.partition_strategy,
    format_type(col.atttypid, NULL::integer) as part_col_data_type,
    pgi.inhparent::regclass,
    pgi.inhrelid::regclass
from
    (select
         partrelid,
         partnatts,
         case partstrat
              when 'l' then 'list'
              when 'r' then 'range'
              when 'h' then 'hash'
         end as partition_strategy,
         unnest(partattrs) column_index
     from
         pg_partitioned_table) pt
join
    pg_class par
    on par.oid = pt.partrelid
join
    pg_attribute col
    on col.attrelid = pt.partrelid
       and col.attnum = pt.column_index
left outer join
    pg_inherits pgi
    on pgi.inhrelid = par.oid
;

create or replace view mpartman.v_pt_tree
as
select
    pi.inhrelid as part_oid,
    pc.relnamespace::regnamespace || '.' || pc.relname as part_name,
    case ppt.partstrat
        when 'l' then 'list'
        when 'r' then 'range'
        when 'h' then 'hash'
    end as partition_strategy,
    pg_get_expr(pc.relpartbound, pc.oid) as part_boundary,
    quote_ident(pt.relnamespace::regnamespace::text) || '.' || quote_ident(pt.relname) as main_table_name,
    pi.inhparent main_table_oid
from
    pg_inherits pi,
    pg_class pc,
    pg_class pt,
    pg_partitioned_table ppt
where
    pc.oid = pi.inhrelid and
    pt.oid = pi.inhparent and
    pt.relkind = 'p' and
    pc.relkind in ('r', 'p') and
    ppt.partrelid = pi.inhparent
;

