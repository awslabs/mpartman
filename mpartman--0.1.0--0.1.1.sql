-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mpartman" to load this file. \quit

create or replace view @extschema@.v_partitioned_tables as
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

create or replace view @extschema@.v_pt_tree
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

create or replace function @extschema@.f_find_part_by_value(
    p_table_name text,
    p_count_defpart boolean,
    p_value text,
    p_value2 text default null::text
)
 returns text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_type text;
    l_type2 text;
    p_table_oid oid;
    p_table_oid2 oid;
    l_sql text;
    l_t text;
begin
    -- Get table oid by name
    p_table_oid := @extschema@.f_is_eligible_for_detach(p_table_name);
    if (p_table_oid is null) then
       raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
       return null;
    end if;

    -- Get partition column type
    select vpt.part_col_data_type into l_type
    from @extschema@.v_partitioned_tables vpt where vpt.oid = p_table_oid;

    -- Get part name by value
    l_table_name := @extschema@.f_get_part_by_value(p_table_oid, l_type, p_value);

    select v1.part_boundary into l_t from @extschema@.v_pt_tree v1 where v1.part_name = l_table_name;
    -- Check the behavior with default partition
    if (not p_count_defpart and l_table_name is not null) then
	if (l_t = 'DEFAULT') then
		return null;
	end if;
    end if;

    if (p_value2 is null or l_t = 'DEFAULT') then
      return l_table_name;
    else
      -- Get subpartition table oid by name
      select oid into p_table_oid2 from pg_class
      where oid = array_to_string(parse_ident(l_table_name),'.')::regclass::oid;

      -- Get subpartition column type
      select vpt.part_col_data_type into l_type2
      from @extschema@.v_partitioned_tables vpt where vpt.oid = p_table_oid2;

      -- Get subpart name by values
      if (l_type2 is null) then
        return null;
      else
        l_table_name := @extschema@.f_get_part_by_value(p_table_oid, l_type, p_value, l_type2, p_value2);
        return l_table_name;
      end if;

    end if;

end;
$function$
;
create or replace function @extschema@.f_find_part_by_value(
    p_table_oid oid,
    p_count_defpart boolean,
    p_value text,
    p_value2 text default null::text
)
 returns text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_type text;
    l_type2 text;
    p_table_oid2 oid;
    l_sql text;
    l_t text;
begin

    -- Get partition column type
    select vpt.part_col_data_type into l_type
    from @extschema@.v_partitioned_tables vpt where vpt.oid = p_table_oid;

    -- Get part name by value
    l_table_name := @extschema@.f_get_part_by_value(p_table_oid, l_type, p_value);

    select v1.part_boundary into l_t from @extschema@.v_pt_tree v1 where v1.part_name = l_table_name;
    -- Check the behavior with default partition
    if (not p_count_defpart and l_table_name is not null) then
	if (l_t = 'DEFAULT') then
		return null;
	end if;
    end if;

    if (p_value2 is null or l_t = 'DEFAULT') then
      return l_table_name;
    else
      -- Get subpartition table oid by name
      select oid into p_table_oid2 from pg_class
      where oid = array_to_string(parse_ident(l_table_name),'.')::regclass::oid;

      -- Get subpartition column type
      select vpt.part_col_data_type into l_type2
      from @extschema@.v_partitioned_tables vpt where vpt.oid = p_table_oid2;

      -- Get subpart name by values
      if (l_type2 is null) then
        return null;
      else
        l_table_name := @extschema@.f_get_part_by_value(p_table_oid, l_type, p_value, l_type2, p_value2);
        return l_table_name;
      end if;

    end if;

end;
$function$
;

create or replace function @extschema@.f_get_hash_part_by_value(
    p_table_oid oid,
    p_type text,
    p_value text
)
 returns text
 language plpgsql
as $function$
declare
    l_sql text;
    l_bool boolean;
    l_rec record;
begin
    for l_rec in (
                  select
                    part_name,
                    regexp_replace(part_boundary, '(.*)modulus ([[:digit:]]+), (.*)', '\2') as mdl,
                    regexp_replace(part_boundary, '(.*)remainder ([[:digit:]]+)\)', '\2') as rmd
                  from @extschema@.v_pt_tree
                  where main_table_oid = p_table_oid
                 )
    loop
        l_sql := 'select satisfies_hash_partition(' || p_table_oid || ', ' || l_rec.mdl || ', ' || l_rec.rmd || ', ' || p_value || '::' || p_type ||')';
        execute l_sql into l_bool;
        if l_bool
        then
          return l_rec.part_name;
        end if;
    end loop;

    return null::text;

end;
$function$
;
create or replace function @extschema@.f_get_part_by_value(
    p_table_oid oid,
    p_type text,
    p_value text,
    p_type2 text default null::text,
    p_value2 text default null::text
)
 returns text
 language plpgsql
as $function$
declare
    l_sql text;
    l_col_name text;
    l_col_name2 text;
    l_table_name text;
    r_table_name text;
    l_table_oid oid;
    r_table_oid oid;
    l_where_clause text;
    l_cnt integer;
    l_pstrategy text;
begin
    -- Get the partition column name.
    l_col_name := @extschema@.f_get_part_column(p_table_oid);

    -- Get partition strategy for main table
    select vpt.partition_strategy into l_pstrategy
    from @extschema@.v_partitioned_tables vpt
    where vpt.oid = p_table_oid;

    if (l_pstrategy = 'hash') then
        r_table_name := @extschema@.f_get_hash_part_by_value(p_table_oid, p_type, p_value);
        if (r_table_name is not null) then
            select r_table_name::regclass::oid into r_table_oid;
        end if;
    else

        l_sql := 'select v1.aaa[1], v1.aaa[2] from (select case ';

        -- Find the suitable partition name
        For l_table_name, l_table_oid, l_where_clause in 
        select
            pt.relnamespace::regnamespace || '.' || pt.relname as relname,
            pt.oid,
            pg_get_partition_constraintdef(pt.oid) as wc
        from
            pg_class base_tb, pg_inherits i, pg_class pt
        where
            i.inhparent = base_tb.oid and
            pt.oid = i.inhrelid and
            base_tb.oid = p_table_oid
        Loop
            l_where_clause := regexp_replace(l_where_clause, l_col_name, '''' || p_value || '''::' || p_type, 'g');
            l_sql := l_sql || 'when ' || l_where_clause || ' then array[' || '''' || l_table_name || '''' || ',' || '''' || l_table_oid || '''' || ']';
        End Loop;
        if (l_where_clause is null) then
            return r_table_name;
        end if;
        l_sql := l_sql || ' else array[null::text,null::text] end as aaa ) v1';

        execute l_sql into r_table_name, r_table_oid;

    end if;

    if (p_type2 is null) then
        return r_table_name;
    else

      -- Get partition strategy for a first level partition
      l_pstrategy := null;
      select vpt.partition_strategy into l_pstrategy
      from @extschema@.v_partitioned_tables vpt
      where vpt.oid = r_table_oid;

      if (l_pstrategy is null) then
        return r_table_name;
      elsif (l_pstrategy = 'hash') then
        r_table_name := @extschema@.f_get_hash_part_by_value(r_table_oid, p_type2, p_value2);
      else

        r_table_name := null;
        -- Get the subpartition column name.
        l_col_name2 := @extschema@.f_get_part_column(r_table_oid);
        -- Find the suitable partition name
        l_sql := 'select v1.aaa[1], v1.aaa[2] from (select case ';
        For l_table_name, l_table_oid, l_where_clause in 
        select
            quote_ident(pt.relnamespace::regnamespace::text) || '.' || quote_ident(pt.relname) as relname,
            pt.oid,
            pg_get_partition_constraintdef(pt.oid) as wc
        from
            pg_class base_tb, pg_inherits i, pg_class pt
        where
            i.inhparent = base_tb.oid and
            pt.oid = i.inhrelid and
            base_tb.oid = r_table_oid
        Loop
            l_where_clause := regexp_replace(l_where_clause, l_col_name, '''' || p_value || '''::' || p_type, 'g');
            l_where_clause := regexp_replace(l_where_clause, l_col_name2, '''' || p_value2 || '''::' || p_type2, 'g');
            l_sql := l_sql || 'when ' || l_where_clause || ' then array[' || '''' || l_table_name || '''' || ',' || '''' || l_table_oid || '''' || ']';
        End Loop;
        l_sql := l_sql || ' else array[null::text,null::text] end as aaa ) v1';
        execute l_sql into r_table_name, r_table_oid;

      end if;

      return r_table_name;

    end if;

end;
$function$
;
