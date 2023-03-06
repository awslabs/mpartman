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

create or replace function @extschema@.f_grant_table_privileges(
    p_schema text,
    p_table_name text,
    p_part_name text
)
 returns void
 language plpgsql
as $function$
declare
    l_record record;
begin

  for l_record in
  select
    format (
      'grant %s on table %I.%I to %I%s',
      string_agg(tg.privilege_type, ', '),
      p_schema,
      p_part_name,
      tg.grantee,
      case
        when tg.is_grantable = 'YES'
        then ' WITH GRANT OPTION'
        else ''
      end
    ) as grantsql
  from information_schema.role_table_grants tg
  join pg_tables t on t.schemaname = tg.table_schema and t.tablename = tg.table_name
  where
    tg.table_schema = p_schema
    and tg.table_name = p_table_name
--    and t.tableowner <> tg.grantee
  group by tg.table_schema, tg.table_name, tg.grantee, tg.is_grantable
  loop
    execute l_record.grantsql;
  end loop;

  exception
    when others then
    raise warning 'Could not grant privileges to %.%! (% %)', p_schema, p_part_name, sqlstate, sqlerrm;

  return;
end;
$function$
;
create or replace function @extschema@.f_set_part_name(
    p_table_name text,
    p_part_prefix text,
    p_part_suffix text
)
 returns text
 language plpgsql
as $function$
declare
    l_part_suffix text;
    l_new_table_name text;
    l_rundom_suffix text;
begin
    l_part_suffix := p_part_suffix;
    l_rundom_suffix := '_' || substr(md5(random()::text), 1, 4);

    -- Remove and/or replace unnecessary symbols.
    l_part_suffix := pg_catalog.regexp_replace(l_part_suffix, ' 00:00:00$', '');
    l_part_suffix := pg_catalog.regexp_replace(l_part_suffix, '[''-\.,\+\s:]', '_', 'ig');

    -- !!! Do not use this replacement due to the significant zeros
    ------ l_part_suffix := pg_catalog.regexp_replace(l_part_suffix, '0+$', '0');

    -- Concatenate new partition name
    l_new_table_name := trim(both '"' from p_table_name) || p_part_prefix || l_part_suffix || l_rundom_suffix;
    -- If new name is longer than allowed table name length, the random name is generated
    if (l_new_table_name <> l_new_table_name::name::text)
    then
        l_new_table_name := pg_catalog.regexp_replace(('npt_' || md5(random()::text) 
			    || gen_random_uuid()::text)::name::text, '[''-\.,\+\s:]', '_', 'ig');
    end if;
    -- Remove a trail underscore.
    l_new_table_name := pg_catalog.regexp_replace(l_new_table_name, '_$', '', 'ig');

    -- !!! Do not use this replacement due to the possible '-' sign in the value
    ------ l_new_table_name := pg_catalog.regexp_replace(l_new_table_name, '_+', '_', 'ig');

    return lower(l_new_table_name);
end;
$function$
;
create or replace function @extschema@.f_is_eligible(
    p_table_name text
)
 returns oid
 language plpgsql
as $function$
declare
    l_i integer;
    l_relkind text;
    l_table_oid oid;
    l_is_partition boolean;
    l_error_text text;
    l_warn_text text;
    l_namespace text;
    l_relname text;
begin
    if (p_table_name is null) then
        return null::oid;
    end if;
    -- Get table oid. Table name can be schema qualified or not.
    begin
        select
            pc.oid, pc.relnamespace::regnamespace, pc.relname, pc.relispartition, pc.relkind
        into strict
            l_table_oid, l_namespace, l_relname, l_is_partition, l_relkind
        from
            pg_class pc
        where
            pc.oid = array_to_string(array(select quote_ident(unnest(parse_ident(p_table_name, true)))), '.')::regclass::oid
        ;
    exception
        when no_data_found then
            raise warning 'Table % does not exist!', p_table_name;
            return null::oid;
        when too_many_rows then
            raise warning 'Table name % is not unique. Use schema to qualify a table!', p_table_name;
            return null::oid;
        when others then
            raise warning '%; sqlstate: %', sqlerrm, sqlstate;
            return null::oid;
    end;

    -- Validate whether the table is partitioned and fit the automated rules.

    -- Is it partitioned?
    if (l_relkind is null or l_relkind <> 'p') then
        l_table_oid := null;
        l_error_text := 'Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown') || ' is not partitioned.';
    end if;

    -- Check DEFAULT partitions
    if (l_table_oid is not null) then
        if (@extschema@.f_default_part_has_data(l_table_oid)) then
            l_warn_text := 'Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown') || ' has default partition with data.';
            raise warning '%', l_warn_text;
        end if;
    end if;

    -- Validate partitioning conditions
    if (l_table_oid is not null) then
        select
            count(*)
        into
            l_i
        from
            @extschema@.v_partitioned_tables vpt
        where
            vpt.oid = l_table_oid and
            vpt.inhparent is null and
            vpt.inhrelid is null
        ;
    end if;

    -- It is partitoned by more than one column
    if (l_table_oid is not null and l_i > 1) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' is partitioned by more than one column.';
    end if;

    -- It is not a high level partitioned table
    if (l_table_oid is not null and l_i = 0) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' is not a high level partitioned table.';
    end if;

    -- Validate partition strategy and columns types
    if (l_table_oid is not null) then
        select
            count(*)
        into
            l_i
        from
            @extschema@.v_partitioned_tables vpt
        where
            vpt.oid = l_table_oid and
            vpt.inhparent is null and
            vpt.inhrelid is null and
            (
                (
                    vpt.partition_strategy = 'range' and
                    vpt.part_col_data_type = ANY (@extschema@.f_get_allowed_types('generalrange'))
                )
                or
                (
                    vpt.partition_strategy = 'list' and
                    vpt.part_col_data_type = ANY (@extschema@.f_get_allowed_types('generallist'))
                )
            )
        ;
    end if;

    -- Wrong combination of partition strategy and columns types
    if (l_table_oid is not null and l_i = 0) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' has a not suitable combination of partition strategy and columns types.';
    end if;

    -- Raise warning and return null
    if (l_error_text is not null) then
        raise warning '%', l_error_text;
        return null::oid;
    end if;

    return l_table_oid;
end;
$function$
;
create or replace function @extschema@.f_get_minvalue_part_name(
    p_table_oid oid
)
 returns text
 language plpgsql
as $function$
declare
    l_t text;
begin
    -- Get partition table name with MINVALUE boundary
    select
        v1.part_name
    into
        l_t
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = p_table_oid and
        v1.part_boundary like 'FOR VALUES FROM (MINVALUE) TO%'
    ;

    return l_t;
end;
$function$
;
create or replace function @extschema@.f_grant_package_privileges(
    p_user text
)
 returns void
 language plpgsql
as $function$
begin

  execute 'grant usage on schema @extschema@ to ' || p_user;
  execute 'grant execute on all functions in schema @extschema@ to ' || p_user;
  execute 'grant select on all tables in schema @extschema@ to ' || p_user;

  exception
    when others then
    raise warning 'Could not grant privileges to %! (% %)', p_user, sqlstate, sqlerrm;

  return;
end;
$function$
;
create or replace function @extschema@.f_subp_is_eligible(
    p_table_name text,
    p_needs_subp boolean
)
 returns text
 language plpgsql
as $function$
declare
    l_relkind text;
    l_table_oid oid;
    l_is_partition boolean;
    l_error_text text;
    l_namespace text;
    l_relname text;
    l_ret text;
begin
    -- TODO: Add more checks

    if (p_table_name is null) then
	return null;
    end if;

    -- Get table oid. Table name can be schema qualified or not.
    begin
        select
            pc.oid, pc.relnamespace::regnamespace, pc.relname, pc.relispartition, pc.relkind
        into strict
            l_table_oid, l_namespace, l_relname, l_is_partition, l_relkind
        from
            pg_class pc
        where
            pc.oid = array_to_string(array(select quote_ident(unnest(parse_ident(p_table_name, true)))), '.')::regclass::oid
        ;
    exception
        when no_data_found then
            raise warning 'Table % does not exist!', p_table_name;
            return null;
        when too_many_rows then
            raise warning 'Table name % is not unique. Use schema to qualify a table!', p_table_name;
            return null;
        when others then
            raise warning '%; sqlstate: %', sqlerrm, sqlstate;
            return null;
    end;

    -- Is it partitioned?
    if ( (l_relkind is null or l_relkind <> 'p') and p_needs_subp) then
        l_error_text := 'Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown') || ' is not partitioned.';
    end if;

    if (l_error_text is not null) then
	raise warning '%', l_error_text;
	return null;
    else
	l_ret := l_namespace || '.' || l_relname;
    end if;

    return l_ret;
end;
$function$
;
create or replace function @extschema@.f_get_part_column(
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
create or replace function @extschema@.f_is_eligible_for_detach(
    p_table_name text
)
 returns oid
 language plpgsql
as $function$
declare
    l_i integer;
    l_relkind text;
    l_table_oid oid;
    l_is_partition boolean;
    l_error_text text;
    l_warn_text text;
    l_namespace text;
    l_relname text;
begin
    if (p_table_name is null) then
        return null::oid;
    end if;
    -- Get table oid. Table name can be schema qualified or not.
    begin
        select
            pc.oid, pc.relnamespace::regnamespace, pc.relname, pc.relispartition, pc.relkind
        into strict
            l_table_oid, l_namespace, l_relname, l_is_partition, l_relkind
        from
            pg_class pc
        where
            pc.oid = array_to_string(array(select quote_ident(unnest(parse_ident(p_table_name, true)))), '.')::regclass::oid
        ;
            --pc.oid = array_to_string(parse_ident(p_table_name, true),'.')::regclass
    exception
        when no_data_found then
            raise warning 'Table % does not exist!', p_table_name;
            return null::oid;
        when too_many_rows then
            raise warning 'Table name % is not unique. Use schema to qualify a table!', p_table_name;
            return null::oid;
        when others then
            raise warning '%; sqlstate: %', sqlerrm, sqlstate;
            return null::oid;
    end;

    -- Validate whether the table is partitioned and fit the automated rules.

    -- Is it partitioned?
    if (l_relkind is null or l_relkind <> 'p') then
        l_table_oid := null;
        l_error_text := 'Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown') || ' is not partitioned.';
    end if;

    -- Check DEFAULT partitions
    if (l_table_oid is not null) then
            if (@extschema@.f_default_part_has_data(l_table_oid)) then
                l_warn_text := 'Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown') || ' has default partition with data.';
                raise warning '%', l_warn_text;
            end if;
    end if;

    -- Validate partitioning conditions
    if (l_table_oid is not null) then
        select
            count(*)
        into
            l_i
        from
            @extschema@.v_partitioned_tables vpt
        where
            vpt.oid = l_table_oid and
            vpt.inhparent is null and
            vpt.inhrelid is null
        ;
    end if;

    -- It is partitoned by more than one column
    if (l_table_oid is not null and l_i > 1) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' is partitioned by more than one column.';
    end if;

    -- It is not a high level partitioned table
    if (l_table_oid is not null and l_i = 0) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' is not a high level partitioned table.';
    end if;

    -- Validate partition strategy and columns types
    if (l_table_oid is not null) then
        select
            count(*)
        into
            l_i
        from
            @extschema@.v_partitioned_tables vpt
        where
            vpt.oid = l_table_oid and
            vpt.inhparent is null and
            vpt.inhrelid is null and
            (
                (
                    vpt.partition_strategy = 'range' and
                    vpt.part_col_data_type = ANY (@extschema@.f_get_allowed_types('detachrange'))
                )
                or
                (
                    vpt.partition_strategy = 'list' and
                    vpt.part_col_data_type = ANY (@extschema@.f_get_allowed_types('detachlist'))
                )
            )
        ;
    end if;

    -- Wrong combination of partition strategy and columns types
    if (l_table_oid is not null and l_i = 0) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' has a not suitable combination of partition strategy and columns types.';
    end if;

    -- Raise error
    if (l_error_text is not null) then
        raise warning '%', l_error_text;
        return null::oid;
    end if;

    return l_table_oid;
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
begin
    -- Get the partition column name.
    l_col_name := @extschema@.f_get_part_column(p_table_oid);
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

    if (p_type2 is null) then
      return r_table_name;
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
      return r_table_name;
    end if;

end;
$function$
;
create or replace function @extschema@.f_set_config(
    p_name text,
    p_setting text,
    p_is_local boolean
)
 returns void
 language plpgsql
 security definer
as $function$
begin
  -- check available parameter names
  if (
	p_name not in (
		'session_replication_role'
	)
  ) then
    raise exception 'There is unavailable parameter to set %!', p_name;
  end if;

  -- check whether we set parameter for current transaction only
  if (not p_is_local) then
    raise exception 'It is allowed to set parameter % for current transaction only!', p_name;
  end if;

  perform set_config(p_name, p_setting, p_is_local);

  exception
    when others then
    raise warning 'Could not set parameter % to %! (% %)', p_name, p_setting, sqlstate, sqlerrm;

  return;
end;
$function$
;
create or replace function @extschema@.f_get_min_max_value_range(
    p_table_oid oid,
    p_value anyelement,
    p_minmax text
)
 returns record
 language plpgsql
as $function$
declare
    l_sql text;
    l_type text;
    l_record record;
begin
    l_type := pg_typeof(p_value);
    if (p_minmax = 'max') then
        l_sql := '
            with v1 as (
            select
		quote_ident(pt.relnamespace::regnamespace::text) || ''.'' || quote_ident(pt.relname) as relname,
                pg_get_expr(pt.relpartbound, pt.oid) as l,
		row_number() over (partition by base_tb.oid)
            from
                pg_class base_tb, pg_inherits i, pg_class pt
            where
                i.inhparent = base_tb.oid and
                pt.oid = i.inhrelid and
                base_tb.oid = ' || p_table_oid || '
            )
            select v1.relname as part_name,
                btrim( (regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' ))[2], '''''''' )::' || l_type || ' as rval
            from
                v1
            where
                v1.l not like ''%(MAXVALUE)%'' and
                v1.l not like ''%(MINVALUE)%''
            order by 2 desc nulls last
            limit 1
        ';
    elsif (p_minmax = 'min') then
        l_sql := '
            with v1 as (
            select
		quote_ident(pt.relnamespace::regnamespace::text) || ''.'' || quote_ident(pt.relname) as relname,
                pg_get_expr(pt.relpartbound, pt.oid) as l,
		row_number() over (partition by base_tb.oid)
            from
                pg_class base_tb, pg_inherits i, pg_class pt
            where
                i.inhparent = base_tb.oid and
                pt.oid = i.inhrelid and
                base_tb.oid = ' || p_table_oid || '
            )
            select v1.relname as part_name,
                btrim( (regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' ))[1], '''''''' )::' || l_type || ' as lval
            from
                v1
            where
                v1.l not like ''%(MAXVALUE)%'' and
                v1.l not like ''%(MINVALUE)%''
            order by 2 asc nulls last
            limit 1
        ';
    end if;
    execute l_sql into l_record;
    return l_record;
end;
$function$
;
create or replace function @extschema@.f_add_interval(
    p_value anyelement,
    p_interval text
)
 returns anyelement
 language plpgsql
as $function$
declare
    l_value p_value%TYPE;
    l_type text;
    l_sql text;
    l_error_text text;
    l_value_arr text[];
    l_interval_arr text[];
    l_i integer;
begin
    l_type := pg_typeof(p_value);
    if (
        l_type = ANY (@extschema@.f_get_allowed_types('intervaldatetime'))
    )
    then
        l_sql := 'select ($1 + ($2)::interval)::' || l_type;
        execute l_sql into l_value using p_value, p_interval;
    elsif (
        l_type = ANY (@extschema@.f_get_allowed_types('intervalnumeric'))
    )
    then
        l_sql := 'select ($1 + ($2)::' || l_type || ')::' || l_type;
        execute l_sql into l_value using p_value, p_interval;
    else
        l_error_text := 'Can not add interval. Type ' || l_type || ' is not allowed to use.';
        raise exception '%', l_error_text;
    end if;

    -- Round result according to the parameters precision if float point data type is used
    if ( l_type in ('float', 'double precision', 'decimal', 'numeric', 'real') )
    then
        select string_to_array(p_value::text, '.') into l_value_arr;
        select string_to_array(p_interval, '.') into l_interval_arr;
	select greatest( length(coalesce(l_value_arr[2], ''::text)), length(coalesce(l_interval_arr[2], ''::text)) ) into l_i;
	if (l_i is not null and l_i > 0)
	then
	   l_sql := 'select round(' || l_value::text || '::numeric, ' || l_i::text || '::integer)::' || l_type;
	   execute l_sql into l_value;
	end if;
    end if;
    return l_value;
end;
$function$
;
create or replace function @extschema@.f_get_maxvalue_part_name(
    p_table_oid oid
)
 returns text
 language plpgsql
as $function$
declare
    l_t text;
begin
    -- Get partition table name with MAXVALUE boundary
    select
        v1.part_name
    into
        l_t
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = p_table_oid and
        v1.part_boundary like '%) TO (MAXVALUE)'
    ;

    return l_t;
end;
$function$
;
create or replace function @extschema@.f_has_default_part(
    p_table_oid oid
)
 returns boolean
 language plpgsql
as $function$
declare
    l_b boolean;
    l_i integer;
begin
    l_b := true;
    select
        count(*)
    into
        l_i
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = p_table_oid and
        v1.part_boundary='DEFAULT'
    ;
    if (l_i = 0) then
        l_b := false;
    end if;
    return l_b;
end;
$function$
;
create or replace function @extschema@.f_default_part_has_data(
    p_table_oid oid
)
 returns boolean
 language plpgsql
as $function$
declare
    l_b boolean;
    l_i integer;
    l_t text;
    l_sql text;
begin
    l_b := false;

    -- Get default partition table name
    l_t := @extschema@.f_get_default_part_name(p_table_oid);

    -- Check whether the default partition contents the data
    if (l_t is not null) then
        l_sql := 'select count(*) from (select 1 from ' || l_t || ' limit 1) v1';
        execute l_sql into l_i;
        if (l_i > 0) then
            l_b := true;
        end if;
    end if;

    return l_b;
end;
$function$
;
create or replace function @extschema@.f_set_schema(
    p_part_name text,
    p_schemaname text
)
 returns void
 language plpgsql
as $function$
declare
  l_r record;
  l_sql text;
begin
  -- Set schema for all subpartitions
  l_sql := 'select part_name from @extschema@.v_pt_tree where main_table_name = ''' || p_part_name || '''';
  raise info 'SQL = %', l_sql;
  for l_r in execute l_sql
  loop
      execute 'alter table if exists ' || l_r.part_name || ' set schema ' || p_schemaname;
  end loop;
  -- Set schema for subpartition
  execute 'alter table if exists ' || p_part_name || ' set schema ' || p_schemaname;

  exception
    when others then
    raise exception 'Could not set schema % for partition %! (%, %)', p_schemaname, p_part_name, sqlstate, sqlerrm;

  return;
end;
$function$
;
create or replace function @extschema@.f_get_allowed_types(
    p_set_name text
)
 returns text[]
 language plpgsql
as $function$
declare
    l_array text[];
    l_error_text text;
    l_message_text text;
    l_context text;
begin
    if ( p_set_name = 'generalrange' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone',
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
                   ]);
    elsif ( p_set_name = 'generallist' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone',
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
                   ]);
    elsif ( p_set_name = 'detachrange' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone',
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
                   ]);
    elsif ( p_set_name = 'detachlist' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone',
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision',
                        'text',
                        'character',
                        'character varying'
                   ]);
    elsif ( p_set_name = 'intervaldatetime' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone'
                   ]);
    elsif ( p_set_name = 'intervalnumeric' ) then
        l_array := (ARRAY[
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
                   ]);
    else
        l_error_text := 'Unknown name of types set ' || p_set_name || '.';
        raise exception '%', l_error_text;
    end if;

    return l_array;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;
end;
$function$
;
create or replace function @extschema@.f_get_min_max_value_list(
    p_table_oid oid,
    p_value anyelement,
    p_minmax text
)
 returns record
 language plpgsql
as $function$
declare
    l_sql text;
    l_type text;
    l_record record;
begin
    l_type := pg_typeof(p_value);
    if (p_minmax = 'max') then
        l_sql := '
            with v1 as (
            select
		quote_ident(pt.relnamespace::regnamespace::text) || ''.'' || quote_ident(pt.relname) as relname,
                pg_get_expr(pt.relpartbound, pt.oid) as l,
		row_number() over (partition by base_tb.oid)
            from
                pg_class base_tb, pg_inherits i, pg_class pt
            where
                i.inhparent = base_tb.oid and
                pt.oid = i.inhrelid and
                base_tb.oid = ' || p_table_oid || '
            )
            select v1.relname,
                unnest(string_to_array(regexp_replace((regexp_match( v1.l, ''\((.*)\)'', ''i''))[1], '''''''', '''', ''ig'')::text, '', '')::' || l_type || '[]) as f1
            from
                v1
            order by f1 desc nulls last
            limit 1
        ';
    elsif (p_minmax = 'min') then
        l_sql := '
            with v1 as (
            select
		quote_ident(pt.relnamespace::regnamespace::text) || ''.'' || quote_ident(pt.relname) as relname,
                pg_get_expr(pt.relpartbound, pt.oid) as l,
		row_number() over (partition by base_tb.oid)
            from
                pg_class base_tb, pg_inherits i, pg_class pt
            where
                i.inhparent = base_tb.oid and
                pt.oid = i.inhrelid and
                base_tb.oid = ' || p_table_oid || '
            )
            select v1.relname,
                unnest(string_to_array(regexp_replace((regexp_match( v1.l, ''\((.*)\)'', ''i''))[1], '''''''', '''', ''ig'')::text, '', '')::' || l_type || '[]) as f1
            from
                v1
            order by f1 asc nulls last
            limit 1
        ';
    end if;
    execute l_sql into l_record;
    return l_record;
end;
$function$
;
create or replace function @extschema@.f_get_default_part_name(
    p_table_oid oid
)
 returns text
 language plpgsql
as $function$
declare
    l_t text;
begin
    -- Get default partition table name
    select
        v1.part_name
    into
        l_t
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = p_table_oid and
        v1.part_boundary='DEFAULT'
    ;

    return l_t;
end;
$function$
;
create or replace function @extschema@.f_get_range_bvalue(
    p_part_name text,
    p_left_right text
)
 returns text
 language plpgsql
as $function$
declare
    l_sql text;
    l_ret text;
begin
    if (p_left_right = 'left') then
            select
                btrim( (regexp_match( pt.part_boundary, '\((.*)\) to \((.*)\)', 'i' ))[1], '''' ) as lval
            into
                l_ret
            from
                @extschema@.v_pt_tree pt
            where
	        pt.part_name = p_part_name
            ;
    elsif (p_left_right = 'right') then
            select
                btrim( (regexp_match( pt.part_boundary, '\((.*)\) to \((.*)\)', 'i' ))[2], '''' ) as rval
            into
                l_ret
            from
                @extschema@.v_pt_tree pt
            where
	        pt.part_name = p_part_name
            ;
    end if;

    return l_ret;
end;
$function$
;
create or replace function @extschema@.f_get_overlap_by_range(
    p_table_oid oid,
    p_type text,
    p_min anyelement,
    p_max anyelement,
    p_filter_part text default null::text
)
 returns text
 language plpgsql
as $function$
declare
    l_sql text;
    l_ret text;
    l_max_part_name text;
    l_left_value text;
begin
    -- Check if "MINVALUE" partition is overlapped
    l_ret := @extschema@.f_find_part_by_value(p_table_oid, false, p_min::text);
    if (l_ret is not null) then
	if (p_filter_part is not null and l_ret = p_filter_part) then
	  null;
        else
	  return l_ret;
	end if;
    end if;

    -- Check if "MAXVALUE" partition is overlapped
    l_max_part_name := @extschema@.f_get_maxvalue_part_name(p_table_oid);
    if (l_max_part_name is not null) then
       l_left_value := @extschema@.f_get_range_bvalue(l_max_part_name, 'left');
       l_sql := 'select ''x'' where ''' || p_max || '''::' || p_type || ' > ''' || l_left_value || '''::' || p_type;
       execute l_sql into l_ret;
       if ( l_ret is not null ) then

	if (p_filter_part is not null and l_max_part_name = p_filter_part) then
	  null;
        else
          return l_max_part_name;
	end if;

       end if;
    end if;
    l_ret := null;

    -- Check if some of the rest partitions is overlapped
    if (p_filter_part is not null) then
	p_filter_part := ' and v1.relname <> ''' || p_filter_part || '''';
    end if;

    l_sql := '
    with v1 as (
    select
	quote_ident(pt.relnamespace::regnamespace::text) || ''.'' || quote_ident(pt.relname) as relname,
        pg_get_expr(pt.relpartbound, pt.oid) as l,
	row_number() over (partition by base_tb.oid)
    from
        pg_class base_tb, pg_inherits i, pg_class pt
    where
        i.inhparent = base_tb.oid and
        pt.oid = i.inhrelid and
        base_tb.oid = ' || p_table_oid || '
    ), v2 as (
    select
        v1.relname,
        btrim(( regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' )  )[1], '''''''')::' || p_type || ' as leftb,
        btrim(( regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' )  )[2], '''''''')::' || p_type || ' as rightb,
	''' || p_min || '''::' || p_type || ' as leftpoint,
	''' || p_max || '''::' || p_type || ' as rightpoint
    from
        v1
    where
        v1.l not like ''%(MAXVALUE)%'' and
        v1.l not like ''%(MINVALUE)%'' ' || coalesce(p_filter_part, ' ') || '
    )
    select
        relname
    from
        v2
    where
        leftpoint < rightb and rightpoint > leftb
    limit 1'
    ;
    execute l_sql into l_ret;
    return l_ret;
end;
$function$
;
create or replace function @extschema@.f_range_add_interval_partitions(
    p_table_name text,
    p_interval text,
    p_start_value anyelement,
    p_end_value anyelement,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_i integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_start p_end_value%TYPE;
    l_current_start p_end_value%TYPE;
    l_current_end p_end_value%TYPE;
    l_default_part_name text;
    l_defpartdata boolean;
    l_subpart_name text;
    l_temp_table_name text;
    l_last_part_name text;
    l_current_part_name text;
    l_sub_by text;
    current_srr_value text;
    l_message_text text;
    l_context text;
begin
    -- Parameters validation
    if (p_start_value >= p_end_value) then
        l_error_text := 'Adding failed! Parameter p_start_value (' || p_start_value::text || ') must be less than p_end_value (' || p_end_value::text || ')!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;
    if (p_interval ~ '-') then
        l_error_text := 'Adding failed! Parameter p_interval (' || p_interval::text || ') must not be negative!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
	  select regexp_replace(p_interval, '-', '', 'g') into p_interval;
          raise warning '%', l_error_text;
          return;
        end if;
    end if;
    -- Validate table
    l_table_oid := @extschema@.f_is_eligible(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise an error due to wrong strategy
    if (l_pstrategy <> 'range') then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' does not have the range partition strategy, it is a ' || l_pstrategy || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Raise an error due to wrong type
    if (l_type <> pg_typeof(p_end_value)::text) then
        l_error_text := 'Adding failed! Type defined in the parameter ' || pg_typeof(p_end_value)::text || ' mismatches column type ' || l_type || ' in the table ' || l_table_name || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check if there is only one partition for values FROM (MINVALUE) TO (MAXVALUE)
    select
        v1.part_name
    into
        l_t
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = l_table_oid and
        v1.part_boundary like '%) TO (MAXVALUE)' and
        v1.part_boundary like 'FOR VALUES FROM (MINVALUE) TO%';

    -- Raise an error due to MINMAX partition only
    if (l_t is not null) then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' has MINMAX partition only ' || l_t || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check overlap
    l_t := @extschema@.f_get_overlap_by_range(l_table_oid, l_type, p_start_value, p_end_value);
    if (l_t is not null) then
        l_error_text := 'Adding failed! New partition for table ' || l_table_name || ' would overlap partition ' || l_t || '!';
	if (p_raiseexception) then
          raise exception '%', l_error_text;
	else
          raise warning '%', l_error_text;
	  return;
	end if;
    end if;

    -- Get "last" partition name and the boundary interval value
    l_sql := 'select a, b from @extschema@.f_get_min_max_value_range($1, $2, ''max'') as (a text, b ' || l_type || ')';
    execute l_sql into l_last_part_name, l_start using l_table_oid, p_end_value;

    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
        l_last_part_name := p_subp_templ;
    end if;

    l_start := p_start_value;

    -- Get subpartition strategy
    l_sql := 'select pg_get_partkeydef($1::regclass)';
    execute l_sql into l_sub_by using l_last_part_name;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    l_temp_table_name := 'yktt' || substr(md5(random()::text), 0, 25);
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);

    -- We use DEFAULT partition
    if (p_keepdefault) then
      -- If default partition exists and contains data we have to detach it and rename to safely add new partition
      if (l_default_part_name is not null and l_defpartdata) then
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
      end if;
    -- We do not need DEFAULT partition
    else
      if (l_defpartdata) then
          raise warning 'Default partition % contains the data, so it should not be dropped!', l_default_part_name;
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
          p_keepdefault := true;
      else
          if (l_default_part_name is not null) then
            execute 'drop table if exists ' || l_default_part_name;
          end if;
      end if;
    end if;

    -- Create RANGE partitions
    l_current_start := l_start;
    l_current_end := @extschema@.f_add_interval(l_current_start, p_interval);
    while l_current_end <= p_end_value
    loop
    
        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', l_current_start::text);
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
		 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;
    
        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;
    
        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || l_current_start::text || ''') to (''' || l_current_end::text || ''') ';
        execute l_sql;
    
        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);
    
        l_current_start := l_current_end;
        l_current_end := @extschema@.f_add_interval(l_current_end, p_interval);

    end loop;

    -- Create default partition if it is necessary and copy data from old default partition
    if (p_keepdefault) then
      -- We did not have default partition and have to create a new one
      if (not l_defpartdata and l_default_part_name is null) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      -- We detached and renamed default partition earlier
      elsif (l_defpartdata) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_table_name;
        execute 'drop table if exists ' || l_schema || '.' || l_temp_table_name;
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      end if;
    end if;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
create or replace function @extschema@.f_list_add_partitions(
    p_table_name text,
    p_values jsonb,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_i integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_default_part_name text;
    l_defpartdata boolean;
    l_temp_table_name text;
    l_last_part_name text;
    l_current_part_name text;
    l_subpart_name text;
    l_sub_by text;
    l_arr text[];
    l_bound_arr text[];
    l_record record;
    current_srr_value text;
    l_message_text text;
    l_context text;
begin
    -- Validate table
    l_table_oid := @extschema@.f_is_eligible_for_detach(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise error due to wrong strategy
    if (l_pstrategy <> 'list') then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' does not have the list partition strategy, it is a ' || l_pstrategy || '.';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Get the array of existing partition values to check the overlapping later on
    select array(
    select unnest(
             string_to_array(regexp_replace(part_boundary, 'FOR VALUES IN \(''?|''?\)|''?', '', 'g'), ', ')
    )
    from @extschema@.v_pt_tree
    where main_table_oid = l_table_oid
    ) into l_bound_arr;

    -- Get partition name with subpartitions and the boundary list value
    select part_name, pg_get_partkeydef(part_oid) as partdef
    into l_last_part_name, l_sub_by
    from @extschema@.v_pt_tree
    where main_table_oid = l_table_oid and pg_get_partkeydef(part_oid) is not null
    limit 1;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;

    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
        -- Get subpartition strategy
        l_sql := 'select pg_get_partkeydef($1::regclass)';
        execute l_sql into l_sub_by using p_subp_templ;
        if (l_sub_by is not null) then
            l_last_part_name := p_subp_templ;
            l_sub_by := 'PARTITION BY ' || l_sub_by;
        end if;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    l_temp_table_name := 'yktt' || substr(md5(random()::text), 0, 25);
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);

    -- We use DEFAULT partition
    if (p_keepdefault) then
      -- If default partition exists and contains data we have to detach it and rename to safely add new partition
      if (l_default_part_name is not null and l_defpartdata) then
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
      end if;
    -- We do not need DEFAULT partition
    else
      if (l_defpartdata) then
	  raise warning 'Default partition % contains the data, so it can not be dropped!', l_default_part_name;
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
          p_keepdefault := true;
      else
	  if (l_default_part_name is not null) then
	    execute 'drop table if exists ' || l_default_part_name;
	  end if;
      end if;
    end if;

    -- Parse the JSONB array to create the corresponding partitions
    for l_record in
    select
            f1,
            jsonb_typeof(f1) as jtype
    from
            jsonb_array_elements(
    		p_values
            ) eee(f1)
    loop
    
      l_arr := null;
      l_current_part_name := null;
    
      if (l_record.jtype = 'array') then
        l_arr := array(select jsonb_array_elements_text(l_record.f1));
        -- Escape an apostrophe if exists.
        l_arr := array(select regexp_replace(t.val, '''', '''''', 'g') from unnest(l_arr) as t(val));
        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', l_arr[1]); 
    
      -- If json array element can not be used as a value for column.
      elsif (l_record.jtype = 'object' or l_record.jtype = 'null' or l_record.jtype = 'boolean') then
        raise warning 'Partition of table % has not been added for value % due to this is %!', l_table_name, l_record.f1::text, l_record.jtype;
    
      else
        l_arr[1] := l_record.f1#>>'{}';
        -- Escape an apostrophe if exists.
        l_arr := array(select regexp_replace(t.val, '''', '''''', 'g') from unnest(l_arr) as t(val));
        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', l_arr[1]); 
    
      end if;

      -- Do we have a partition to create
      if (l_current_part_name is not null) then

      select count(*) into l_i from pg_class where relname = l_current_part_name and relnamespace = l_schema::regnamespace::oid;
      -- Create the partition if such a table does not exist only
      if (l_i = 0) then

      -- If the partition we are going to create overlaps with existing one
      select array(select regexp_replace(unn::text, '\.?00+$', '') from (select unnest(l_bound_arr) as unn) v1) into l_bound_arr;
      select array(select regexp_replace(unn::text, '\.?00+$', '') from (select unnest(l_arr) as unn) v1) into l_arr;
      if (l_bound_arr && l_arr) then
	raise warning 'Partition of table % has not been added for the list of values starts from % due to it overlaps the existing partition!', l_table_name, l_arr[1];
      else
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;

        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text); 
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name || 
    	         ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;
        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values in (''' || array_to_string(l_arr, ''', ''') || ''')';
        execute l_sql;
        l_bound_arr := array_cat(l_bound_arr, l_arr); 
        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);
      end if;

      else
	raise warning 'The table %.% already exists! New partition has not been created!', l_schema, l_current_part_name;
      end if;

      end if;
    end loop;

    -- Create default partition if it is necessary and copy data from old default partition
    if (p_keepdefault) then
      -- We did not have default partition and create new one
      if (not l_defpartdata and l_default_part_name is null) then
	l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' || 
	        l_table_name || ' default';
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      -- We detached and renamed default partition earlier
      elsif (l_defpartdata) then
	l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
	        l_table_name || ' default';
        execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_table_name;
        execute 'drop table if exists ' || l_schema || '.' || l_temp_table_name;
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      end if;
    end if;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
create or replace function @extschema@.f_range_add_interval_partitions(
    p_table_name text,
    p_interval text,
    p_end_value anyelement,
    p_keepdefault boolean,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_i integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_start p_end_value%TYPE;
    l_current_start p_end_value%TYPE;
    l_current_end p_end_value%TYPE;
    l_default_part_name text;
    l_defpartdata boolean;
    l_subpart_name text;
    l_temp_table_name text;
    l_temp_sub_table_name text;
    l_last_part_name text;
    l_current_part_name text;
    l_sub_by text;
    l_min_part text;
    l_min_value p_end_value%TYPE;
    l_max_part text;
    l_max_value p_end_value%TYPE;
    l_temp_mm_table_name text;
    current_srr_value text;
    l_message_text text;
    l_context text;
begin
    -- Validate table
    l_table_oid := @extschema@.f_is_eligible(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise an error due to wrong strategy
    if (l_pstrategy <> 'range') then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' does not have the range partition strategy, it is a ' || l_pstrategy || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Raise an error due to wrong type
    if (l_type <> pg_typeof(p_end_value)::text) then
        l_error_text := 'Adding failed! Type defined in the parameter ' || pg_typeof(p_end_value)::text || ' mismatches column type ' || l_type || ' in the table ' || l_table_name || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check if there is only one partition for values FROM (MINVALUE) TO (MAXVALUE)
    select
        v1.part_name
    into
        l_t
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = l_table_oid and
        v1.part_boundary like '%) TO (MAXVALUE)' and
        v1.part_boundary like 'FOR VALUES FROM (MINVALUE) TO%';

    -- Raise an error due to MINMAX partition only
    if (l_t is not null) then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' has MINMAX partition only ' || l_t || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Get "last" partition name and the boundary interval value
    if (p_interval ~ '-') then
        l_sql := 'select a, b from @extschema@.f_get_min_max_value_range($1, $2, ''min'') as (a text, b ' || l_type || ')';
    else
        l_sql := 'select a, b from @extschema@.f_get_min_max_value_range($1, $2, ''max'') as (a text, b ' || l_type || ')';
    end if;
    execute l_sql into l_last_part_name, l_start using l_table_oid, p_end_value;

    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
        l_last_part_name := p_subp_templ;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    -- Handle the case if there are MINVALUE or MAXVALUE partitions
    l_max_part := @extschema@.f_get_maxvalue_part_name(l_table_oid);
    l_max_value := @extschema@.f_get_range_bvalue(l_max_part, 'left');
    l_min_part := @extschema@.f_get_minvalue_part_name(l_table_oid);
    l_min_value := @extschema@.f_get_range_bvalue(l_min_part, 'right');
    if (p_interval ~ '-' and l_min_part is not null) then
	l_start := l_min_value;
        l_temp_mm_table_name := 'ykmm' || substr(md5(random()::text), 0, 25);
        execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_min_part;
        execute 'alter table if exists ' || l_min_part || ' rename to ' || l_temp_mm_table_name;
	if (l_last_part_name = l_min_part) then
		l_last_part_name := l_schema || '.' || l_temp_mm_table_name;
	end if;
    elsif (l_max_part is not null and p_interval !~ '-') then
	l_start := l_max_value;
        l_temp_mm_table_name := 'ykmm' || substr(md5(random()::text), 0, 25);
        execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_max_part;
        execute 'alter table if exists ' || l_max_part || ' rename to ' || l_temp_mm_table_name;
	if (l_last_part_name = l_max_part) then
		l_last_part_name := l_schema || '.' || l_temp_mm_table_name;
	end if;
    elsif (l_start is null) then
        -- Raise error due to there are no any partitions
        l_error_text := 'Adding failed! Table ' || l_table_name || ' does not have any partitions. Create at least one.';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Get subpartition strategy
    l_sql := 'select pg_get_partkeydef($1::regclass)';
    execute l_sql into l_sub_by using l_last_part_name;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;

    l_temp_table_name := 'yktt' || substr(md5(random()::text), 0, 25);
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);

    -- We use DEFAULT partition
    if (p_keepdefault) then
      -- If default partition exists and contains data we have to detach it and rename to safely add new partition
      if (l_default_part_name is not null and l_defpartdata) then
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
      end if;
    -- We do not need DEFAULT partition
    else
      if (l_defpartdata) then
          raise warning 'Default partition % contains the data, so it should not be dropped!', l_default_part_name;
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
          p_keepdefault := true;
      else
          if (l_default_part_name is not null) then
            execute 'drop table if exists ' || l_default_part_name;
          end if;
      end if;
    end if;

    -- Create RANGE partitions
    if (p_interval ~ '-') then
        l_current_end := l_start;
        l_current_start := @extschema@.f_add_interval(l_current_end, p_interval);

	while l_current_start >= p_end_value
        loop

        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', l_current_start::text);
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;

        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_temp_sub_table_name := 'yksubtemp' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || l_current_start::text || ''') to (''' || l_current_end::text || ''') ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

        l_current_end := l_current_start;
        l_current_start := @extschema@.f_add_interval(l_current_start, p_interval);
        end loop;

    else

        l_current_start := l_start;
        l_current_end := @extschema@.f_add_interval(l_current_start, p_interval);
        while l_current_end <= p_end_value
        loop

        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', l_current_start::text);
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;

        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_temp_sub_table_name := 'yksubtemp' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || l_current_start::text || ''') to (''' || l_current_end::text || ''') ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

        l_current_start := l_current_end;
        l_current_end := @extschema@.f_add_interval(l_current_end, p_interval);
        end loop;
    end if;

    -- Handle the case if there are MINVALUE or MAXVALUE partitions
    -- Create MINMAXvalue partition if necessary 
    if (p_interval ~ '-' and p_keepminmaxvalue) then

        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'minvalue'::text);
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;

        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_temp_sub_table_name := 'yksubtemp' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (MINVALUE) to (''' || l_current_end::text || ''') ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    elsif (p_keepminmaxvalue) then

        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'maxvalue'::text);
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;

        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_temp_sub_table_name := 'yksubtemp' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || l_current_start::text || ''') to (MAXVALUE) ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    end if;

    -- Create default partition if it is necessary and copy data from old default partition
    if (p_keepdefault) then
      -- We did not have default partition and have to create a new one
      if (not l_defpartdata and l_default_part_name is null) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      -- We detached and renamed default partition earlier
      elsif (l_defpartdata) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_table_name;
        execute 'drop table if exists ' || l_schema || '.' || l_temp_table_name;
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      end if;
    end if;

    -- Insert the data from detached minmax partition if such a partiton exists
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    if (l_temp_mm_table_name is not null) then
      -- Create default partition to be sure we do not lose the data from MINMAX partition
      if (l_default_part_name is null) then
        l_t := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_t || ' partition of ' || l_table_name || ' default';
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_t);
      end if;
      execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_mm_table_name;
      execute 'drop table if exists ' || l_schema || '.' || l_temp_mm_table_name;
      -- Raise exception if shouldn't have to be default partition but it has a data
      l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);
      if (l_default_part_name is null and l_defpartdata) then
        if (p_raiseexception) then
          raise exception 'New partitions for table % do not cover all the data from MINMAX partition!', l_table_name;
        else
          raise warning 'New partitions for table % do not cover all the data from MINMAX partition! DEFAULT partition % has been created!', l_table_name, l_t;
        end if;
      elsif (l_default_part_name is null and not l_defpartdata) then
        execute 'drop table if exists ' || l_schema || '.' || l_t;
      end if;
    end if;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
create or replace function @extschema@.f_detach_part(
    p_table_name text,
    p_value text,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean,
    p_retentionschema text default null::text
)
 returns text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_part_name text;
    l_default_part_name text;
    l_defpartdata boolean;
    l_table_oid oid;
    l_min_part text;
    l_max_part text;
    l_error_text text;
    l_message_text text;
    l_context text;
begin
    -- Validate table
    l_table_oid := @extschema@.f_is_eligible_for_detach(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return null;
        end if;
    end if;
    select
	quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn
    into
        l_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition name
    l_part_name := @extschema@.f_find_part_by_value(l_table_oid, true, p_value);

    -- Get default partition name if exists
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);

    -- Get MINVALUE or MAXVALUE partitions
    l_max_part := @extschema@.f_get_maxvalue_part_name(l_table_oid);
    l_min_part := @extschema@.f_get_minvalue_part_name(l_table_oid);

    -- Raise error if there is no suitable partition
    if (l_part_name is null) then
        l_error_text := 'Detach failed! The table ' || l_table_name || ' does not have a suitable partition to detach!';
    elsif (l_default_part_name = l_part_name and l_defpartdata) then
        l_error_text := 'Detach failed! We can not detach the default partition ' || l_part_name || ' with data!';
    elsif ( (l_part_name = coalesce(l_max_part, '') or l_part_name = coalesce(l_min_part, '')) and p_keepminmaxvalue ) then
        l_error_text := 'Detach failed! You wanted not to detach the MINVALUE or MAXVALUE partition ' || l_part_name;
    end if;

    if (l_error_text is null) then
        -- Detach partition
        execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_part_name;
        if (p_retentionschema is not null) then
            perform @extschema@.f_set_schema(l_part_name, p_retentionschema);
        end if;
    else
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
	  return null;
        end if;
    end if;

    return l_part_name;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
create or replace function @extschema@.f_range_merge_partitions(
    p_table_name text,
    p_part_arr text[],
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_i integer;
    l_a integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_subpart_name text;
    l_temp_merge_table_name text;
    l_last_part_name text;
    l_current_part_name text;
    l_sub_by text;
    current_srr_value text;
    l_left_final_boundary text;
    l_right_final_boundary text;
    l_bool boolean;
    l_message_text text;
    l_context text;
begin
    -- Parameters validation
    if (array_length(p_part_arr, 1) < 2) then
        l_error_text := 'Merge failed! The number of partitions to merge must be greater than or equal to 2';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Validate table
    l_table_oid := @extschema@.f_is_eligible(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise an error due to wrong strategy
    if (l_pstrategy <> 'range') then
        l_error_text := 'Merge failed! Table ' || l_table_name || ' does not have the range partition strategy, it is a ' || l_pstrategy || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Calculate final boundaries
    for l_a in 1..array_length(p_part_arr, 1)
    loop
	-- Check the partition name correctness
	l_t := p_part_arr[l_a];
	p_part_arr[l_a] := @extschema@.f_subp_is_eligible(p_part_arr[l_a], false);
	if (p_part_arr[l_a] is null) then
          l_error_text := 'Merge failed! Table ' || coalesce(l_t, '') || ' is not found!';
	  if (p_raiseexception) then
             raise exception '%', l_error_text;
	  else
             raise warning '%', l_error_text;
	     return;
	  end if;
	end if;
	-- set left boundary
        l_t := @extschema@.f_get_range_bvalue(p_part_arr[l_a], 'left');
	if (l_left_final_boundary is not null and l_left_final_boundary = 'MINVALUE')
	then
          null;
	elsif (l_left_final_boundary is null or l_t = 'MINVALUE')
	then
          l_left_final_boundary := l_t;
        else
	  execute 'select case when ''' || l_t || '''::' || l_type || ' <= ''' || l_left_final_boundary || '''::' || l_type || ' then true else false end' into l_bool;
	  if (l_bool)
	  then
	    l_left_final_boundary := l_t;
	  end if;
	end if;
	-- set right boundary
        l_t := @extschema@.f_get_range_bvalue(p_part_arr[l_a], 'right');
	if (l_right_final_boundary is not null and l_right_final_boundary = 'MAXVALUE')
	then
          null;
	elsif (l_right_final_boundary is null or l_t = 'MAXVALUE')
	then
          l_right_final_boundary := l_t;
        else
	  execute 'select case when ''' || l_t || '''::' || l_type || ' >= ''' || l_right_final_boundary || '''::' || l_type || ' then true else false end' into l_bool;
	  if (l_bool)
	  then
	    l_right_final_boundary := l_t;
	  end if;
	end if;
    end loop;

    -- Get "last" partition name
    l_sql := 'select a from @extschema@.f_get_min_max_value_range($1, null::' || l_type || ', ''max'') as (a text, b ' || l_type || ')';
    execute l_sql into l_last_part_name using l_table_oid;

    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
	l_last_part_name := p_subp_templ;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    -- Detach partitions we are going to merge
    for l_a in 1..array_length(p_part_arr, 1)
    loop
      l_temp_merge_table_name := 'ykmerge' || substr(md5(random()::text), 0, 25);
      execute 'alter table if exists ' || l_table_name || ' detach partition ' || p_part_arr[l_a];
      execute 'alter table if exists ' || p_part_arr[l_a] || ' rename to ' || l_temp_merge_table_name;
      if (l_last_part_name = p_part_arr[l_a]) then
	l_last_part_name := l_schema || '.' || l_temp_merge_table_name;
      end if;
      p_part_arr[l_a] := l_schema || '.' || l_temp_merge_table_name;
    end loop;

    -- Get subpartition strategy
    l_sql := 'select pg_get_partkeydef($1::regclass)';
    execute l_sql into l_sub_by using l_last_part_name;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;


    -- Create merged RANGE partition
    l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', l_left_final_boundary);
    -- Create table to attach as a partition later on
    if current_setting('server_version_num')::integer >= 120000::integer then
     l_t := ' including generated ';
    else
     l_t := null;
    end if;
    l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
             ' including defaults including constraints including storage including indexes' ||
             coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
    execute l_sql;

    -- Create the subpartitions if any
    l_i := 0;
    for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
    loop
        l_i := l_i + 1;
        l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
        l_temp_merge_table_name := 'ykmerge' || substr(md5(random()::text), 0, 25);
        execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_merge_table_name;
        l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
             ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
        execute l_sql;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
    end loop;
    -- Attach the table as a partition to the main table
    if (l_left_final_boundary = 'MINVALUE') then
	l_left_final_boundary := '(MINVALUE)';
    else
	l_left_final_boundary := '(''' || l_left_final_boundary || ''')';
    end if;
    if (l_right_final_boundary = 'MAXVALUE') then
	l_right_final_boundary := '(MAXVALUE)';
    else
	l_right_final_boundary := '(''' || l_right_final_boundary || ''')';
    end if;

    l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
             ' for values from ' || l_left_final_boundary || ' to ' || l_right_final_boundary;
    execute l_sql;
    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    -- Insert the data from detached partition
    for l_a in 1..array_length(p_part_arr, 1)
    loop
      execute 'insert into ' || l_table_name || ' overriding system value select * from ' || p_part_arr[l_a];
      execute 'drop table if exists ' || p_part_arr[l_a] || ' cascade';
    end loop;
    
    return next l_schema || '.' || l_current_part_name;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;
end;
$function$
;
create or replace function @extschema@.f_do_retention(
    p_table_name text,
    p_direction text, -- Either "old" or "new"
    p_action text,    -- Either "drop" or "detach"
    p_value anyelement,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean,
    p_retentionschema text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_schema text;
    l_max_part text;
    l_min_part text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_cond text;
    l_t text;
    l_r record;
    l_b boolean;
    l_start text;
    l_message_text text;
    l_context text;
begin
    -- Parameters validation
    if (p_direction is null or p_direction not in ('old', 'new')) then
        if (p_raiseexception) then
          raise exception 'Parameter p_direction accepts values either "old" or "new" only!';
        else
          raise warning 'Parameter p_direction accepts values either "old" or "new" only!';
          return;
        end if;
    end if;
    if (p_action is null or p_action not in ('drop', 'detach')) then
        if (p_raiseexception) then
          raise exception 'Parameter p_action accepts values either "drop" or "detach" only!';
        else
          raise warning 'Parameter p_action accepts values either "drop" or "detach" only!';
          return;
        end if;
    end if;
    if (p_retentionschema is not null) then
        l_sql := 'select exists(select 1 from pg_namespace where nspname=''' || p_retentionschema || '''::name)';
	execute l_sql into l_b;
	if (not l_b) then
          if (p_raiseexception) then
            raise exception 'Schema % does not exists!', p_retentionschema;
          else
            raise warning 'Schema % does not exists!', p_retentionschema;
            return;
          end if;
	end if;
    end if;
    -- Validate table
    l_table_oid := @extschema@.f_is_eligible(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise an error due to wrong strategy
    if (l_pstrategy <> 'range') then
        l_error_text := 'Retention failed! Table ' || l_table_name || ' does not have the range partition strategy, it is a ' || l_pstrategy || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Raise an error due to wrong type
    if (l_type <> pg_typeof(p_value)::text) then
        l_error_text := 'Retention failed! Type defined in the parameter ' || pg_typeof(p_value)::text || ' mismatches column type ' || l_type || ' in the table ' || l_table_name || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check if there is only one partition for values FROM (MINVALUE) TO (MAXVALUE)
    select
        v1.part_name
    into
        l_t
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = l_table_oid and
        v1.part_boundary like '%) TO (MAXVALUE)' and
        v1.part_boundary like 'FOR VALUES FROM (MINVALUE) TO%';

    -- Raise an error due to MINMAX partition only
    if (l_t is not null) then
        l_error_text := 'Retention failed! Table ' || l_table_name || ' has MINMAX partition only ' || l_t || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Get list of partitions to do retention
    l_start := p_value::text;
    if (p_direction = 'old') then
	l_cond := 'leftb <= retentionpoint';
    elsif (p_direction = 'new') then
	l_cond := 'rightb > retentionpoint';
    end if;
    l_sql := '
    with v1 as (
    select
        quote_ident(pt.relnamespace::regnamespace::text) || ''.'' || quote_ident(pt.relname) as relname,
        pg_get_expr(pt.relpartbound, pt.oid) as l,
        row_number() over (partition by base_tb.oid)
    from
        pg_class base_tb, pg_inherits i, pg_class pt
    where
        i.inhparent = base_tb.oid and
        pt.oid = i.inhrelid and
        base_tb.oid = ' || l_table_oid || '
    ), v2 as (
    select
        v1.relname,
        btrim(( regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' )  )[1], '''''''')::' || l_type || ' as leftb,
        btrim(( regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' )  )[2], '''''''')::' || l_type || ' as rightb,
        ''' || l_start || '''::' || l_type || ' as retentionpoint
    from
        v1
    where
        v1.l not like ''%(MAXVALUE)%'' and
        v1.l not like ''%(MINVALUE)%''
    )
    select
        relname
    from
        v2
    where
        ' || l_cond || '
    '
    ;

    for l_r in execute l_sql
    loop
	if (p_action = 'detach') then
            -- Detach partition
            execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_r.relname;
	    if (p_retentionschema is not null) then
		perform @extschema@.f_set_schema(l_r.relname, p_retentionschema);
	    end if;
	elsif (p_action = 'drop') then
            -- Drop partition
            execute 'drop table if exists ' || l_r.relname || ' cascade';
	end if;
	return next l_r.relname;
    end loop;

    -- Get MINVALUE or MAXVALUE partitions
    l_max_part := @extschema@.f_get_maxvalue_part_name(l_table_oid);
    l_min_part := @extschema@.f_get_minvalue_part_name(l_table_oid);

    if (not p_keepminmaxvalue) then
	if (p_action = 'detach') then
            -- Detach partition
            if (p_direction = 'old' and l_min_part is not null) then
                execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_min_part;
	        if (p_retentionschema is not null) then
		    perform @extschema@.f_set_schema(l_min_part, p_retentionschema);
	        end if;
		return next l_min_part;
            elsif (p_direction = 'new' and l_max_part is not null) then
                execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_max_part;
	        if (p_retentionschema is not null) then
		    perform @extschema@.f_set_schema(l_max_part, p_retentionschema);
	        end if;
		return next l_max_part;
            end if;
	elsif (p_action = 'drop') then
            -- Drop partition
            if (p_direction = 'old' and l_min_part is not null) then
                execute 'drop table if exists ' || l_min_part || ' cascade';
		return next l_min_part;
            elsif (p_direction = 'new' and l_max_part is not null) then
                execute 'drop table if exists ' || l_max_part || ' cascade';
		return next l_max_part;
	    end if;
	end if;
    end if;

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
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

    -- Check the behavior with default partition
    if (not p_count_defpart and l_table_name is not null) then
	select v1.part_boundary into l_t from @extschema@.v_pt_tree v1 where v1.part_name = l_table_name;
	if (l_t = 'DEFAULT') then
		return null;
	end if;
    end if;

    if (p_value2 is null) then
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

    -- Check the behavior with default partition
    if (not p_count_defpart and l_table_name is not null) then
	select v1.part_boundary into l_t from @extschema@.v_pt_tree v1 where v1.part_name = l_table_name;
	if (l_t = 'DEFAULT') then
		return null;
	end if;
    end if;

    if (p_value2 is null) then
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

create or replace function @extschema@.f_list_split_partition(
    p_part_name text,
    p_values jsonb,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_a integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_temp_split_table_name text;
    l_last_part_name text;
    l_sub_by text;
    l_arr text[];
    l_bound_arr text[];
    current_srr_value text;
    l_record record;
    l_message_text text;
    l_context text;
begin
    -- Parameters validation
    p_part_name := @extschema@.f_subp_is_eligible(p_part_name, false);
    if (p_part_name is null) then
        if (p_raiseexception) then
          raise exception 'Could not validate partition name %', coalesce(p_part_name, 'unknown');
        else
          raise warning 'Could not validate partition name %', coalesce(p_part_name, 'unknown');
          return;
        end if;
    end if;

    select main_table_name into l_table_name from @extschema@.v_pt_tree where part_name = p_part_name;

    -- Validate table
    l_table_oid := @extschema@.f_is_eligible_for_detach(l_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(l_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(l_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise error due to wrong strategy
    if (l_pstrategy <> 'list') then
        l_error_text := 'Split failed! Table ' || l_table_name || ' does not have the list partition strategy, it is a ' || l_pstrategy || '.';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Get the array of splitted partition values
    select array(
    select unnest(
             string_to_array(regexp_replace(part_boundary, 'FOR VALUES IN \(''?|''?\)|''?', '', 'g'), ', ')
    )
    from @extschema@.v_pt_tree
    where main_table_oid = l_table_oid  and part_name = p_part_name
    )
    into l_bound_arr
    ;
    select array(select regexp_replace(unn::text, '\.?00+$', '') from (select unnest(l_bound_arr) as unn) v1) into l_bound_arr;

    -- Get the array of the target partitions values
    select array(
    select unnest(jbarr)
    from
    (
    select
      case
        when jtype = 'array' then array(select jsonb_array_elements_text(f1))
        when jtype = 'string' or jtype = 'number' then array[f1#>>'{}']
      end as jbarr
    from
    (
    select
                f1,
                jsonb_typeof(f1) as jtype
        from
                jsonb_array_elements(
                    p_values
                ) eee(f1)
    ) v1
    ) v2
    where jbarr is not null
    )
    into l_arr
    ;
    select array(select regexp_replace(unn::text, '\.?00+$', '') from (select unnest(l_arr) as unn) v1) into l_arr;

SELECT array_agg(x order by x) into l_arr FROM unnest(l_arr) x;
SELECT array_agg(x order by x) into l_bound_arr FROM unnest(l_bound_arr) x;

    if (l_bound_arr::text[] <> l_arr::text[]) then
        l_error_text := 'Split list partition failed. The target and source sets of values mismatch!';
        if (p_raiseexception) then
          raise exception '%, %, %', l_error_text, l_bound_arr::text[], l_arr::text[];
        else
          raise warning '%, %, %', l_error_text, l_bound_arr::text[], l_arr::text[];
          return;
        end if;
    end if;

    -- Get subpartition strategy
    -- Get partition name with subpartitions and the boundary list value
    select part_name, pg_get_partkeydef(part_oid) as partdef
    into l_last_part_name, l_sub_by
    from @extschema@.v_pt_tree
    where main_table_oid = l_table_oid and pg_get_partkeydef(part_oid) is not null
    limit 1;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;
    -- Check if subpartition template can be used
    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
        l_sql := 'select pg_get_partkeydef($1::regclass)';
        execute l_sql into l_sub_by using p_subp_templ;
        if (l_sub_by is not null) then
            l_last_part_name := p_subp_templ;
            l_sub_by := 'PARTITION BY ' || l_sub_by;
        end if;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    -- Detach partition we are going to split
    l_temp_split_table_name := 'yksplit' || substr(md5(random()::text), 0, 25);
    execute 'alter table if exists ' || l_table_name || ' detach partition ' || p_part_name;
    execute 'alter table if exists ' || p_part_name || ' rename to ' || l_temp_split_table_name;
    if (l_last_part_name = p_part_name) then
      l_last_part_name := l_schema || '.' || l_temp_split_table_name;
    end if;

    -- Create new partitions
    for l_record in select * from @extschema@.f_list_add_partitions(l_table_name, p_values, p_keepdefault, p_raiseexception, p_subp_templ) flap(f1)
    loop
	return next l_record.f1;
    end loop;

    -- Insert the data from detached partition
    execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_split_table_name;
    execute 'drop table if exists ' || l_schema || '.' || l_temp_split_table_name;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;
end;
$function$
;
create or replace function @extschema@.f_drop_part(
    p_table_name text,
    p_value text,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean
)
 returns text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_part_name text;
    l_default_part_name text;
    l_defpartdata boolean;
    l_table_oid oid;
    l_min_part text;
    l_max_part text;
    l_error_text text;
    l_message_text text;
    l_context text;
begin
    -- Validate table
    l_table_oid := @extschema@.f_is_eligible_for_detach(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return null;
        end if;
    end if;
    select
	quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn
    into
        l_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition name
    l_part_name := @extschema@.f_find_part_by_value(l_table_oid, true, p_value);

    -- Get default partition name if exists
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);

    -- Get MINVALUE or MAXVALUE partitions
    l_max_part := @extschema@.f_get_maxvalue_part_name(l_table_oid);
    l_min_part := @extschema@.f_get_minvalue_part_name(l_table_oid);

    -- Raise error if there is no suitable partition
    if (l_part_name is null) then
        l_error_text := 'Drop failed! The table ' || l_table_name || ' does not have a suitable partition to drop!';
    elsif (l_default_part_name = l_part_name and l_defpartdata) then
        l_error_text := 'Drop failed! We can not drop the default partition ' || l_part_name || ' with data!';
    elsif ( (l_part_name = coalesce(l_max_part, '') or l_part_name = coalesce(l_min_part, '')) and p_keepminmaxvalue ) then
        l_error_text := 'Drop failed! You wanted not to drop the MINVALUE or MAXVALUE partition ' || l_part_name;
    end if;

    if (l_error_text is null) then
	-- Drop partition
        execute 'drop table if exists ' || l_part_name || ' cascade';
    else
	if (p_raiseexception) then
          raise exception '%', l_error_text;
	else
          raise warning '%', l_error_text;
	  return null;
	end if;
    end if;

    return l_part_name;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
create or replace function @extschema@.f_check_gap_range(
    p_table_name text
)
 returns table (
	partition_name text,
	left_boundary text,
	right_boundary text,
	previous_left_boundary text,
	previous_right_boundary text
 )
 language plpgsql
as $function$
declare
    l_table_oid oid;
    l_pstrategy text;
    l_sql text;
    l_type text;
    l_record record;
    l_error_text text;
begin
    -- Validate table
    l_table_oid := @extschema@.f_is_eligible(p_table_name);
    if (l_table_oid is null) then
      raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
      return;
    end if;
    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;
    -- Raise error due to wrong strategy
    if (l_pstrategy <> 'range') then
        l_error_text := 'Check failed! Table ' || l_table_name || ' has not the range partition strategy, it is a ' || l_pstrategy || '.';
        raise warning '%', l_error_text;
        return;
    end if;

        l_sql := '
            with v1 as (
            select
		quote_ident(pt.relnamespace::regnamespace::text) || ''.'' || quote_ident(pt.relname) as relname,
                pg_get_expr(pt.relpartbound, pt.oid) as l,
		row_number() over (partition by base_tb.oid)
            from
                pg_class base_tb, pg_inherits i, pg_class pt
            where
                i.inhparent = base_tb.oid and
                pt.oid = i.inhrelid and
                base_tb.oid = ' || l_table_oid || '
            ), ranges as (
	    select
		v2.*,
		lag(lval,1) over(order by lval) prev_lval,
		lag(rval,1) over(order by lval) prev_rval
	    from (
              select v1.relname,
                (regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' ))[1]::' || l_type || ' as lval,
                (regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' ))[2]::' || l_type || ' as rval
              from v1
	      where
                   v1.l not like ''%(MAXVALUE)%'' and
                   v1.l not like ''%(MINVALUE)%''
	    ) v2
	    where
		v2.lval is not null and
		v2.rval is not null
            order by v2.lval asc
	    )
		select *
		from ranges
		where
			prev_rval is not null and
			prev_rval <> lval
	    
        ';
    for l_record in execute l_sql
	loop
		partition_name := l_record.relname;
		left_boundary := l_record.lval;
		right_boundary := l_record.rval;
		previous_left_boundary := l_record.prev_lval;
		previous_right_boundary := l_record.prev_rval;
		return next;
	end loop;
end;
$function$
;
create or replace function @extschema@.f_list_add_interval_partitions(
    p_table_name text,
    p_interval text,
    p_end_value anyelement,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_i integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_start p_end_value%TYPE;
    l_current_start p_end_value%TYPE;
    l_current_end p_end_value%TYPE;
    l_default_part_name text;
    l_defpartdata boolean;
    l_subpart_name text;
    l_temp_table_name text;
    l_last_part_name text;
    l_current_part_name text;
    l_sub_by text;
    current_srr_value text;
    l_message_text text;
    l_context text;
begin
    -- Validate table
    l_table_oid := @extschema@.f_is_eligible(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise error due to wrong strategy
    if (l_pstrategy <> 'list') then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' has not the list partition strategy, it is a ' || l_pstrategy || '.';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Raise error due to wrong type
    if (l_type <> pg_typeof(p_end_value)::text) then
        l_error_text := 'Adding failed! Type defined in the parameter ' || pg_typeof(p_end_value)::text || ' mismatches column type ' || l_type || ' in the table ' || l_table_name || '.';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Get "last" partition name and the boundary list value
    if (p_interval ~ '-') then
        l_sql := 'select a, b from @extschema@.f_get_min_max_value_list($1, $2, ''min'') as (a text, b ' || l_type || ')';
    else
        l_sql := 'select a, b from @extschema@.f_get_min_max_value_list($1, $2, ''max'') as (a text, b ' || l_type || ')';
    end if;
    execute l_sql into l_last_part_name, l_start using l_table_oid, p_end_value;

    -- Raise error due to there are no any partitions
    if (l_start is null) then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' does not have any partitions. Use f_list_add_partitions() or create at least one partition manually.';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
        l_last_part_name := p_subp_templ;
    end if;

    -- Get subpartition strategy
    l_sql := 'select pg_get_partkeydef($1::regclass)';
    execute l_sql into l_sub_by using l_last_part_name;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    l_temp_table_name := 'yktt' || substr(md5(random()::text), 0, 25);
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);

    -- We use DEFAULT partition
    if (p_keepdefault) then
      -- If default partition exists and contains data we have to detach it and rename to safely add new partition
      if (l_default_part_name is not null and l_defpartdata) then
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
      end if;
    -- We do not need DEFAULT partition
    else
      if (l_defpartdata) then
          raise warning 'Default partition % contains the data, so it can not be dropped!', l_default_part_name;
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
          p_keepdefault := true;
      else
          if (l_default_part_name is not null) then
            execute 'drop table if exists ' || l_default_part_name;
          end if;
      end if;
    end if;

    -- Create LIST partitions
    l_current_start := @extschema@.f_add_interval(l_start, p_interval);
    while ((l_current_start <= p_end_value and p_interval !~ '-') or (l_current_start >= p_end_value and p_interval ~ '-'))
    loop
        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', l_current_start::text);
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;

        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
	         ' for values in (''' || l_current_start::text || ''') ';
        execute l_sql;

	return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

      l_current_start := @extschema@.f_add_interval(l_current_start, p_interval);
    end loop;

    -- Create default partition if it is necessary and copy data from old default partition
    if (p_keepdefault) then
      -- We did not have default partition and create new one
      if (not l_defpartdata and l_default_part_name is null) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      -- We detached and renamed default partition earlier
      elsif (l_defpartdata) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_table_name;
        execute 'drop table if exists ' || l_schema || '.' || l_temp_table_name;
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      end if;
    end if;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
create or replace function @extschema@.f_range_add_partitions(
    p_table_name text,
    p_bond_arr anyarray,
    p_keepdefault boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_i integer;
    l_a integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_default_part_name text;
    l_defpartdata boolean;
    l_subpart_name text;
    l_temp_table_name text;
    l_last_part_name text;
    l_current_part_name text;
    l_sub_by text;
    current_srr_value text;
    l_message_text text;
    l_context text;
begin
    -- Sort the boundaries array
    select array(select distinct unnest(p_bond_arr) order by 1) into p_bond_arr;

    -- Parameters validation
    if (array_length(p_bond_arr, 1) < 2) then
        l_error_text := 'Adding failed! The number of boundaries must be greater than or equal to 2';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Validate table
    l_table_oid := @extschema@.f_is_eligible(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise an error due to wrong strategy
    if (l_pstrategy <> 'range') then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' does not have the range partition strategy, it is a ' || l_pstrategy || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Raise an error due to wrong type
    if (l_type <> pg_typeof(p_bond_arr[1])::text) then
        l_error_text := 'Adding failed! Type of array elements ' || pg_typeof(p_bond_arr[1])::text || ' mismatches column type ' || l_type || ' in the table ' || l_table_name || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check if there is only one partition for values FROM (MINVALUE) TO (MAXVALUE)
    select
        v1.part_name
    into
        l_t
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = l_table_oid and
        v1.part_boundary like '%) TO (MAXVALUE)' and
        v1.part_boundary like 'FOR VALUES FROM (MINVALUE) TO%';

    -- Raise an error due to MINMAX partition only
    if (l_t is not null) then
        l_error_text := 'Adding failed! Table ' || l_table_name || ' has MINMAX partition only ' || l_t || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check overlap
    l_t := @extschema@.f_get_overlap_by_range(l_table_oid, l_type, p_bond_arr[1], p_bond_arr[array_upper(p_bond_arr, 1)]);
    if (l_t is not null) then
        l_error_text := 'Adding failed! New partition for table ' || l_table_name || ' would overlap partition ' || l_t || '!';
	if (p_raiseexception) then
          raise exception '%', l_error_text;
	else
          raise warning '%', l_error_text;
	  return;
	end if;
    end if;

    -- Get "last" partition name
    l_sql := 'select a from @extschema@.f_get_min_max_value_range($1, $2, ''max'') as (a text, b ' || l_type || ')';
    execute l_sql into l_last_part_name using l_table_oid, p_bond_arr[array_upper(p_bond_arr, 1)];

    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
	l_last_part_name := p_subp_templ;
    end if;

    -- Get subpartition strategy
    l_sql := 'select pg_get_partkeydef($1::regclass)';
    execute l_sql into l_sub_by using l_last_part_name;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    l_temp_table_name := 'yktt' || substr(md5(random()::text), 0, 25);
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);

    -- We use DEFAULT partition
    if (p_keepdefault) then
      -- If default partition exists and contains data we have to detach it and rename to safely add new partition
      if (l_default_part_name is not null and l_defpartdata) then
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
      end if;
    -- We do not need DEFAULT partition
    else
      if (l_defpartdata) then
          raise warning 'Default partition % contains the data, so it should not be dropped!', l_default_part_name;
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
          p_keepdefault := true;
      else
          if (l_default_part_name is not null) then
            execute 'drop table if exists ' || l_default_part_name;
          end if;
      end if;
    end if;

    -- Create RANGE partitions
    for l_a in 2..array_length(p_bond_arr, 1)
    loop

        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', p_bond_arr[l_a - 1]::text);
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;
    
        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;
    
        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || p_bond_arr[l_a - 1]::text || ''') to (''' || p_bond_arr[l_a]::text || ''') ';
        execute l_sql;
    
        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    end loop;

    -- Create default partition if it is necessary and copy data from old default partition
    if (p_keepdefault) then
      -- We did not have default partition andhave to create a new one
      if (not l_defpartdata and l_default_part_name is null) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      -- We detached and renamed default partition earlier
      elsif (l_defpartdata) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_table_name;
        execute 'drop table if exists ' || l_schema || '.' || l_temp_table_name;
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      end if;
    end if;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
create or replace function @extschema@.f_list_merge_partitions(
    p_table_name text,
    p_part_arr text[],
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_i integer;
    l_a integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_temp_merge_table_name text;
    l_last_part_name text;
    l_current_part_name text;
    l_subpart_name text;
    l_sub_by text;
    l_arr text[];
    l_bound_arr text[];
    current_srr_value text;
    l_message_text text;
    l_context text;
begin
    -- Parameters validation
    if (array_length(p_part_arr, 1) < 2) then
        l_error_text := 'Merge failed! The number of partitions to merge must be greater than or equal to 2';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Validate table
    l_table_oid := @extschema@.f_is_eligible_for_detach(p_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise error due to wrong strategy
    if (l_pstrategy <> 'list') then
        l_error_text := 'Merge failed! Table ' || l_table_name || ' does not have the list partition strategy, it is a ' || l_pstrategy || '.';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check the partition name correctness
    for l_a in 1..array_length(p_part_arr, 1)
    loop
        l_t := p_part_arr[l_a];
        p_part_arr[l_a] := @extschema@.f_subp_is_eligible(p_part_arr[l_a], false);
        if (p_part_arr[l_a] is null) then
          l_error_text := 'Merge failed! Table ' || coalesce(l_t, '') || ' is not found!';
          if (p_raiseexception) then
             raise exception '%', l_error_text;
          else
             raise warning '%', l_error_text;
             return;
          end if;
        end if;
    end loop;

    -- Get the array of merged partition values
    l_t := '(''' || array_to_string(p_part_arr, ''',''') || ''')';
    l_sql := $$
    select array(
    select unnest(
             string_to_array(regexp_replace(part_boundary, 'FOR VALUES IN \(''?|''?\)|''?', '', 'g'), ', ')
    )
    from @extschema@.v_pt_tree
    where main_table_oid = $$ || l_table_oid || ' and part_name in ' || l_t || ')';
    execute l_sql into l_bound_arr;

    -- Get subpartition strategy
    -- Get partition name with subpartitions and the boundary list value
    select part_name, pg_get_partkeydef(part_oid) as partdef
    into l_last_part_name, l_sub_by
    from @extschema@.v_pt_tree
    where main_table_oid = l_table_oid and pg_get_partkeydef(part_oid) is not null
    limit 1;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;
    -- Check if subpartition template can be used
    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
        l_sql := 'select pg_get_partkeydef($1::regclass)';
        execute l_sql into l_sub_by using p_subp_templ;
        if (l_sub_by is not null) then
            l_last_part_name := p_subp_templ;
            l_sub_by := 'PARTITION BY ' || l_sub_by;
        end if;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    -- Detach partitions we are going to merge
    for l_a in 1..array_length(p_part_arr, 1)
    loop
      l_temp_merge_table_name := 'ykmerge' || substr(md5(random()::text), 0, 25);
      execute 'alter table if exists ' || l_table_name || ' detach partition ' || p_part_arr[l_a];
      execute 'alter table if exists ' || p_part_arr[l_a] || ' rename to ' || l_temp_merge_table_name;
      if (l_last_part_name = p_part_arr[l_a]) then
        l_last_part_name := l_schema || '.' || l_temp_merge_table_name;
      end if;
      p_part_arr[l_a] := l_schema || '.' || l_temp_merge_table_name;
    end loop;

    -- Create table to attach as a partition later on
    l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', l_bound_arr[1]);
    if current_setting('server_version_num')::integer >= 120000::integer then
     l_t := ' including generated ';
    else
     l_t := null;
    end if;
    l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
             ' including defaults including constraints including storage including indexes' ||
             coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
    execute l_sql;
    -- Create the subpartitions if any
    l_i := 0;
    for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
    loop
        l_i := l_i + 1;
        l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text); 
        l_temp_merge_table_name := 'ykmerge' || substr(md5(random()::text), 0, 25);
        execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_merge_table_name;
        l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name || 
	         ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
        execute l_sql;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
    end loop;
    -- Attach the table as a partition to the main table
    l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
             ' for values in (''' || array_to_string(l_bound_arr, ''', ''') || ''')';
    execute l_sql;
    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    -- Insert the data from detached partition
    for l_a in 1..array_length(p_part_arr, 1)
    loop
      execute 'insert into ' || l_table_name || ' overriding system value select * from ' || p_part_arr[l_a];
      execute 'drop table if exists ' || p_part_arr[l_a];
    end loop;

    return next l_schema || '.' || l_current_part_name;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;
end;
$function$
;
create or replace function @extschema@.f_get_all_part_info(
    p_table_name text
)
returns table(
    main_table_name     text,
    partition_name      text,
    partition_boundary  text,
    partition_strategy  text
)
language sql
as $function$
with recursive subparts as (
        select
            main_table_name,
            part_name,
            part_boundary,
            partition_strategy
        from
            @extschema@.v_pt_tree
	where
	    main_table_name = p_table_name
        union
        select
            e.main_table_name,
            e.part_name,
            e.part_boundary,
            e.partition_strategy
        from
            @extschema@.v_pt_tree e,
            subparts s
        where
            s.part_name = e.main_table_name
    )
    select
        *
    from
        subparts
    order by 1,2
;
$function$
;
create or replace function @extschema@.f_range_split_partition(
    p_part_name text,
    p_bond_arr anyarray,
    p_keepdefault boolean,
    p_keepminmaxvalue boolean,
    p_raiseexception boolean,
    p_subp_templ text default null::text
)
 returns setof text
 language plpgsql
as $function$
declare
    l_table_name text;
    l_only_table_name text;
    l_table_oid oid;
    l_error_text text;
    l_i integer;
    l_a integer;
    l_schema text;
    l_pstrategy text;
    l_type text;
    l_sql text;
    l_t text;
    l_default_part_name text;
    l_defpartdata boolean;
    l_subpart_name text;
    l_temp_table_name text;
    l_last_part_name text;
    l_current_part_name text;
    l_sub_by text;
    l_min_part text;
    l_max_part text;
    l_temp_split_table_name text;
    l_temp_sub_table_name text;
    current_srr_value text;
    l_message_text text;
    l_context text;
begin
    -- Sort the boundaries array
    select array(select distinct unnest(p_bond_arr) order by 1) into p_bond_arr;

    -- Parameters validation
    if (array_length(p_bond_arr, 1) < 2) then
        l_error_text := 'Split failed! The number of boundaries must be greater than or equal to 2';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    p_part_name := @extschema@.f_subp_is_eligible(p_part_name, false);
    if (p_part_name is null) then
        if (p_raiseexception) then
          raise exception 'Could not validate partition name %', coalesce(p_part_name, 'unknown');
        else
          raise warning 'Could not validate partition name %', coalesce(p_part_name, 'unknown');
          return;
        end if;
    end if;

    select main_table_name into l_table_name from @extschema@.v_pt_tree where part_name = p_part_name;

    -- Validate table
    l_table_oid := @extschema@.f_is_eligible(l_table_name);
    if (l_table_oid is null) then
        if (p_raiseexception) then
          raise exception 'Could not get an OID for table %', coalesce(l_table_name, 'unknown');
        else
          raise warning 'Could not get an OID for table %', coalesce(l_table_name, 'unknown');
          return;
        end if;
    end if;
    -- Get valid table name qualified by schema
    select
        quote_ident(pc.relnamespace::regnamespace::text) || '.' || quote_ident(pc.relname) as tn,
        pc.relnamespace::regnamespace,
        quote_ident(pc.relname)
    into
        l_table_name,
        l_schema,
        l_only_table_name
    from
        pg_class pc
    where
        pc.oid = l_table_oid
    ;

    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        @extschema@.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;

    -- Raise an error due to wrong strategy
    if (l_pstrategy <> 'range') then
        l_error_text := 'Split failed! Table ' || l_table_name || ' does not have the range partition strategy, it is a ' || l_pstrategy || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Raise an error due to wrong type
    if (l_type <> pg_typeof(p_bond_arr[1])::text) then
        l_error_text := 'Split failed! Type of array elements ' || pg_typeof(p_bond_arr[1])::text || ' mismatches column type ' || l_type || ' in the table ' || l_table_name || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check if there is only one partition for values FROM (MINVALUE) TO (MAXVALUE)
    select
        v1.part_name
    into
        l_t
    from
        @extschema@.v_pt_tree v1
    where
        v1.main_table_oid = l_table_oid and
        v1.part_boundary like '%) TO (MAXVALUE)' and
        v1.part_boundary like 'FOR VALUES FROM (MINVALUE) TO%';

    -- Raise an error due to MINMAX partition only
    if (l_t is not null) then
        l_error_text := 'Split failed! Table ' || l_table_name || ' has MINMAX partition only ' || l_t || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Check if main table has MINMAX partitions
    l_max_part := @extschema@.f_get_maxvalue_part_name(l_table_oid);
    l_min_part := @extschema@.f_get_minvalue_part_name(l_table_oid);

    -- Check overlap with rest of partitions
    l_t := @extschema@.f_get_overlap_by_range(l_table_oid, l_type, p_bond_arr[1], p_bond_arr[array_upper(p_bond_arr, 1)], p_part_name);
    if (l_t is not null) then
        l_error_text := 'Adding failed! New partition for table ' || l_table_name || ' would overlap partition ' || l_t || '!';
        if (p_raiseexception) then
          raise exception '%', l_error_text;
        else
          raise warning '%', l_error_text;
          return;
        end if;
    end if;

    -- Get "last" partition name
    l_sql := 'select a from @extschema@.f_get_min_max_value_range($1, $2, ''max'') as (a text, b ' || l_type || ')';
    execute l_sql into l_last_part_name using l_table_oid, p_bond_arr[array_upper(p_bond_arr, 1)];
    if (l_max_part is not null) then
	l_last_part_name := l_max_part;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform @extschema@.f_set_config('session_replication_role', 'replica', true);

    -- Detach partition we are going to split
    l_temp_split_table_name := 'yksplit' || substr(md5(random()::text), 0, 25);
    execute 'alter table if exists ' || l_table_name || ' detach partition ' || p_part_name;
    execute 'alter table if exists ' || p_part_name || ' rename to ' || l_temp_split_table_name;
    if (l_last_part_name = p_part_name) then
        l_last_part_name := l_schema || '.' || l_temp_split_table_name;
    end if;

    p_subp_templ := @extschema@.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
	l_last_part_name := p_subp_templ;
    end if;

    -- Get subpartition strategy
    l_sql := 'select pg_get_partkeydef($1::regclass)';
    execute l_sql into l_sub_by using l_last_part_name;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;

    l_temp_table_name := 'yktt' || substr(md5(random()::text), 0, 25);
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);

    -- We use DEFAULT partition
    if (p_keepdefault) then
      -- If default partition exists and contains data we have to detach it and rename to safely add new partition
      if (l_default_part_name is not null and l_defpartdata) then
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
      end if;
    -- We do not need DEFAULT partition
    else
      if (l_defpartdata) then
          raise warning 'Default partition % contains the data, so it should not be dropped!', l_default_part_name;
          execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_default_part_name;
          execute 'alter table if exists ' || l_default_part_name || ' rename to ' || l_temp_table_name;
          p_keepdefault := true;
      else
          if (l_default_part_name is not null) then
            execute 'drop table if exists ' || l_default_part_name;
          end if;
      end if;
    end if;

    -- Create RANGE partitions
    for l_a in 2..array_length(p_bond_arr, 1)
    loop

        l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', p_bond_arr[l_a - 1]::text);
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;
    
        -- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_temp_sub_table_name := 'yksplit' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || p_bond_arr[l_a - 1]::text || ''') to (''' || p_bond_arr[l_a]::text || ''') ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    end loop;

    -- Create MINMAX partition if we split it and going to use it.
    if (p_part_name = l_max_part and p_keepminmaxvalue) then

	l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'maxvalue');
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;
	-- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;
	-- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || p_bond_arr[array_upper(p_bond_arr, 1)]::text || ''') to (MAXVALUE) ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);
    elsif (p_part_name = l_min_part and p_keepminmaxvalue) then

	l_current_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'minvalue');
        -- Create table to attach as a partition later on
        if current_setting('server_version_num')::integer >= 120000::integer then
         l_t := ' including generated ';
        else
         l_t := null;
        end if;
        l_sql := 'create table ' || l_schema || '.' || l_current_part_name || '( like ' || l_table_name ||
                 ' including defaults including constraints including storage including indexes' ||
                 coalesce(l_t, '') || ') ' || coalesce(l_sub_by, '');
        execute l_sql;
	-- Create the subpartitions if any
        l_i := 0;
        for l_t in select part_boundary from @extschema@.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := @extschema@.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text);
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;
	-- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (MINVALUE) to (''' || p_bond_arr[1]::text || ''')';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);
    end if;

    -- Create default partition if it is necessary and copy data from old default partition
    if (p_keepdefault) then
      -- We did not have default partition and have to create a new one
      if (not l_defpartdata and l_default_part_name is null) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      -- We detached and renamed default partition earlier
      elsif (l_defpartdata) then
        l_default_part_name := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
                l_table_name || ' default';
        execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_table_name;
        execute 'drop table if exists ' || l_schema || '.' || l_temp_table_name;
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      end if;
    end if;

    -- Insert the data from detached partition
    l_default_part_name := @extschema@.f_get_default_part_name(l_table_oid);
    -- Create default partition to be sure we do not lose the data from detached partition
    if (l_default_part_name is null) then
      l_t := @extschema@.f_set_part_name(l_only_table_name, '_p_', 'default');
      execute 'create table if not exists ' || l_schema || '.' || l_t || ' partition of ' || l_table_name || ' default';
      perform @extschema@.f_grant_table_privileges(l_schema, l_only_table_name, l_t);
    end if;
    execute 'insert into ' || l_table_name || ' overriding system value select * from ' || l_schema || '.' || l_temp_split_table_name;
    execute 'drop table if exists ' || l_schema || '.' || l_temp_split_table_name;
    -- Raise exception if shouldn't have to be default partition but it has a data
    l_defpartdata := @extschema@.f_default_part_has_data(l_table_oid);
    if (l_default_part_name is null and l_defpartdata) then
        raise warning 'New partitions for table % do not cover all the data from MINMAX partition! DEFAULT partition % has been created!', l_table_name, l_t;
    elsif (l_default_part_name is null and not l_defpartdata) then
      execute 'drop table if exists ' || l_schema || '.' || l_t;
    end if;

    -- Set session_replication_role parameter to previous value
    perform @extschema@.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
