create or replace function mpartman.f_list_split_partition(
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
    p_part_name := mpartman.f_subp_is_eligible(p_part_name, false);
    if (p_part_name is null) then
        if (p_raiseexception) then
          raise exception 'Could not validate partition name %', coalesce(p_part_name, 'unknown');
        else
          raise warning 'Could not validate partition name %', coalesce(p_part_name, 'unknown');
          return;
        end if;
    end if;

    select main_table_name into l_table_name from mpartman.v_pt_tree where part_name = p_part_name;

    -- Validate table
    l_table_oid := mpartman.f_is_eligible_for_detach(l_table_name);
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
        mpartman.v_partitioned_tables vpt
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
    from mpartman.v_pt_tree
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
    from mpartman.v_pt_tree
    where main_table_oid = l_table_oid and pg_get_partkeydef(part_oid) is not null
    limit 1;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;
    -- Check if subpartition template can be used
    p_subp_templ := mpartman.f_subp_is_eligible(p_subp_templ, true);
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
    perform mpartman.f_set_config('session_replication_role', 'replica', true);

    -- Detach partition we are going to split
    l_temp_split_table_name := 'yksplit' || substr(md5(random()::text), 0, 25);
    execute 'alter table if exists ' || l_table_name || ' detach partition ' || p_part_name;
    execute 'alter table if exists ' || p_part_name || ' rename to ' || l_temp_split_table_name;
    if (l_last_part_name = p_part_name) then
      l_last_part_name := l_schema || '.' || l_temp_split_table_name;
    end if;

    -- Create new partitions
    for l_record in select * from mpartman.f_list_add_partitions(l_table_name, p_values, p_keepdefault, p_raiseexception, p_subp_templ) flap(f1)
    loop
	return next l_record.f1;
    end loop;

    -- Insert the data from detached partition
    execute 'insert into ' || l_table_name || ' select * from ' || l_schema || '.' || l_temp_split_table_name;
    execute 'drop table if exists ' || l_schema || '.' || l_temp_split_table_name;

    -- Set session_replication_role parameter to previous value
    perform mpartman.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;
end;
$function$
;
