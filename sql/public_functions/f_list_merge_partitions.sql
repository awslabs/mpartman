create or replace function mpartman.f_list_merge_partitions(
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
    l_table_oid := mpartman.f_is_eligible_for_detach(p_table_name);
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
        mpartman.v_partitioned_tables vpt
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
        p_part_arr[l_a] := mpartman.f_subp_is_eligible(p_part_arr[l_a], false);
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
    from mpartman.v_pt_tree
    where main_table_oid = $$ || l_table_oid || ' and part_name in ' || l_t || ')';
    execute l_sql into l_bound_arr;

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
    l_current_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', l_bound_arr[1]);
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
    for l_t in select part_boundary from mpartman.v_pt_tree where main_table_name = l_last_part_name
    loop
        l_i := l_i + 1;
        l_subpart_name := mpartman.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text); 
        l_temp_merge_table_name := 'ykmerge' || substr(md5(random()::text), 0, 25);
        execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_merge_table_name;
        l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name || 
	         ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
        execute l_sql;
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
    end loop;
    -- Attach the table as a partition to the main table
    l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
             ' for values in (''' || array_to_string(l_bound_arr, ''', ''') || ''')';
    execute l_sql;
    perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    -- Insert the data from detached partition
    for l_a in 1..array_length(p_part_arr, 1)
    loop
      execute 'insert into ' || l_table_name || ' select * from ' || p_part_arr[l_a];
      execute 'drop table if exists ' || p_part_arr[l_a];
    end loop;

    return next l_schema || '.' || l_current_part_name;

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
