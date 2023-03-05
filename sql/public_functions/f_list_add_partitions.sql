create or replace function mpartman.f_list_add_partitions(
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
    from mpartman.v_pt_tree
    where main_table_oid = l_table_oid
    ) into l_bound_arr;

    -- Get partition name with subpartitions and the boundary list value
    select part_name, pg_get_partkeydef(part_oid) as partdef
    into l_last_part_name, l_sub_by
    from mpartman.v_pt_tree
    where main_table_oid = l_table_oid and pg_get_partkeydef(part_oid) is not null
    limit 1;
    if (l_sub_by is not null) then
        l_sub_by := 'PARTITION BY ' || l_sub_by;
    end if;

    p_subp_templ := mpartman.f_subp_is_eligible(p_subp_templ, true);
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
    perform mpartman.f_set_config('session_replication_role', 'replica', true);

    l_temp_table_name := 'yktt' || substr(md5(random()::text), 0, 25);
    l_default_part_name := mpartman.f_get_default_part_name(l_table_oid);
    l_defpartdata := mpartman.f_default_part_has_data(l_table_oid);

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
        l_current_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', l_arr[1]); 
    
      -- If json array element can not be used as a value for column.
      elsif (l_record.jtype = 'object' or l_record.jtype = 'null' or l_record.jtype = 'boolean') then
        raise warning 'Partition of table % has not been added for value % due to this is %!', l_table_name, l_record.f1::text, l_record.jtype;
    
      else
        l_arr[1] := l_record.f1#>>'{}';
        -- Escape an apostrophe if exists.
        l_arr := array(select regexp_replace(t.val, '''', '''''', 'g') from unnest(l_arr) as t(val));
        l_current_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', l_arr[1]); 
    
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
        for l_t in select part_boundary from mpartman.v_pt_tree where main_table_name = l_last_part_name
        loop
            l_i := l_i + 1;
            l_subpart_name := mpartman.f_set_part_name(l_current_part_name, '_subp_'::text, l_i::text); 
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name || 
    	         ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;
        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values in (''' || array_to_string(l_arr, ''', ''') || ''')';
        execute l_sql;
        l_bound_arr := array_cat(l_bound_arr, l_arr); 
        return next l_schema || '.' || l_current_part_name;
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);
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
	l_default_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' || 
	        l_table_name || ' default';
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      -- We detached and renamed default partition earlier
      elsif (l_defpartdata) then
	l_default_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_default_part_name || ' partition of ' ||
	        l_table_name || ' default';
        execute 'insert into ' || l_table_name || ' select * from ' || l_schema || '.' || l_temp_table_name;
        execute 'drop table if exists ' || l_schema || '.' || l_temp_table_name;
        -- We created new default partition, so have to return its name along with other created partitions
        return next l_schema || '.' || l_default_part_name;
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_default_part_name);
      end if;
    end if;

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
