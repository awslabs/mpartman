create or replace function mpartman.f_range_add_interval_partitions(
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
    l_table_oid := mpartman.f_is_eligible(p_table_name);
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
        mpartman.v_pt_tree v1
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
        l_sql := 'select a, b from mpartman.f_get_min_max_value_range($1, $2, ''min'') as (a text, b ' || l_type || ')';
    else
        l_sql := 'select a, b from mpartman.f_get_min_max_value_range($1, $2, ''max'') as (a text, b ' || l_type || ')';
    end if;
    execute l_sql into l_last_part_name, l_start using l_table_oid, p_end_value;

    p_subp_templ := mpartman.f_subp_is_eligible(p_subp_templ, true);
    if (p_subp_templ is not null) then
        l_last_part_name := p_subp_templ;
    end if;

    -- Set session_replication_role parameter to replica
    current_srr_value := current_setting('session_replication_role');
    perform mpartman.f_set_config('session_replication_role', 'replica', true);

    -- Handle the case if there are MINVALUE or MAXVALUE partitions
    l_max_part := mpartman.f_get_maxvalue_part_name(l_table_oid);
    l_max_value := mpartman.f_get_range_bvalue(l_max_part, 'left');
    l_min_part := mpartman.f_get_minvalue_part_name(l_table_oid);
    l_min_value := mpartman.f_get_range_bvalue(l_min_part, 'right');
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
        l_current_start := mpartman.f_add_interval(l_current_end, p_interval);

	while l_current_start >= p_end_value
        loop

        l_current_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', l_current_start::text);
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
            l_temp_sub_table_name := 'yksubtemp' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || l_current_start::text || ''') to (''' || l_current_end::text || ''') ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

        l_current_end := l_current_start;
        l_current_start := mpartman.f_add_interval(l_current_start, p_interval);
        end loop;

    else

        l_current_start := l_start;
        l_current_end := mpartman.f_add_interval(l_current_start, p_interval);
        while l_current_end <= p_end_value
        loop

        l_current_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', l_current_start::text);
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
            l_temp_sub_table_name := 'yksubtemp' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || l_current_start::text || ''') to (''' || l_current_end::text || ''') ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

        l_current_start := l_current_end;
        l_current_end := mpartman.f_add_interval(l_current_end, p_interval);
        end loop;
    end if;

    -- Handle the case if there are MINVALUE or MAXVALUE partitions
    -- Create MINMAXvalue partition if necessary 
    if (p_interval ~ '-' and p_keepminmaxvalue) then

        l_current_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', 'minvalue'::text);
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
            l_temp_sub_table_name := 'yksubtemp' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (MINVALUE) to (''' || l_current_end::text || ''') ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    elsif (p_keepminmaxvalue) then

        l_current_part_name := mpartman.f_set_part_name(l_only_table_name, '_p_', 'maxvalue'::text);
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
            l_temp_sub_table_name := 'yksubtemp' || substr(md5(random()::text), 0, 25);
            execute 'alter table if exists ' || l_schema || '.' || l_subpart_name || ' rename to ' || l_temp_sub_table_name;
            l_sql := 'create table if not exists ' || l_schema || '.' || l_subpart_name ||
                 ' partition of ' || l_schema || '.' || l_current_part_name || ' ' || l_t;
            execute l_sql;
	    perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_subpart_name);
        end loop;

        -- Attach the table as a partition to the main table
        l_sql := 'alter table ' || l_table_name || ' attach partition ' || l_schema || '.' || l_current_part_name ||
                 ' for values from (''' || l_current_start::text || ''') to (MAXVALUE) ';
        execute l_sql;

        return next l_schema || '.' || l_current_part_name;
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_current_part_name);

    end if;

    -- Create default partition if it is necessary and copy data from old default partition
    if (p_keepdefault) then
      -- We did not have default partition and have to create a new one
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

    -- Insert the data from detached minmax partition if such a partiton exists
    l_default_part_name := mpartman.f_get_default_part_name(l_table_oid);
    if (l_temp_mm_table_name is not null) then
      -- Create default partition to be sure we do not lose the data from MINMAX partition
      if (l_default_part_name is null) then
        l_t := mpartman.f_set_part_name(l_only_table_name, '_p_', 'default');
        execute 'create table if not exists ' || l_schema || '.' || l_t || ' partition of ' || l_table_name || ' default';
	perform mpartman.f_grant_table_privileges(l_schema, l_only_table_name, l_t);
      end if;
      execute 'insert into ' || l_table_name || ' select * from ' || l_schema || '.' || l_temp_mm_table_name;
      execute 'drop table if exists ' || l_schema || '.' || l_temp_mm_table_name;
      -- Raise exception if shouldn't have to be default partition but it has a data
      l_defpartdata := mpartman.f_default_part_has_data(l_table_oid);
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
    perform mpartman.f_set_config('session_replication_role', current_srr_value, true);

    return;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;

end;
$function$
;
