create or replace function mpartman.f_do_retention(
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
        mpartman.v_pt_tree v1
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
		perform mpartman.f_set_schema(l_r.relname, p_retentionschema);
	    end if;
	elsif (p_action = 'drop') then
            -- Drop partition
            execute 'drop table if exists ' || l_r.relname || ' cascade';
	end if;
	return next l_r.relname;
    end loop;

    -- Get MINVALUE or MAXVALUE partitions
    l_max_part := mpartman.f_get_maxvalue_part_name(l_table_oid);
    l_min_part := mpartman.f_get_minvalue_part_name(l_table_oid);

    if (not p_keepminmaxvalue) then
	if (p_action = 'detach') then
            -- Detach partition
            if (p_direction = 'old' and l_min_part is not null) then
                execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_min_part;
	        if (p_retentionschema is not null) then
		    perform mpartman.f_set_schema(l_min_part, p_retentionschema);
	        end if;
		return next l_min_part;
            elsif (p_direction = 'new' and l_max_part is not null) then
                execute 'alter table if exists ' || l_table_name || ' detach partition ' || l_max_part;
	        if (p_retentionschema is not null) then
		    perform mpartman.f_set_schema(l_max_part, p_retentionschema);
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
