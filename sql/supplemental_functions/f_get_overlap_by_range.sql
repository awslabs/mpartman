create or replace function mpartman.f_get_overlap_by_range(
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
    l_ret := mpartman.f_find_part_by_value(p_table_oid, false, p_min::text);
    if (l_ret is not null) then
	if (p_filter_part is not null and l_ret = p_filter_part) then
	  null;
        else
	  return l_ret;
	end if;
    end if;

    -- Check if "MAXVALUE" partition is overlapped
    l_max_part_name := mpartman.f_get_maxvalue_part_name(p_table_oid);
    if (l_max_part_name is not null) then
       l_left_value := mpartman.f_get_range_bvalue(l_max_part_name, 'left');
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
