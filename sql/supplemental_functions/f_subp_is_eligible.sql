create or replace function mpartman.f_subp_is_eligible(
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
