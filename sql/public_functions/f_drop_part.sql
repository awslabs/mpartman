create or replace function mpartman.f_drop_part(
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
    l_table_oid := mpartman.f_is_eligible_for_detach(p_table_name);
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
    l_part_name := mpartman.f_find_part_by_value(l_table_oid, true, p_value);

    -- Get default partition name if exists
    l_default_part_name := mpartman.f_get_default_part_name(l_table_oid);
    l_defpartdata := mpartman.f_default_part_has_data(l_table_oid);

    -- Get MINVALUE or MAXVALUE partitions
    l_max_part := mpartman.f_get_maxvalue_part_name(l_table_oid);
    l_min_part := mpartman.f_get_minvalue_part_name(l_table_oid);

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
