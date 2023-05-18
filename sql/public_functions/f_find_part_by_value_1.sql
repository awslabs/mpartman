create or replace function mpartman.f_find_part_by_value(
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
    p_table_oid := mpartman.f_is_eligible_for_detach(p_table_name);
    if (p_table_oid is null) then
       raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
       return null;
    end if;

    -- Get partition column type
    select vpt.part_col_data_type into l_type
    from mpartman.v_partitioned_tables vpt where vpt.oid = p_table_oid;

    -- Get part name by value
    l_table_name := mpartman.f_get_part_by_value(p_table_oid, l_type, p_value);

    select v1.part_boundary into l_t from mpartman.v_pt_tree v1 where v1.part_name = l_table_name;
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
      from mpartman.v_partitioned_tables vpt where vpt.oid = p_table_oid2;

      -- Get subpart name by values
      if (l_type2 is null) then
        return null;
      else
        l_table_name := mpartman.f_get_part_by_value(p_table_oid, l_type, p_value, l_type2, p_value2);
        return l_table_name;
      end if;

    end if;

end;
$function$
;
