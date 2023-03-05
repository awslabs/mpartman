create or replace function mpartman.f_default_part_has_data(
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
    l_t := mpartman.f_get_default_part_name(p_table_oid);

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
