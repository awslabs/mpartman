create or replace function mpartman.f_set_part_name(
    p_table_name text,
    p_part_prefix text,
    p_part_suffix text
)
 returns text
 language plpgsql
as $function$
declare
    l_part_suffix text;
    l_new_table_name text;
    l_rundom_suffix text;
begin
    l_part_suffix := p_part_suffix;
    l_rundom_suffix := '_' || substr(md5(random()::text), 1, 4);

    -- Remove and/or replace unnecessary symbols.
    l_part_suffix := pg_catalog.regexp_replace(l_part_suffix, ' 00:00:00$', '');
    l_part_suffix := pg_catalog.regexp_replace(l_part_suffix, '[''-\.,\+\s:]', '_', 'ig');

    -- !!! Do not use this replacement due to the significant zeros
    ------ l_part_suffix := pg_catalog.regexp_replace(l_part_suffix, '0+$', '0');

    -- Concatenate new partition name
    l_new_table_name := trim(both '"' from p_table_name) || p_part_prefix || l_part_suffix || l_rundom_suffix;
    -- If new name is longer than allowed table name length, the random name is generated
    if (l_new_table_name <> l_new_table_name::name::text)
    then
        l_new_table_name := pg_catalog.regexp_replace(('npt_' || md5(random()::text) 
			    || gen_random_uuid()::text)::name::text, '[''-\.,\+\s:]', '_', 'ig');
    end if;
    -- Remove a trail underscore.
    l_new_table_name := pg_catalog.regexp_replace(l_new_table_name, '_$', '', 'ig');

    -- !!! Do not use this replacement due to the possible '-' sign in the value
    ------ l_new_table_name := pg_catalog.regexp_replace(l_new_table_name, '_+', '_', 'ig');

    return lower(l_new_table_name);
end;
$function$
;
