create or replace function mpartman.f_get_hash_part_by_value(
    p_table_oid oid,
    p_type text,
    p_value text
)
 returns text
 language plpgsql
as $function$
declare
    l_sql text;
    l_bool boolean;
    l_rec record;
begin
    for l_rec in (
                  select
                    part_name,
                    regexp_replace(part_boundary, '(.*)modulus ([[:digit:]]+), (.*)', '\2') as mdl,
                    regexp_replace(part_boundary, '(.*)remainder ([[:digit:]]+)\)', '\2') as rmd
                  from mpartman.v_pt_tree
                  where main_table_oid = p_table_oid
                 )
    loop
        l_sql := 'select satisfies_hash_partition(' || p_table_oid || ', ' || l_rec.mdl || ', ' || l_rec.rmd || ', ' || p_value || '::' || p_type ||')';
        execute l_sql into l_bool;
        if l_bool
        then
          return l_rec.part_name;
        end if;
    end loop;

    return null::text;

end;
$function$
;
