create or replace function mpartman.f_has_default_part(
    p_table_oid oid
)
 returns boolean
 language plpgsql
as $function$
declare
    l_b boolean;
    l_i integer;
begin
    l_b := true;
    select
        count(*)
    into
        l_i
    from
        mpartman.v_pt_tree v1
    where
        v1.main_table_oid = p_table_oid and
        v1.part_boundary='DEFAULT'
    ;
    if (l_i = 0) then
        l_b := false;
    end if;
    return l_b;
end;
$function$
;
