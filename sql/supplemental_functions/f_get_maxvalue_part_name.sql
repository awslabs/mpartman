create or replace function mpartman.f_get_maxvalue_part_name(
    p_table_oid oid
)
 returns text
 language plpgsql
as $function$
declare
    l_t text;
begin
    -- Get partition table name with MAXVALUE boundary
    select
        v1.part_name
    into
        l_t
    from
        mpartman.v_pt_tree v1
    where
        v1.main_table_oid = p_table_oid and
        v1.part_boundary like '%) TO (MAXVALUE)'
    ;

    return l_t;
end;
$function$
;
