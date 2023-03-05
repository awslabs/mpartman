create or replace function mpartman.f_get_range_bvalue(
    p_part_name text,
    p_left_right text
)
 returns text
 language plpgsql
as $function$
declare
    l_sql text;
    l_ret text;
begin
    if (p_left_right = 'left') then
            select
                btrim( (regexp_match( pt.part_boundary, '\((.*)\) to \((.*)\)', 'i' ))[1], '''' ) as lval
            into
                l_ret
            from
                mpartman.v_pt_tree pt
            where
	        pt.part_name = p_part_name
            ;
    elsif (p_left_right = 'right') then
            select
                btrim( (regexp_match( pt.part_boundary, '\((.*)\) to \((.*)\)', 'i' ))[2], '''' ) as rval
            into
                l_ret
            from
                mpartman.v_pt_tree pt
            where
	        pt.part_name = p_part_name
            ;
    end if;

    return l_ret;
end;
$function$
;
