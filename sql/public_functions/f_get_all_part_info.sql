create or replace function mpartman.f_get_all_part_info(
    p_table_name text
)
returns table(
    main_table_name     text,
    partition_name      text,
    partition_boundary  text,
    partition_strategy  text
)
language sql
as $function$
with recursive subparts as (
        select
            main_table_name,
            part_name,
            part_boundary,
            partition_strategy
        from
            mpartman.v_pt_tree
	where
	    main_table_name = p_table_name
        union
        select
            e.main_table_name,
            e.part_name,
            e.part_boundary,
            e.partition_strategy
        from
            mpartman.v_pt_tree e,
            subparts s
        where
            s.part_name = e.main_table_name
    )
    select
        *
    from
        subparts
    order by 1,2
;
$function$
;
