create or replace function mpartman.f_check_gap_range(
    p_table_name text
)
 returns table (
	partition_name text,
	left_boundary text,
	right_boundary text,
	previous_left_boundary text,
	previous_right_boundary text
 )
 language plpgsql
as $function$
declare
    l_table_oid oid;
    l_pstrategy text;
    l_sql text;
    l_type text;
    l_record record;
    l_error_text text;
begin
    -- Validate table
    l_table_oid := mpartman.f_is_eligible(p_table_name);
    if (l_table_oid is null) then
      raise warning 'Could not get an OID for table %', coalesce(p_table_name, 'unknown');
      return;
    end if;
    -- Get partition strategy and column type
    select
        vpt.partition_strategy, part_col_data_type
    into
        l_pstrategy, l_type
    from
        mpartman.v_partitioned_tables vpt
    where
        vpt.oid = l_table_oid
    ;
    -- Raise error due to wrong strategy
    if (l_pstrategy <> 'range') then
        l_error_text := 'Check failed! Table ' || l_table_name || ' has not the range partition strategy, it is a ' || l_pstrategy || '.';
        raise warning '%', l_error_text;
        return;
    end if;

        l_sql := '
            with v1 as (
            select
		quote_ident(pt.relnamespace::regnamespace::text) || ''.'' || quote_ident(pt.relname) as relname,
                pg_get_expr(pt.relpartbound, pt.oid) as l,
		row_number() over (partition by base_tb.oid)
            from
                pg_class base_tb, pg_inherits i, pg_class pt
            where
                i.inhparent = base_tb.oid and
                pt.oid = i.inhrelid and
                base_tb.oid = ' || l_table_oid || '
            ), ranges as (
	    select
		v2.*,
		lag(lval,1) over(order by lval) prev_lval,
		lag(rval,1) over(order by lval) prev_rval
	    from (
              select v1.relname,
                (regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' ))[1]::' || l_type || ' as lval,
                (regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' ))[2]::' || l_type || ' as rval
              from v1
	      where
                   v1.l not like ''%(MAXVALUE)%'' and
                   v1.l not like ''%(MINVALUE)%''
	    ) v2
	    where
		v2.lval is not null and
		v2.rval is not null
            order by v2.lval asc
	    )
		select *
		from ranges
		where
			prev_rval is not null and
			prev_rval <> lval
	    
        ';
    for l_record in execute l_sql
	loop
		partition_name := l_record.relname;
		left_boundary := l_record.lval;
		right_boundary := l_record.rval;
		previous_left_boundary := l_record.prev_lval;
		previous_right_boundary := l_record.prev_rval;
		return next;
	end loop;
end;
$function$
;
