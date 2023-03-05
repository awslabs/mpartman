create or replace function mpartman.f_get_min_max_value_range(
    p_table_oid oid,
    p_value anyelement,
    p_minmax text
)
 returns record
 language plpgsql
as $function$
declare
    l_sql text;
    l_type text;
    l_record record;
begin
    l_type := pg_typeof(p_value);
    if (p_minmax = 'max') then
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
                base_tb.oid = ' || p_table_oid || '
            )
            select v1.relname as part_name,
                btrim( (regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' ))[2], '''''''' )::' || l_type || ' as rval
            from
                v1
            where
                v1.l not like ''%(MAXVALUE)%'' and
                v1.l not like ''%(MINVALUE)%''
            order by 2 desc nulls last
            limit 1
        ';
    elsif (p_minmax = 'min') then
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
                base_tb.oid = ' || p_table_oid || '
            )
            select v1.relname as part_name,
                btrim( (regexp_match( v1.l, ''\((.*)\) to \((.*)\)'', ''i'' ))[1], '''''''' )::' || l_type || ' as lval
            from
                v1
            where
                v1.l not like ''%(MAXVALUE)%'' and
                v1.l not like ''%(MINVALUE)%''
            order by 2 asc nulls last
            limit 1
        ';
    end if;
    execute l_sql into l_record;
    return l_record;
end;
$function$
;
