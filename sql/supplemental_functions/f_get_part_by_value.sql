create or replace function mpartman.f_get_part_by_value(
    p_table_oid oid,
    p_type text,
    p_value text,
    p_type2 text default null::text,
    p_value2 text default null::text
)
 returns text
 language plpgsql
as $function$
declare
    l_sql text;
    l_col_name text;
    l_col_name2 text;
    l_table_name text;
    r_table_name text;
    l_table_oid oid;
    r_table_oid oid;
    l_where_clause text;
    l_cnt integer;
    l_pstrategy text;
begin
    -- Get the partition column name.
    l_col_name := mpartman.f_get_part_column(p_table_oid);

    -- Get partition strategy for main table
    select vpt.partition_strategy into l_pstrategy
    from mpartman.v_partitioned_tables vpt
    where vpt.oid = p_table_oid;

    if (l_pstrategy = 'hash') then
        r_table_name := mpartman.f_get_hash_part_by_value(p_table_oid, p_type, p_value);
        if (r_table_name is not null) then
            select r_table_name::regclass::oid into r_table_oid;
        end if;
    else

        l_sql := 'select v1.aaa[1], v1.aaa[2] from (select case ';

        -- Find the suitable partition name
        For l_table_name, l_table_oid, l_where_clause in 
        select
            pt.relnamespace::regnamespace || '.' || pt.relname as relname,
            pt.oid,
            pg_get_partition_constraintdef(pt.oid) as wc
        from
            pg_class base_tb, pg_inherits i, pg_class pt
        where
            i.inhparent = base_tb.oid and
            pt.oid = i.inhrelid and
            base_tb.oid = p_table_oid
        Loop
            l_where_clause := regexp_replace(l_where_clause, l_col_name, '''' || p_value || '''::' || p_type, 'g');
            l_sql := l_sql || 'when ' || l_where_clause || ' then array[' || '''' || l_table_name || '''' || ',' || '''' || l_table_oid || '''' || ']';
        End Loop;
        if (l_where_clause is null) then
            return r_table_name;
        end if;
        l_sql := l_sql || ' else array[null::text,null::text] end as aaa ) v1';

        execute l_sql into r_table_name, r_table_oid;

    end if;

    if (p_type2 is null) then
        return r_table_name;
    else

      -- Get partition strategy for a first level partition
      l_pstrategy := null;
      select vpt.partition_strategy into l_pstrategy
      from mpartman.v_partitioned_tables vpt
      where vpt.oid = r_table_oid;

      if (l_pstrategy is null) then
        return r_table_name;
      elsif (l_pstrategy = 'hash') then
        r_table_name := mpartman.f_get_hash_part_by_value(r_table_oid, p_type2, p_value2);
      else

        r_table_name := null;
        -- Get the subpartition column name.
        l_col_name2 := mpartman.f_get_part_column(r_table_oid);
        -- Find the suitable partition name
        l_sql := 'select v1.aaa[1], v1.aaa[2] from (select case ';
        For l_table_name, l_table_oid, l_where_clause in 
        select
            quote_ident(pt.relnamespace::regnamespace::text) || '.' || quote_ident(pt.relname) as relname,
            pt.oid,
            pg_get_partition_constraintdef(pt.oid) as wc
        from
            pg_class base_tb, pg_inherits i, pg_class pt
        where
            i.inhparent = base_tb.oid and
            pt.oid = i.inhrelid and
            base_tb.oid = r_table_oid
        Loop
            l_where_clause := regexp_replace(l_where_clause, l_col_name, '''' || p_value || '''::' || p_type, 'g');
            l_where_clause := regexp_replace(l_where_clause, l_col_name2, '''' || p_value2 || '''::' || p_type2, 'g');
            l_sql := l_sql || 'when ' || l_where_clause || ' then array[' || '''' || l_table_name || '''' || ',' || '''' || l_table_oid || '''' || ']';
        End Loop;
        l_sql := l_sql || ' else array[null::text,null::text] end as aaa ) v1';
        execute l_sql into r_table_name, r_table_oid;

      end if;

      return r_table_name;

    end if;

end;
$function$
;
