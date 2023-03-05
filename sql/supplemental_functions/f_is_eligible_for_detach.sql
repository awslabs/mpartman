create or replace function mpartman.f_is_eligible_for_detach(
    p_table_name text
)
 returns oid
 language plpgsql
as $function$
declare
    l_i integer;
    l_relkind text;
    l_table_oid oid;
    l_is_partition boolean;
    l_error_text text;
    l_warn_text text;
    l_namespace text;
    l_relname text;
begin
    if (p_table_name is null) then
        return null::oid;
    end if;
    -- Get table oid. Table name can be schema qualified or not.
    begin
        select
            pc.oid, pc.relnamespace::regnamespace, pc.relname, pc.relispartition, pc.relkind
        into strict
            l_table_oid, l_namespace, l_relname, l_is_partition, l_relkind
        from
            pg_class pc
        where
            pc.oid = array_to_string(array(select quote_ident(unnest(parse_ident(p_table_name, true)))), '.')::regclass::oid
        ;
            --pc.oid = array_to_string(parse_ident(p_table_name, true),'.')::regclass
    exception
        when no_data_found then
            raise warning 'Table % does not exist!', p_table_name;
            return null::oid;
        when too_many_rows then
            raise warning 'Table name % is not unique. Use schema to qualify a table!', p_table_name;
            return null::oid;
        when others then
            raise warning '%; sqlstate: %', sqlerrm, sqlstate;
            return null::oid;
    end;

    -- Validate whether the table is partitioned and fit the automated rules.

    -- Is it partitioned?
    if (l_relkind is null or l_relkind <> 'p') then
        l_table_oid := null;
        l_error_text := 'Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown') || ' is not partitioned.';
    end if;

    -- Check DEFAULT partitions
    if (l_table_oid is not null) then
            if (mpartman.f_default_part_has_data(l_table_oid)) then
                l_warn_text := 'Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown') || ' has default partition with data.';
                raise warning '%', l_warn_text;
            end if;
    end if;

    -- Validate partitioning conditions
    if (l_table_oid is not null) then
        select
            count(*)
        into
            l_i
        from
            mpartman.v_partitioned_tables vpt
        where
            vpt.oid = l_table_oid and
            vpt.inhparent is null and
            vpt.inhrelid is null
        ;
    end if;

    -- It is partitoned by more than one column
    if (l_table_oid is not null and l_i > 1) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' is partitioned by more than one column.';
    end if;

    -- It is not a high level partitioned table
    if (l_table_oid is not null and l_i = 0) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' is not a high level partitioned table.';
    end if;

    -- Validate partition strategy and columns types
    if (l_table_oid is not null) then
        select
            count(*)
        into
            l_i
        from
            mpartman.v_partitioned_tables vpt
        where
            vpt.oid = l_table_oid and
            vpt.inhparent is null and
            vpt.inhrelid is null and
            (
                (
                    vpt.partition_strategy = 'range' and
                    vpt.part_col_data_type = ANY (mpartman.f_get_allowed_types('detachrange'))
                )
                or
                (
                    vpt.partition_strategy = 'list' and
                    vpt.part_col_data_type = ANY (mpartman.f_get_allowed_types('detachlist'))
                )
            )
        ;
    end if;

    -- Wrong combination of partition strategy and columns types
    if (l_table_oid is not null and l_i = 0) then
        l_table_oid := null;
        l_error_text := coalesce(l_error_text, '') || ' Table ' || coalesce(l_namespace, 'unknown') || '.' || coalesce(l_relname, 'unknown')
                        || ' has a not suitable combination of partition strategy and columns types.';
    end if;

    -- Raise error
    if (l_error_text is not null) then
        raise warning '%', l_error_text;
        return null::oid;
    end if;

    return l_table_oid;
end;
$function$
;
