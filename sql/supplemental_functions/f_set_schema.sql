create or replace function mpartman.f_set_schema(
    p_part_name text,
    p_schemaname text
)
 returns void
 language plpgsql
as $function$
declare
  l_r record;
  l_sql text;
begin
  -- Set schema for all subpartitions
  l_sql := 'select part_name from mpartman.v_pt_tree where main_table_name = ''' || p_part_name || '''';
  raise info 'SQL = %', l_sql;
  for l_r in execute l_sql
  loop
      execute 'alter table if exists ' || l_r.part_name || ' set schema ' || p_schemaname;
  end loop;
  -- Set schema for subpartition
  execute 'alter table if exists ' || p_part_name || ' set schema ' || p_schemaname;

  exception
    when others then
    raise exception 'Could not set schema % for partition %! (%, %)', p_schemaname, p_part_name, sqlstate, sqlerrm;

  return;
end;
$function$
;
