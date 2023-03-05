create or replace function mpartman.f_grant_table_privileges(
    p_schema text,
    p_table_name text,
    p_part_name text
)
 returns void
 language plpgsql
as $function$
declare
    l_record record;
begin

  for l_record in
  select
    format (
      'grant %s on table %I.%I to %I%s',
      string_agg(tg.privilege_type, ', '),
      p_schema,
      p_part_name,
      tg.grantee,
      case
        when tg.is_grantable = 'YES'
        then ' WITH GRANT OPTION'
        else ''
      end
    ) as grantsql
  from information_schema.role_table_grants tg
  join pg_tables t on t.schemaname = tg.table_schema and t.tablename = tg.table_name
  where
    tg.table_schema = p_schema
    and tg.table_name = p_table_name
--    and t.tableowner <> tg.grantee
  group by tg.table_schema, tg.table_name, tg.grantee, tg.is_grantable
  loop
    execute l_record.grantsql;
  end loop;

  exception
    when others then
    raise warning 'Could not grant privileges to %.%! (% %)', p_schema, p_part_name, sqlstate, sqlerrm;

  return;
end;
$function$
;
