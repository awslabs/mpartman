create or replace function mpartman.f_grant_package_privileges(
    p_user text
)
 returns void
 language plpgsql
as $function$
begin

  execute 'grant usage on schema mpartman to ' || p_user;
  execute 'grant execute on all functions in schema mpartman to ' || p_user;
  execute 'grant select on all tables in schema mpartman to ' || p_user;

  exception
    when others then
    raise warning 'Could not grant privileges to %! (% %)', p_user, sqlstate, sqlerrm;

  return;
end;
$function$
;
