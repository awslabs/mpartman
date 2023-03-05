create or replace function mpartman.f_set_config(
    p_name text,
    p_setting text,
    p_is_local boolean
)
 returns void
 language plpgsql
 security definer
as $function$
begin
  -- check available parameter names
  if (
	p_name not in (
		'session_replication_role'
	)
  ) then
    raise exception 'There is unavailable parameter to set %!', p_name;
  end if;

  -- check whether we set parameter for current transaction only
  if (not p_is_local) then
    raise exception 'It is allowed to set parameter % for current transaction only!', p_name;
  end if;

  perform set_config(p_name, p_setting, p_is_local);

  exception
    when others then
    raise warning 'Could not set parameter % to %! (% %)', p_name, p_setting, sqlstate, sqlerrm;

  return;
end;
$function$
;
