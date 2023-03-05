create or replace function mpartman.f_add_interval(
    p_value anyelement,
    p_interval text
)
 returns anyelement
 language plpgsql
as $function$
declare
    l_value p_value%TYPE;
    l_type text;
    l_sql text;
    l_error_text text;
    l_value_arr text[];
    l_interval_arr text[];
    l_i integer;
begin
    l_type := pg_typeof(p_value);
    if (
        l_type = ANY (mpartman.f_get_allowed_types('intervaldatetime'))
    )
    then
        l_sql := 'select ($1 + ($2)::interval)::' || l_type;
        execute l_sql into l_value using p_value, p_interval;
    elsif (
        l_type = ANY (mpartman.f_get_allowed_types('intervalnumeric'))
    )
    then
        l_sql := 'select ($1 + ($2)::' || l_type || ')::' || l_type;
        execute l_sql into l_value using p_value, p_interval;
    else
        l_error_text := 'Can not add interval. Type ' || l_type || ' is not allowed to use.';
        raise exception '%', l_error_text;
    end if;

    -- Round result according to the parameters precision if float point data type is used
    if ( l_type in ('float', 'double precision', 'decimal', 'numeric', 'real') )
    then
        select string_to_array(p_value::text, '.') into l_value_arr;
        select string_to_array(p_interval, '.') into l_interval_arr;
	select greatest( length(coalesce(l_value_arr[2], ''::text)), length(coalesce(l_interval_arr[2], ''::text)) ) into l_i;
	if (l_i is not null and l_i > 0)
	then
	   l_sql := 'select round(' || l_value::text || '::numeric, ' || l_i::text || '::integer)::' || l_type;
	   execute l_sql into l_value;
	end if;
    end if;
    return l_value;
end;
$function$
;
