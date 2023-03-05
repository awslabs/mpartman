create or replace function mpartman.f_get_allowed_types(
    p_set_name text
)
 returns text[]
 language plpgsql
as $function$
declare
    l_array text[];
    l_error_text text;
    l_message_text text;
    l_context text;
begin
    if ( p_set_name = 'generalrange' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone',
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
                   ]);
    elsif ( p_set_name = 'generallist' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone',
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
                   ]);
    elsif ( p_set_name = 'detachrange' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone',
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
                   ]);
    elsif ( p_set_name = 'detachlist' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone',
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision',
                        'text',
                        'character',
                        'character varying'
                   ]);
    elsif ( p_set_name = 'intervaldatetime' ) then
        l_array := (ARRAY[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone'
                   ]);
    elsif ( p_set_name = 'intervalnumeric' ) then
        l_array := (ARRAY[
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
                   ]);
    else
        l_error_text := 'Unknown name of types set ' || p_set_name || '.';
        raise exception '%', l_error_text;
    end if;

    return l_array;

    exception
        when others then
        get stacked diagnostics l_message_text = MESSAGE_TEXT, l_context = PG_EXCEPTION_CONTEXT;
        raise exception E'% \nCONTEXT: % \n', l_message_text, l_context;
end;
$function$
;
