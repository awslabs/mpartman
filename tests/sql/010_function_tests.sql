select is(:mpschema.f_add_interval('2022-01-01'::date, '2 days'), '2022-01-03'::date, 'Check add interval date');
select is(:mpschema.f_add_interval(18.3::numeric, '5'), 23.3::numeric, 'Check add interval numeric');
select is(:mpschema.f_default_part_has_data((:'testschema' || '.prl')::regclass::oid), true, 'Check is default partitoon has data');
select set_eq(
	'select * from unnest(' || :'mpschema' || '.f_get_allowed_types(''generalrange''))',
	array[
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
        ],
	'Check general range data types'
);
select set_eq(
	'select * from unnest(' || :'mpschema' || '.f_get_allowed_types(''generallist''))',
	array[
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
        ],
	'Check general list data types'
);
select set_eq(
	'select * from unnest(' || :'mpschema' || '.f_get_allowed_types(''detachrange''))',
	array[
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
        ],
	'Check detach range data types'
);
select set_eq(
	'select * from unnest(' || :'mpschema' || '.f_get_allowed_types(''detachlist''))',
	array[
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
        ],
	'Check detach list data types'
);
select set_eq(
	'select * from unnest(' || :'mpschema' || '.f_get_allowed_types(''intervaldatetime''))',
	array[
                        'date',
                        'timestamp without time zone',
                        'timestamp with time zone'
        ],
	'Check interval datetime data types'
);
select set_eq(
	'select * from unnest(' || :'mpschema' || '.f_get_allowed_types(''intervalnumeric''))',
	array[
                        'smallint',
                        'integer',
                        'bigint',
                        'decimal',
                        'numeric',
                        'real',
                        'float',
                        'double precision'
        ],
	'Check general range data types'
);

select is(
	:mpschema.f_get_default_part_name((:'testschema' || '.prl')::regclass::oid),
	(:'testschema' || '.prl_p_default')::text,
	'Get default partition name');

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_add_interval_partitions('''
		|| :'testschema' || '.prl'', ''1 month'', ''2022-03-01''::timestamp, true, true, true)',
        array[ '3' ],
	'Append interval range partitions with MAXVALUE and DEFAULT partition.'
);

select isnt_empty(
	'select part_name from ' || :'mpschema' || '.v_pt_tree where main_table_name like ''' || :'testschema' || '.prl_p_maxvalue%''',
	'Is MAXVALUE partition has sub-partitions'
);

select matches(
	:mpschema.f_get_maxvalue_part_name((:'testschema' || '.prl')::regclass::oid),
	:'testschema' || '.prl_p_maxvalue_.*',
	'Get MAXVALUE partition name'
);

select set_eq(
	'select t.b::text from ' || :'mpschema' || '.f_get_min_max_value_range('''
		|| :'testschema' || '.prl''::regclass::oid, null::timestamp, ''max'') as t(a text, b timestamp)',
        array['2022-03-01 00:00:00'],
	'Get range partition with biggest value not accounting MAXVALUE partition.'
);

select set_eq(
	'select t.a from ' || :'mpschema' || '.f_get_min_max_value_range('''
		|| :'testschema' || '.prl''::regclass::oid, null::timestamp, ''min'') as t(a text, b timestamp)',
        array[:'testschema' || '.prl_p_2022_01'],
	'Get range partition with minimum value not accounting MINVALUE partition.'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_add_interval_partitions('''
		|| :'testschema' || '.prl'', ''-1 month'', ''2021-11-01''::timestamp, true, true, true)',
        array[ '4' ],
	'Prepend interval range partitions to the beginning with MINVALUE and DEFAULT partition.'
);

select matches(
	:mpschema.f_get_minvalue_part_name((:'testschema' || '.prl')::regclass::oid),
	:'testschema' || '.prl_p_minvalue_.*',
	'Get MINVALUE partition name'
);

select matches(
	:mpschema.f_get_overlap_by_range(
		(:'testschema' || '.prl')::regclass::oid,
		'timestamp without time zone',
		'2021-01-01'::timestamp,
		'2021-02-01'::timestamp
	),
	:'testschema' || '.prl_p_minvalue_.*',
	'Check range partition overlapping'
);

select is(:mpschema.f_get_part_column((:'testschema' || '.prl')::regclass::oid), 'loaded_at'::text, 'Get partitioning column name');

select is(
	:mpschema.f_get_range_bvalue(
		(
			select partition_name from :mpschema.f_get_all_part_info((:'testschema' || '.prl'))
			where partition_boundary like '%FROM (''2022-01-01 00:00:00'')%'
		),
		'left'::text
	),
	'2022-01-01 00:00:00'::text,
	'Get left range partition boundary'
);

select is(
	:mpschema.f_get_range_bvalue(
		(
			select partition_name from :mpschema.f_get_all_part_info((:'testschema' || '.prl'))
			where partition_boundary like '%FROM (''2022-01-01 00:00:00'')%'
		),
		'right'::text
	),
	'2022-02-01 00:00:00'::text,
	'Get right range partition boundary'
);

select is(:mpschema.f_has_default_part((:'testschema' || '.prl')::regclass::oid), true, 'Table has default partition');

select is(:mpschema.f_subp_is_eligible(:'testschema' || '.prl_p_2022_01', true), :'testschema' || '.prl_p_2022_01', 'Can table be used as a subpartition template');

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_check_gap_range(''' || :'testschema' || '.prl'')',
        array[ '0' ],
	'Check the gaps in the range partitioned table'
);

select isnt(
	:mpschema.f_find_part_by_value(
		:'testschema' || '.prl',
		false,
		'2022-01-05'::text,
		'WA'::text
	),
	null,
	'Get partition name by partitioning column value (table name as a text)'
);

select isnt(
	:mpschema.f_find_part_by_value(
		(:'testschema' || '.prl')::regclass::oid,
		false,
		'2022-01-05'::text,
		'WA'::text
	),
	null,
	'Get partition name by partitioning column value (table name as an OID)'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_get_all_part_info(''' || :'testschema' || '.prl'')',
        array[ '19' ],
	'Show info about partitioned table'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_add_interval_partitions('''
		|| :'testschema' || '.prl'', ''1 month'', ''2022-03-01''::timestamp, ''2022-04-01''::timestamp, true, false)',
        array[ '0' ],
	'Insert interval range partitions. It is expected to have 0 new partitions because of overlap.'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_do_retention('''
                || :'testschema' || '.prl'', ''new'', ''drop'', ''2023-01-01''::timestamp, false, true)',
        array[ '1' ],
	'Drop all partitions newer than 2023-01-01 including MAXVALUE partition.'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_add_interval_partitions('''
		|| :'testschema' || '.prl'', ''1 month'', ''2022-03-01''::timestamp, ''2023-01-01''::timestamp, true, true)',
        array[ '10' ],
	'Insert interval range partitions.'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_add_partitions('''
		|| :'testschema' || '.prl'', array[''2023-03-01'', ''2023-04-01'', ''2023-05-01'']::timestamp[], true, true)',
        array[ '2' ],
	'Insert an arbitrary set of range partitions.'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_merge_partitions('''
		|| :'testschema' || '.prl'', array['''
		|| :mpschema.f_find_part_by_value(:'testschema' || '.prl', false, '2023-03-01'::text) || ''', ''' 
		|| :mpschema.f_find_part_by_value(:'testschema' || '.prl', false, '2023-04-01'::text) || ''' 
		]::text[], true)',
        array[ '1' ],
	'Merge range partitions.'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_split_partition('''
		|| :mpschema.f_find_part_by_value(:'testschema' || '.prl', false, '2023-03-01'::text) || ''',
		array[''2023-03-01'', ''2023-04-01'']::timestamp[],
		true,
		true,
		true
	)',
        array[ '1' ],
	'Split range partitions.'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_detach_part('''
		|| :'testschema' || '.prl'', ''2022-03-01''::text, true, true)',
        array[ '1' ],
	'Detach partition by value'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_drop_part('''
		|| :'testschema' || '.prl'', ''2022-04-01''::text, true, true)',
        array[ '1' ],
	'Drop partition by value'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_add_partitions('''
		|| :'testschema' || '.prl_numeric'', array[''1.1'', ''2.2'', ''3.3'']::numeric[], true, true)',
        array[ '3' ],
	'Insert an arbitrary set of range partitions (numeric).'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_add_interval_partitions('''
		|| :'testschema' || '.prl_numeric'', ''-1.1'', ''-18''::numeric, true, true, true)',
        array[ '18' ],
	'Prepend interval range partitions (numeric).'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_merge_partitions('''
		|| :'testschema' || '.prl_numeric'', array['''
		|| :mpschema.f_find_part_by_value(:'testschema' || '.prl_numeric', false, '0'::text) || ''', '''
		|| :mpschema.f_find_part_by_value(:'testschema' || '.prl_numeric', false, '1.1'::text) || ''', '''
		|| :mpschema.f_find_part_by_value(:'testschema' || '.prl_numeric', false, '-1.1'::text) || '''
		]::text[], true)',
        array[ '1' ],
	'Merge range partitions (numeric).'
);

select set_eq(
	'select count(*)::text from ' || :'mpschema' || '.f_range_split_partition('''
		|| :mpschema.f_find_part_by_value(:'testschema' || '.prl_numeric', false, '0'::text) || ''',
		array[0, 1.1, -1.1]::numeric[],
		true,
		true,
		true
	)',
        array[ '2' ],
	'Split range partitions (numeric).'
);

