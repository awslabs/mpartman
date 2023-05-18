select is(
	:mpschema.f_get_hash_part_by_value(
		(:'testschema' || '.tb_hash_bigint')::regclass::oid, 
		'bigint'::text, 
		'13'::text
	), 
	:'testschema' || '.tb_hash_bigint_p_2', 
	'Get partition name by hash value');

