select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_add_partitions('''
                || :'testschema' || '.tb_list_decimal'', ''[50]''::jsonb, true, true)',
        array[ '2' ],
        'Insert an arbitrary set of list partitions (decimal).'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_add_interval_partitions('''
                || :'testschema' || '.tb_list_decimal'', ''7.541'', 100::decimal, true, true)',
        array[ '7' ],
        'Insert "interval" list partitions (decimal).'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_add_partitions('''
                || :'testschema' || '.tb_list_decimal'', ''[-1,0.1,1,-2.6,3,7,500]''::jsonb, true, true)',
        array[ '8' ],
        'Insert an arbitrary set of list partitions (decimal).'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_add_partitions('''
                || :'testschema' || '.tb_list_decimal'', ''[-1,0.1,1,-2.6,3,7,500]''::jsonb, true, true)',
        array[ '1' ],
        'Insert the same set of list partitions (decimal).'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_merge_partitions('''
                || :'testschema' || '.tb_list_decimal'', array['''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_decimal', false, '-1'::text) || ''', '''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_decimal', false, '-2.6'::text) || ''', '''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_decimal', false, '7'::text) || ''', '''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_decimal', false, '0.1'::text) || '''
                ]::text[], true)',
        array[ '1' ],
        'Merge list partitions (decimal).'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_split_partition('''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_decimal', false, '-2.6'::text) || ''',
                ''[-2.6, 7, 0.1, -1]''::jsonb,
                true,
                true
        )',
        array[ '5' ],
        'Split list partitions (decimal).'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_add_partitions('''
                || :'testschema' || '.tb_list_text'', ''["One","gREEN","John Dow","Petrol"]''::jsonb, true, true)',
        array[ '4' ],
        'Insert an arbitrary set of list partitions (text).'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_merge_partitions('''
                || :'testschema' || '.tb_list_text'', array['''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_text', false, 'Petrol'::text) || ''', '''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_text', false, 'gREEN'::text) || ''', '''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_text', false, 'John Dow'::text) || '''
                ]::text[], true)',
        array[ '1' ],
        'Merge list partitions (text).'
);

select set_eq(
        'select count(*)::text from ' || :'mpschema' || '.f_list_split_partition('''
                || :mpschema.f_find_part_by_value(:'testschema' || '.tb_list_text', false, 'Petrol'::text) || ''',
                ''["gREEN","John Dow","Petrol"]''::jsonb,
                true,
                true
        )',
        array[ '3' ],
        'Split list partitions (text).'
);

