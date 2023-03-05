-- ### Public functions

SELECT function_returns(:'mpschema', 'f_check_gap_range', ARRAY['text'], 'setof record');
SELECT     isnt_definer(:'mpschema', 'f_check_gap_range', ARRAY['text']);

SELECT function_returns(:'mpschema', 'f_detach_part', ARRAY['text','text', 'boolean', 'boolean', 'text'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_detach_part', ARRAY['text','text', 'boolean', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_drop_part', ARRAY['text','text', 'boolean', 'boolean'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_drop_part', ARRAY['text','text', 'boolean', 'boolean']);

SELECT function_returns(:'mpschema', 'f_do_retention', ARRAY['text','text', 'text', 'anyelement', 'boolean', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_do_retention', ARRAY['text','text', 'text', 'anyelement', 'boolean', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_find_part_by_value', ARRAY['text', 'boolean', 'text', 'text'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_find_part_by_value', ARRAY['text', 'boolean', 'text', 'text']);

SELECT function_returns(:'mpschema', 'f_find_part_by_value', ARRAY['oid', 'boolean', 'text', 'text'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_find_part_by_value', ARRAY['oid', 'boolean', 'text', 'text']);

SELECT function_returns(:'mpschema', 'f_get_all_part_info', ARRAY['text'], 'setof record');
SELECT     isnt_definer(:'mpschema', 'f_get_all_part_info', ARRAY['text']);

SELECT function_returns(:'mpschema', 'f_list_add_interval_partitions', ARRAY['text', 'text', 'anyelement', 'boolean', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_list_add_interval_partitions', ARRAY['text', 'text', 'anyelement', 'boolean', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_list_add_partitions', ARRAY['text', 'jsonb', 'boolean', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_list_add_partitions', ARRAY['text', 'jsonb', 'boolean', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_list_merge_partitions', ARRAY['text', 'text[]', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_list_merge_partitions', ARRAY['text', 'text[]', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_list_split_partition', ARRAY['text', 'jsonb', 'boolean', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_list_split_partition', ARRAY['text', 'jsonb', 'boolean', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_range_add_interval_partitions', ARRAY['text', 'text', 'anyelement', 'boolean', 'boolean', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_range_add_interval_partitions', ARRAY['text', 'text', 'anyelement', 'boolean', 'boolean', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_range_add_interval_partitions', ARRAY['text', 'text', 'anyelement', 'anyelement', 'boolean', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_range_add_interval_partitions', ARRAY['text', 'text', 'anyelement', 'anyelement', 'boolean', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_range_add_partitions', ARRAY['text', 'anyarray', 'boolean', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_range_add_partitions', ARRAY['text', 'anyarray', 'boolean', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_range_merge_partitions', ARRAY['text', 'text[]', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_range_merge_partitions', ARRAY['text', 'text[]', 'boolean', 'text']);

SELECT function_returns(:'mpschema', 'f_range_split_partition', ARRAY['text', 'anyarray', 'boolean', 'boolean', 'boolean', 'text'], 'setof text');
SELECT     isnt_definer(:'mpschema', 'f_range_split_partition', ARRAY['text', 'anyarray', 'boolean', 'boolean', 'boolean', 'text']);

-- ### Supplemental functions

SELECT function_returns(:'mpschema', 'f_add_interval', ARRAY['anyelement', 'text'], 'anyelement');
SELECT     isnt_definer(:'mpschema', 'f_add_interval', ARRAY['anyelement', 'text']);

SELECT function_returns(:'mpschema', 'f_default_part_has_data', ARRAY['oid'], 'boolean');
SELECT     isnt_definer(:'mpschema', 'f_default_part_has_data', ARRAY['oid']);

SELECT function_returns(:'mpschema', 'f_get_allowed_types', ARRAY['text'], 'text[]');
SELECT     isnt_definer(:'mpschema', 'f_get_allowed_types', ARRAY['text']);

SELECT function_returns(:'mpschema', 'f_get_default_part_name', ARRAY['oid'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_get_default_part_name', ARRAY['oid']);

SELECT function_returns(:'mpschema', 'f_get_maxvalue_part_name', ARRAY['oid'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_get_maxvalue_part_name', ARRAY['oid']);

SELECT function_returns(:'mpschema', 'f_get_min_max_value_list', ARRAY['oid', 'anyelement', 'text'], 'record');
SELECT     isnt_definer(:'mpschema', 'f_get_min_max_value_list', ARRAY['oid', 'anyelement', 'text']);

SELECT function_returns(:'mpschema', 'f_get_min_max_value_range', ARRAY['oid', 'anyelement', 'text'], 'record');
SELECT     isnt_definer(:'mpschema', 'f_get_min_max_value_range', ARRAY['oid', 'anyelement', 'text']);

SELECT function_returns(:'mpschema', 'f_get_minvalue_part_name', ARRAY['oid'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_get_minvalue_part_name', ARRAY['oid']);

SELECT function_returns(:'mpschema', 'f_get_overlap_by_range', ARRAY['oid', 'text', 'anyelement', 'anyelement', 'text'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_get_overlap_by_range', ARRAY['oid', 'text', 'anyelement', 'anyelement', 'text']);

SELECT function_returns(:'mpschema', 'f_get_part_by_value', ARRAY['oid', 'text', 'text', 'text', 'text'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_get_part_by_value', ARRAY['oid', 'text', 'text', 'text', 'text']);

SELECT function_returns(:'mpschema', 'f_get_part_column', ARRAY['oid'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_get_part_column', ARRAY['oid']);

SELECT function_returns(:'mpschema', 'f_get_range_bvalue', ARRAY['text', 'text'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_get_range_bvalue', ARRAY['text', 'text']);

SELECT function_returns(:'mpschema', 'f_grant_package_privileges', ARRAY['text'], 'void');
SELECT     isnt_definer(:'mpschema', 'f_grant_package_privileges', ARRAY['text']);

SELECT function_returns(:'mpschema', 'f_grant_table_privileges', ARRAY['text', 'text', 'text'], 'void');
SELECT     isnt_definer(:'mpschema', 'f_grant_table_privileges', ARRAY['text', 'text', 'text']);

SELECT function_returns(:'mpschema', 'f_has_default_part', ARRAY['oid'], 'boolean');
SELECT     isnt_definer(:'mpschema', 'f_has_default_part', ARRAY['oid']);

SELECT function_returns(:'mpschema', 'f_is_eligible', ARRAY['text'], 'oid');
SELECT     isnt_definer(:'mpschema', 'f_is_eligible', ARRAY['text']);

SELECT function_returns(:'mpschema', 'f_is_eligible_for_detach', ARRAY['text'], 'oid');
SELECT     isnt_definer(:'mpschema', 'f_is_eligible_for_detach', ARRAY['text']);

SELECT function_returns(:'mpschema', 'f_set_config', ARRAY['text', 'text', 'boolean'], 'void');
SELECT       is_definer(:'mpschema', 'f_set_config', ARRAY['text', 'text', 'boolean']);

SELECT function_returns(:'mpschema', 'f_set_part_name', ARRAY['text', 'text', 'text'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_set_part_name', ARRAY['text', 'text', 'text']);

SELECT function_returns(:'mpschema', 'f_subp_is_eligible', ARRAY['text', 'boolean'], 'text');
SELECT     isnt_definer(:'mpschema', 'f_subp_is_eligible', ARRAY['text', 'boolean']);

