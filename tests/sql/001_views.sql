SELECT has_schema(:'mpschema', 'Schema ' || :'mpschema' || ' exists.');
SELECT has_view(:'mpschema', 'v_partitioned_tables', 'View ' || :'mpschema' || '.v_partitioned_tables exists');
SELECT has_view(:'mpschema', 'v_pt_tree', 'View ' || :'mpschema' || '.v_pt_tree exists');
