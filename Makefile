EXTENSION = mpartman
DATA = mpartman--0.1.0.sql mpartman--0.1.0--0.1.1.sql
 
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
