0. Change 02.gfxdimport_egen_data.sql to set right data file location, data converter jar file and database connection information
1. Create TPCE schema from 01.tpce_1.12.0_fast_load.sql
2. Load the generated TPCE data to GemFireXD (02.gfxdimport_egen_data.sql) using import_table (need customize the import class for clob, see implementation in 3.MigrateDB)
3. Add the table constraints from 03.tpce_1.12.0_add_constraints.sql.
3. See versions of TPCE schemas which evolved with development/testing to accommodate/workaround the limitations from GemFireXD.
