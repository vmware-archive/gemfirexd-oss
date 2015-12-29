delete from APP.TD_INSTRUMENT_SCD;
call syscs_util.import_table_ex('APP', 'TD_INSTRUMENT_SCD', '<path_prefix>/lib/ImportData/TD_INSTRUMENT_SCD.dat', '|', NULL, NULL, 0, 0, 5, 0, 'ImportOra', 'TD_INSTRUMENT_SCD.dat');
