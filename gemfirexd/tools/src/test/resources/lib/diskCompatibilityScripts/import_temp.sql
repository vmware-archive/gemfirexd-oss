-- import
call syscs_util.import_table ('APP', 'CONTEXT_HISTORY_TEMP','/varuna1/sample_code/csv/CONTEXT_HISTORY.file', null, null, null, 0);
call syscs_util.import_table ('APP', 'CHART_HISTORY_TEMP','/varuna1/sample_code/csv/CHART_HISTORY.file', null, null, null, 0);
call syscs_util.import_table ('APP', 'RECEIVED_HISTORY_LOG_TEMP','/varuna1/sample_code/csv/RECEIVED_HISTORY_LOG.file', null, null, null, 0);
