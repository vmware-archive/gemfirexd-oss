set schema SEC_OWNER;

delete from DATAHINTS;
delete from SECT_CHANNEL_DATA;
delete from SECT_CHANNEL_RAW_DATA;


call syscs_util.import_table_ex('SEC_OWNER', 'DATAHINTS', '/soubhikc1/poc/useCase1/dataGenerator/DATAHINTS.dat', ',', NULL, NULL, 0, 0, 1, 0, null, NULL);
call syscs_util.import_data_ex('SEC_OWNER', 'SECT_CHANNEL_DATA', null,null, '/soubhikc1/poc/useCase1/dataGenerator/SECT_CHANNEL_DATA.dat', ',', NULL, NULL, 0, 0, 6, 0, null, null);
call syscs_util.import_data_ex('SEC_OWNER', 'SECT_CHANNEL_RAW_DATA', 'CHANNEL_TXN_ID,CHN_RAW_DATA,ERROR_CODE',null, '/soubhikc1/poc/useCase1/dataGenerator/SECT_CHANNEL_RAW_DATA.dat', ',', NULL, NULL, 0, 0, 6, 0, null, null);

