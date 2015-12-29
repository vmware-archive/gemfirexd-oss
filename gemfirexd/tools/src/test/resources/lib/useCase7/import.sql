set schema APP;

truncate table C_BO_POSTAL_ADDR;
truncate table C_BO_PRTY;

-- call syscs_util.import_table_ex('SEC_OWNER', 'DATAHINTS', '/soubhikc1/poc/useCase1/dataGenerator/DATAHINTS.dat', ',', NULL, NULL, 0, 0, 1, 0, null, NULL);
--call syscs_util.import_data_ex('APP', 'C_BO_PRTY', null,null, '/soubhikc1/poc/useCase7/datagenerator/C_BO_PRTY-4m.dat', ',', NULL, NULL, 0, 0, 6, 0, null, null);
--call syscs_util.import_data_ex('APP', 'C_BO_POSTAL_ADDR', null,null, '/soubhikc1/poc/useCase7/datagenerator/C_BO_POSTAL_ADDR-4m.dat', ',', NULL, NULL, 0, 0, 6, 0, null, null);

 call syscs_util.import_data_ex('APP', 'C_BO_PRTY', null,null, '<path_prefix>/lib/useCase7/C_BO_PRTY.dat', ',', NULL, NULL, 0, 0, 6, 0, null, null);
 call syscs_util.import_data_ex('APP', 'C_BO_POSTAL_ADDR', null,null, '<path_prefix>/lib/useCase7/C_BO_POSTAL_ADDR.dat', ',', NULL, NULL, 0, 0, 6, 0, null, null);
