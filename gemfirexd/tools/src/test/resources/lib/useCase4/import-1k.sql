--delete from APP.QUOTE;
--delete from APP.HOLDING;
--delete from APP.ORDERS;
call syscs_util.import_table_ex('APP', 'QUOTE', '<path_prefix>/lib/useCase4/QUOTE-1k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
call syscs_util.import_table_ex('APP', 'HOLDING', '<path_prefix>/lib/useCase4/HOLDING-1k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
call syscs_util.import_table_ex('APP', 'ACCOUNTPROFILE', '<path_prefix>/lib/useCase4/ACCOUNTPROFILE-1k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
call syscs_util.import_table_ex('APP', 'ACCOUNT', '<path_prefix>/lib/useCase4/ACCOUNT-1k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
call syscs_util.import_table_ex('APP', 'ORDERS', '<path_prefix>/lib/useCase4/ORDERS-1k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
