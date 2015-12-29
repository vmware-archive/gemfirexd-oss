--delete from APP.QUOTE;
--delete from APP.HOLDING;
--delete from APP.ORDERS;
call syscs_util.import_table_ex('APP', 'QUOTE', '<path>/lib/useCase4/QUOTE-100k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
call syscs_util.import_table_ex('APP', 'HOLDING', '<path>/lib/useCase4/HOLDING-100k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
call syscs_util.import_table_ex('APP', 'ACCOUNTPROFILE', '<path>/lib/useCase4/ACCOUNTPROFILE-100k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
call syscs_util.import_table_ex('APP', 'ACCOUNT', '<path>/lib/useCase4/ACCOUNT-100k.dat', ',', NULL, NULL, 0, 0, 6, 0, null, NULL);
call syscs_util.import_table_ex('APP', 'ORDERS', '<path>/lib/useCase4/ORDERS-100k.dat', ',', NULL, NULL, 0, 0, 12, 0, null, NULL);

