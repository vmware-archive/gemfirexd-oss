TRUNCATE TABLE APP.ITEM_NOTA_FISCAL;
TRUNCATE TABLE APP.NOTA_FISCAL;
TRUNCATE TABLE APP.TEMPO;
TRUNCATE TABLE APP.VENDEDOR;
TRUNCATE TABLE APP.REVENDA;
TRUNCATE TABLE APP.PRODUTO;
TRUNCATE TABLE APP.TIPO_PRODUTO;
TRUNCATE TABLE APP.CLIENTE;

CALL SYSCS_UTIL.IMPORT_TABLE ( 'APP','CLIENTE', '/Users/asifs/eclipse/workspace/GemfirexdNov10/gemfirexd/GemFireXDTests/lib/useCase10/dm_cliente.txt', ';', null, null, 0);
CALL SYSCS_UTIL.IMPORT_TABLE ( 'APP','TIPO_PRODUTO', '/Users/asifs/eclipse/workspace/GemfirexdNov10/gemfirexd/GemFireXDTests/lib/useCase10/dm_tipo_prod.txt', ';', null, null, 0);
CALL SYSCS_UTIL.IMPORT_TABLE ( 'APP','PRODUTO', '/Users/asifs/eclipse/workspace/GemfirexdNov10/gemfirexd/GemFireXDTests/lib/useCase10/dm_prod.txt', ';', null, null, 0);
CALL SYSCS_UTIL.IMPORT_TABLE ( 'APP','REVENDA', '/Users/asifs/eclipse/workspace/GemfirexdNov10/gemfirexd/GemFireXDTests/lib/useCase10/dm_revenda.txt', ';', null, null, 0);
CALL SYSCS_UTIL.IMPORT_TABLE ( 'APP','VENDEDOR', '/Users/asifs/eclipse/workspace/GemfirexdNov10/gemfirexd/GemFireXDTests/lib/useCase10/dm_vendedor.txt', ';', null, null, 0);
CALL SYSCS_UTIL.IMPORT_TABLE ( 'APP','TEMPO', '/Users/asifs/eclipse/workspace/GemfirexdNov10/gemfirexd/GemFireXDTests/lib/useCase10/dm_tempo.txt', ';', null, null, 0);
CALL SYSCS_UTIL.IMPORT_TABLE ( 'APP','NOTA_FISCAL', '/Users/asifs/eclipse/workspace/GemfirexdNov10/gemfirexd/GemFireXDTests/lib/useCase10/dm_nf.txt', ';', null, null, 0);
CALL SYSCS_UTIL.IMPORT_TABLE ( 'APP','ITEM_NOTA_FISCAL', '/Users/asifs/eclipse/workspace/GemfirexdNov10/gemfirexd/GemFireXDTests/lib/useCase10/dm_item_nf.txt', ';', null, null, 0);

