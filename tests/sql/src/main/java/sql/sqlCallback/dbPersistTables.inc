hydra.gemfirexd.GfxdHelperPrms-persistTables = true;
hydra.gemfirexd.GfxdHelperPrms-persistDD     = true ;
sql.SQLPrms-testPartitionBy = true;

sql.SQLPrms-redundancyClause =
    " REDUNDANCY ${redundantCopies}"
    " REDUNDANCY ${redundantCopies}"
    " REDUNDANCY ${redundantCopies}"
    " REDUNDANCY ${redundantCopies}"
    " REDUNDANCY ${redundantCopies}"  
    " REDUNDANCY ${redundantCopies}" 
    " REDUNDANCY ${redundantCopies}"
    " REDUNDANCY ${redundantCopies}"
    " REDUNDANCY ${redundantCopies}"
    " REDUNDANCY ${redundantCopies}" ;

sql.SQLPrms-createDiskStore = "create diskstore DBSynchStore 'persistDir'"
      "create diskstore persistSecu 'persistSecu'"
      "create diskstore persistCust   'persistCust' "
      "create diskstore persistNetworth  'persistNetworth' "
      "create diskstore persistPortf  'persistPortf' "
      "create diskstore persistSellorders  'persistSellorders' "
      "create diskstore persistBuyorders  'persistBuyorders' "
      "create diskstore persistTxHistory  'persistTxHistory' ";

sql.SQLPrms-gfePersistExtension = 
	" PERSISTENT SYNCHRONOUS 'persistSecu' " 
    " PERSISTENT SYNCHRONOUS 'persistCust' "
	" PERSISTENT SYNCHRONOUS 'persistNetworth' "
	" PERSISTENT SYNCHRONOUS 'persistPortf'"
	" PERSISTENT SYNCHRONOUS 'persistSellorders' "  
	" PERSISTENT SYNCHRONOUS 'persistBuyorders'" 
	" PERSISTENT SYNCHRONOUS 'persistTxHistory'"
	" "
	" " 
	" ";