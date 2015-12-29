
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_addPortfolioV1Table
	threadGroups = ddlThread;

sql.SQLPrms-portfoliov1Statement = "create table trade.portfoliov1 (cid int not null, sid int not null, qty int not null, availQty int not null, subTotal decimal(30,20), tid int, constraint portf_pk_v1 primary key (cid, sid), constraint cust_fk_v1 foreign key (cid) references trade.customers (cid), constraint sec_fk_v1 foreign key (sid) references trade.securities (sec_id), constraint qty_ck_v1 check (qty>=0), constraint avail_ch_v1 check (availQty>=0 and availQty<=qty))"; 
sql.SQLPrms-dmlTables += "trade.portfoliov1";
sql.SQLPrms-hasPortfolioV1 = true;			