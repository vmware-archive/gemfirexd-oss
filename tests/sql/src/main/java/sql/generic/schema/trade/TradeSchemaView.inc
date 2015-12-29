sql.view.ViewPrms-viewDDLFilePath = "sql/view/viewDDL.sql";
sql.view.ViewPrms-viewDataFilePath = "sql/view/viewData.sql";

sql.view.ViewPrms-queryViewsStatements = 
        "select * from trade.securities_vw"   						//query a simple view
        "select * from trade.securities_vw where symbol = 'VMW'"  			//query a simple view using where
        "select * from trade.cust_networth_vw"  					//query a joined view
        "select * from trade.cust_networth_vw where cid in (1001, 2003, 3005, 4007)" 	//query a joined view using joined column in where
        "select * from trade.cust_networth_vw where cust_name = 'Gfxdtest1'" 		//query a joined view using non-joined column in where
        "select * from trade.cust_trade_vw"						
        "select * from trade.emp_trade_vw"   						//query a joined view with base tables from different schemas     
        "select * from trade.cust_emp_vw"						//query a joined view with base tables from different schemas 
        "select * from trade.cust_emp_vw where name in ('Gfxdtest1', 'Gfxdtest3', 'Gfxdtest5')"  //query a joined view with base tables from different schemas using joined column in where
        "select * from trade.cust_emp_vw where ssn > '222334444'"                       //query a joined view with base tables from different schemas using non-joined column in where
        "select * from trade.buy_history_vw"                                          //query a joined view (joined by non-paritioned column, and would not work to partitioned tables)
        "select * from trade.buy_history_vw where ordertime < current_timestamp and cid in (1002, 2004, 3006,4008)"   //query a joined view (joined by non-paritioned column, and would not work to partitioned tables)
        "select * from trade.cust_networth_trade_alltables_vw"				//query a joined view from multiple (three) base tables
        "select name, cash, count(*) from trade.cust_networth_trade_alltables_vw group by name, cash having cash > 5000" 	//query a view using group and having       
        "select * from trade.cust_networth_trade_vw"  					//query a joined view from a view and table
      //"select * from trade.cust_count_since2010_vw where cust_count > 8" 		//query a group view using where -- bug44619
      //"select cust_count from trade.cust_count_since2010_vw group by cust_count having cust_count > 10" //query a group view using group and having -- bug44619
        "select name, trade_count, sum(cash) total_cash, avg(cash) avg_cash, max(cash) max_cash, min(cash) min_cash from trade.cust_tradeCount_with5KNetworth_vw group by name, trade_count having trade_count > 4" 	//query a group view using group and having
        "select * from trade.cust_tradeCount_with5KNetworth_vw order by name asc fetch first row only"
      //"select t1.eid, t1.emp_name, v1.cash from emp.employees t1, trade.cust_tradeCount_with5KNetworth_vw v1 where t1.emp_name = v1.name"   //query a join of a table with a group view -- bug44619
        "select * from trade.cust_networth_trade_vw where name = 'Gfxdtest8'"
	  //"select count(cid) total_customers, sum(cash) total_cash from trade.cust_networth_trade_vw where tradedate > '2007-01-01' group by name having sum(cash) > 20000"  //bug45083
	    "select * from trade.cust_networth_trade_vw v, emp.employees t where v.name=t.emp_name"
	    "select * from trade.cust_networth_trade_vw v where v.name in (select distinct t.emp_name from emp.employees t where t.ssn > '555555555')"
        "select * from trade.cust_tradeCount_with5KNetworth_vw v where v.name in (select distinct t.emp_name from emp.employees t where t.ssn > '555555555')"        
	;	 		
