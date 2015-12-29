
sql.generic.SQLGenericPrms-genericDmlOperations =
//groups for buyorders
	"select * from trade.buyorders where status = 'open' and tid = ::trade.buyorders.tid::"
    "select sid, count(*) from trade.buyorders  where status =::trade.buyorders.status:: and tid =::trade.buyorders.tid:: GROUP BY sid HAVING count(*) >=1"
    "select cid, bid, cid, sid from trade.buyorders where cid > ::trade.buyorders.cid:: and sid < ::trade.buyorders.sid::  and qty > ::trade.buyorders.qty:: and orderTime< ::trade.buyorders.ordertime:: and tid = ::trade.buyorders.tid::"
    ,
    
    "select cid, min(qty*bid) as smallest_order from trade.buyorders  where status =::trade.buyorders.status:: and tid =::trade.buyorders.tid:: GROUP BY cid"
    "select cid, cast (avg(qty*bid) as decimal (30, 20)) as amount from trade.buyorders  where status = ::trade.buyorders.status:: and tid = ::trade.buyorders.tid:: GROUP BY cid ORDER BY amount"
    "select cid, max(qty*bid) as largest_order from trade.buyorders  where status = ::trade.buyorders.status:: and tid = ::trade.buyorders.tid::  GROUP BY cid HAVING max(qty*bid) > 2000 ORDER BY largest_order DESC, cid DESC "
    ,
    
    "select * from trade.buyorders where sid = ::trade.buyorders.sid:: and tid = ::trade.buyorders.tid::"
    "update trade.buyorders set status = 'filled'  where sid = ::trade.buyorders.sid:: and bid > ::trade.buyorders.bid:: and status = 'open' and tid = ::trade.buyorders.tid::"
    "update trade.buyorders set bid = ::trade.buyorders.bid:: where cid = ::trade.buyorders.cid:: and sid = ::trade.buyorders.sid:: and status = 'open' and qty > ::trade.buyorders.qty:: and tid = ::trade.buyorders.tid::"
    "select * from trade.buyorders where sid = ::trade.buyorders.sid:: and tid = ::trade.buyorders.tid::"
    ,
    
    "update trade.buyorders set status = 'cancelled' where (ordertime > ::trade.buyorders.ordertime:: or sid = ::trade.buyorders.sid::) and status = 'open' and tid = ::trade.buyorders.tid:: "    
    "select status from trade.buyorders where (ordertime > ::trade.buyorders.ordertime:: or sid = ::trade.buyorders.sid::) and tid = ::trade.buyorders.tid::"
    "update trade.buyorders set bid = ::trade.buyorders.bid:: , qty=::trade.buyorders.qty:: where cid = ::trade.buyorders.cid:: and sid= ::trade.buyorders.sid:: and bid <::trade.buyorders.bid:: and status = 'open' and tid =::trade.buyorders.tid:: "
    "select bid, qty from trade.buyorders where cid = ::trade.buyorders.cid:: and sid= ::trade.buyorders.sid:: and bid <::trade.buyorders.bid:: and status = 'open' and tid =::trade.buyorders.tid:: "
    "update trade.buyorders set sid = ::trade.buyorders.sid:: where cid = ::trade.buyorders.cid:: and sid= ::trade.buyorders.sid:: and status = 'open' and tid =::trade.buyorders.tid:: "
    "select sid, status, bid, qty from trade.buyorders where cid = ::trade.buyorders.cid:: and status = 'open' and tid =::trade.buyorders.tid::"
    ,    
    
    "select * from trade.buyorders where cid=::trade.buyorders.cid:: and sid=::trade.buyorders.sid:: and tid=::trade.buyorders.tid::"
    "delete from trade.buyorders where cid=::trade.buyorders.cid:: and sid=::trade.buyorders.sid:: and tid=::trade.buyorders.tid::"
    "select * from trade.buyorders where cid=::trade.buyorders.cid:: and sid=::trade.buyorders.sid:: and tid=::trade.buyorders.tid::"
    ,    
    
    "delete from trade.buyorders where oid=::trade.buyorders.oid:: and (bid <::trade.buyorders.bid:: or qty>::trade.buyorders.qty::) and tid=::trade.buyorders.tid::"
    "select * from trade.buyorders where oid=::trade.buyorders.oid:: and (bid <::trade.buyorders.bid:: or qty>::trade.buyorders.qty::) and tid=::trade.buyorders.tid::"
    "delete from trade.buyorders where (status ='cancelled' or status = 'filled' or qty<::trade.buyorders.qty::) and bid>::trade.buyorders.bid:: and tid=::trade.buyorders.tid::"    
    "select * from trade.buyorders where (status ='cancelled' or status = 'filled' or qty<::trade.buyorders.qty::) and bid>::trade.buyorders.bid:: and tid=::trade.buyorders.tid::"
    ,
    
//group buyorders - join
//    "select b1.oid, b1.cid, b1.sid, b1.qty, b1.tid, b2.oid, b2.cid, b2.sid, b2.qty, b2.tid from trade.buyorders b1, trade.buyorders b2 where b1.cid = b2.cid and b1.tid = ::trade.buyorders.tid:: and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid",
    
//groups for securities
    "select sec_id, symbol, price, exchange, tid from trade.securities where tid = ::trade.securities.tid::"
	"update trade.securities set price = ::trade.securities.price:: where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"select sec_id, price from trade.securities where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"delete from trade.securities where (sec_id = ::trade.securities.sec_id:: or price = ::trade.securities.price:: ) and tid = ::trade.securities.tid::"
	"select * from trade.securities where (sec_id = ::trade.securities.sec_id:: or price = ::trade.securities.price:: ) and tid = ::trade.securities.tid::"
	,

	"select cast(avg( price) as decimal (30, 20)) as avg_price from trade.securities where tid= ::trade.securities.tid:: and symbol >::trade.securities.symbol::"
	"update trade.securities set symbol = ::trade.securities.symbol:: where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"select symbol from trade.securities where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"delete from trade.securities where (symbol= ::trade.securities.symbol:: and exchange = ::trade.securities.exchange:: ) and tid = ::trade.securities.tid::"
	"select * from trade.securities where (symbol= ::trade.securities.symbol:: and exchange = ::trade.securities.exchange:: ) and tid = ::trade.securities.tid::"
	,	
	
	"select sec_id , price, symbol, exchange from trade.securities where (price > ::trade.securities.price:: or price <=::trade.securities.price > $1::) and tid =::trade.securities.tid::"
	"update trade.securities set symbol = ::trade.securities.symbol::, exchange =::trade.securities.exchange:: where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"select sec_id , price, symbol, exchange from trade.securities where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"delete from trade.securities where sec_id=::trade.securities.sec_id:: and tid = ::trade.securities.tid::" 
	"select * from trade.securities where sec_id=::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	,

	"select sec_id, symbol, price, exchange from trade.securities  where (price > ::trade.securities.price:: or price <=::trade.securities.price > $1::) and exchange =::trade.securities.exchange:: and tid =::trade.securities.tid::"
	"select sec_id, symbol, price, exchange, tid from trade.securities where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"select sec_id, price, symbol from trade.securities where symbol >::trade.securities.symbol:: and tid = ::trade.securities.tid::"
	"select price, symbol, exchange from trade.securities where (price > ::trade.securities.price:: or price <=::trade.securities.price < $1::)  and tid = ::trade.securities.tid::"
	"select sec_id, symbol, price, exchange from trade.securities  where (price > ::trade.securities.price:: or price <=::trade.securities.price < $1::) and exchange =::trade.securities.exchange:: and  tid = ::trade.securities.tid::"
	,	

	"update trade.securities set exchange =::trade.securities.exchange:: where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"select sec_id, symbol, price, exchange, tid from trade.securities where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"update trade.securities set price = ::trade.securities.price:: where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"select sec_id, symbol, price, exchange, tid from trade.securities where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"	
	"update trade.securities set symbol = ::trade.securities.symbol:: where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	"select sec_id, symbol, price, exchange, tid from trade.securities where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	,
	
	"insert into trade.securities values (::trade.securities.sec_id::,::trade.securities.symbol::,::trade.securities.price::,::trade.securities.exchange::,::trade.securities.tid::)"
	"update trade.securities set exchange =::trade.securities.exchange:: where sec_id = ::trade.securities.sec_id::  and tid = ::trade.securities.tid::"
	"select sec_id, symbol, price, exchange, tid from trade.securities where sec_id = ::trade.securities.sec_id:: and tid = ::trade.securities.tid::"
	,	

	"insert into trade.securities values (::trade.securities.sec_id::,::trade.securities.symbol::,::trade.securities.price::,::trade.securities.exchange::,::trade.securities.tid::)"
    "insert into trade.securities (sec_id,symbol,price,exchange,tid) values (::trade.securities.sec_id::,::trade.securities.symbol::,::trade.securities.price::,::trade.securities.exchange::,::trade.securities.tid::)"
    ,    
    
    "insert into trade.securities (sec_id,symbol,price,exchange,tid) values (::trade.securities.sec_id::,::trade.securities.symbol::,::trade.securities.price::,::trade.securities.exchange::,::trade.securities.tid::)"
    "update trade.securities set exchange =::trade.securities.exchange:: where sec_id = ::trade.securities.sec_id::  and tid = ::trade.securities.tid::"
    "delete from trade.securities where sec_id = ::trade.securities.sec_id:: and exchange =::trade.securities.exchange:: and tid = ::trade.securities.tid::"
    "select sec_id, symbol, price, exchange, tid from trade.securities where sec_id = ::trade.securities.sec_id:: and exchange =::trade.securities.exchange:: and tid = ::trade.securities.tid::"
    ,

    "update trade.securities set price=20 where tid=::trade.securities.tid::"
    "select price from trade.securities where tid = ::trade.securities.tid::"
    ,  
        
// groups for customers
    "insert into trade.customers(cid, cust_name, since, addr, tid) values (::trade.customers.cid:: , ::trade.customers.cust_name::, ::trade.customers.since:: , ::trade.customers.addr::, ::trade.customers.tid::)"
	"insert into trade.customers values (::trade.customers.cid:: , ::trade.customers.cust_name::, ::trade.customers.since:: , ::trade.customers.addr::, ::trade.customers.tid::)"
	,
	
	"select * from trade.customers where tid = ::trade.customers.tid::"
	"update trade.customers set cid = ::trade.customers.cid:: where cid=::trade.customers.cid:: and tid = ::trade.customers.tid:: "
	"select cid, tid from trade.customers where cid=::trade.customers.cid:: and tid = ::trade.customers.tid::"	 	 
    "update trade.customers set cust_name = ::trade.customers.cust_name:: , addr = ::trade.customers.addr:: where cid=::trade.customers.cid:: and tid =::trade.customers.tid::" 
    "select cid, cust_name, tid from trade.customers where cid=::trade.customers.cid:: and tid =::trade.customers.tid::"
    ,
    
    "select cid, since, cust_name from trade.customers where tid=::trade.customers.tid:: and cid >::trade.customers.cid::"
    "update trade.customers set cust_name = ::trade.customers.cust_name::, since =::trade.customers.since:: where cid=::trade.customers.cid:: and tid =::trade.customers.tid::"
    "select cid, since, cust_name, tid from trade.customers where cid=::trade.customers.cid:: and tid =::trade.customers.tid::"
    "delete from trade.customers where (cust_name = ::trade.customers.cust_name:: or cid = ::trade.customers.cid:: ) and tid = ::trade.customers.tid::"
    "select cid, since, cust_name, tid from trade.customers where (cust_name = ::trade.customers.cust_name:: or cid = ::trade.customers.cid:: ) and tid = ::trade.customers.tid::"
    ,
    
    "select since, addr, cust_name from trade.customers where (tid<::trade.customers.tid:: or cid <=::trade.customers.cid::) and since >::trade.customers.since:: and tid = ::trade.customers.tid::"
    "select addr, since, cust_name from trade.customers where (cid >::trade.customers.cid:: or since <::trade.customers.since::) and tid = ::trade.customers.tid::"
    "delete from trade.customers where cid=::trade.customers.cid:: and tid=::trade.customers.tid::"
    "select cid, since, cust_name, tid from trade.customers where cid=::trade.customers.cid:: and tid=::trade.customers.tid::"
    ,
    
    "insert into trade.customers(cid, cust_name, since, addr, tid) values (::trade.customers.cid:: , ::trade.customers.cust_name::, ::trade.customers.since:: , ::trade.customers.addr::, ::trade.customers.tid::)"
    "update trade.customers set cust_name = ::trade.customers.cust_name::, since =::trade.customers.since:: where cid=::trade.customers.cid:: and tid =::trade.customers.tid::"
    "delete from trade.customers where cid=::trade.customers.cid:: and tid=::trade.customers.tid::"
    "update trade.customers set cust_name = ::trade.customers.cust_name::, since =::trade.customers.since:: where cid=::trade.customers.cid:: and tid =::trade.customers.tid::"
    "select cid, since, cust_name, tid from trade.customers where cid=::trade.customers.cid:: and tid=::trade.customers.tid::"
    ,

// groups for networth  
	"insert into trade.networth(cid,cash,securities,loanLimit,availLoan,tid) values (::trade.networth.cid::,::trade.networth.cash::,::trade.networth.securities::,::trade.networth.loanlimit::,::trade.networth.availloan::,::trade.networth.tid::)"
	"insert into trade.networth values (::trade.networth.cid::,::trade.networth.cash::,::trade.networth.securities::,::trade.networth.loanlimit::,::trade.networth.availloan::,::trade.networth.tid::)"
	,
	
	"select * from trade.networth where tid = ::trade.networth.tid::"
    "update trade.networth set availloan=availloan-1 where cid = ::trade.networth.cid:: and tid= ::trade.networth.tid::"
    "select availloan from trade.networth where cid = ::trade.networth.cid:: and tid= ::trade.networth.tid::"
    ,
    
    "select cid, (cash + securities - (loanLimit - availloan)) as networth from trade.networth where tid = ::trade.networth.tid::"
    "select cid, (cash + securities - (loanLimit - availloan))as networth from trade.networth where tid = ::trade.networth.tid:: order by networth desc, cid"
    "update trade.networth set securities=::trade.networth.securities:: where cid = ::trade.networth.cid::  and tid= ::trade.networth.tid::"
    "update trade.networth set cash=cash-::trade.networth.cash:: where cid = ::trade.networth.cid:: and tid= ::trade.networth.tid::" 
    "select cid, cash, securities, availloan from trade.networth where cid = ::trade.networth.cid:: and tid= ::trade.networth.tid::"
    ,
    
    "select cid, loanlimit, availloan from trade.networth where (loanlimit >::trade.networth.loanlimit:: and loanlimit-availloan <= ::trade.networth.loanlimit::) and tid=::trade.networth.tid::"
    "select cid, cash, securities from trade.networth where (cash<::trade.networth.cash:: or securities >=::trade.networth.securities::) and tid =::trade.networth.tid::"
    "update trade.networth set loanLimit=::trade.networth.loanlimit:: where  tid= ::trade.networth.tid:: and securities > ::trade.networth.securities::"
	"select cid, cash, securities, availloan from trade.networth where tid= ::trade.networth.tid::"
    ,
    
    "select * from trade.networth where (cash > loanLimit - availloan) and tid =::trade.networth.tid::",
    "select cash, securities, loanlimit, cid, availloan from trade.networth where (cash <=securities or cash > loanLimit - availloan) and tid =::trade.networth.tid::",
    "select securities, cash, availloan, loanlimit, cid, availloan from trade.networth where (availloan >=securities or cash > loanLimit - availloan) and tid = ::trade.networth.tid::"
    "delete from trade.networth where cid = ::trade.networth.cid:: and tid = ::trade.networth.tid::"
    "select * from trade.networth where cid = ::trade.networth.cid:: and tid = ::trade.networth.tid::"
    ,
    
    "insert into trade.networth(cid,cash,securities,loanLimit,availLoan,tid) values (::trade.networth.cid::,::trade.networth.cash::,::trade.networth.securities::,::trade.networth.loanlimit::,::trade.networth.availloan::,::trade.networth.tid::)"
    "delete from trade.networth where cid = ::trade.networth.cid:: and tid = ::trade.networth.tid::"
    "update trade.networth set securities=::trade.networth.securities:: where cid = ::trade.networth.cid::  and tid= ::trade.networth.tid::"
    "select * from trade.networth where cid = ::trade.networth.cid:: and tid = ::trade.networth.tid::"

// groups for portfolio
	"insert into trade.portfolio values (::trade.portfolio.cid::,::trade.portfolio.sid::,::trade.portfolio.qty::,::trade.portfolio.availqty::,::trade.portfolio.subtotal::, ::trade.portfolio.tid::)"
	"insert into trade.portfolio(cid, sid, qty, availQty, subTotal, tid) values (::trade.portfolio.cid::,::trade.portfolio.sid::,::trade.portfolio.qty::,::trade.portfolio.availqty::,::trade.portfolio.subtotal::, ::trade.portfolio.tid::)"
	,
	
	"select sid, cid, subTotal from trade.portfolio where (subTotal >::trade.portfolio.subtotal:: and subTotal <= ::trade.portfolio.subtotal > $1::) and tid=::trade.portfolio.tid:: "
	"update trade.portfolio set availQty=availQty-100 where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"	 
	"update trade.portfolio set qty=qty-::trade.portfolio.qty:: where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	"select sid, cid, subTotal from trade.portfolio where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	,
	
	"select sid, cid, qty from trade.portfolio  where (qty >=::trade.portfolio.qty:: and availQty<::trade.portfolio.availqty::) and tid =::trade.portfolio.tid::"
	"update trade.portfolio set subTotal =  qty  where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	"select sid, cid, subTotal from trade.portfolio where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	,
	
	"select * from trade.portfolio where sid =::trade.portfolio.sid:: and cid=::trade.portfolio.cid:: and tid = ::trade.portfolio.tid::"
	"update trade.portfolio set subTotal=::trade.portfolio.subTotal:: where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	"update trade.portfolio set cid=::trade.portfolio.cid:: where cid = ::trade.portfolio.cid > $1:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	"select sid, cid, subTotal from trade.portfolio where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	,

	"update trade.portfolio set sid=::trade.portfolio.sid:: where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	"select sid from trade.portfolio where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	"select count(distinct cid) as num_distinct_cid from trade.portfolio where (subTotal<::trade.portfolio.subTotal:: or subTotal >=::trade.portfolio.subTotal < $1::) and tid =::trade.portfolio.tid::"
	"select distinct sid from trade.portfolio where (qty >=::trade.portfolio.qty:: and subTotal >= ::trade.portfolio.subTotal::) and tid =::trade.portfolio.tid::"
	,
	
	"delete from trade.portfolio where cid=::trade.portfolio.cid:: and sid=::trade.portfolio.sid:: and tid=::trade.portfolio.tid::"
	"select * from trade.portfolio where cid = ::trade.portfolio.cid:: and sid = ::trade.portfolio.sid::  and tid= ::trade.portfolio.tid::"
	,
  
  
// grops for sellorders
	"insert into trade.sellorders values (::trade.sellorders.oid::,::trade.sellorders.cid::,::trade.sellorders.sid::,::trade.sellorders.qty::,::trade.sellorders.ask::,::trade.sellorders.order_time::,::trade.sellorders.status::,::trade.sellorders.tid::)"
    "insert into trade.sellorders (oid, cid, sid, qty, ask, order_time, tid) values (::trade.sellorders.oid::,::trade.sellorders.cid::,::trade.sellorders.sid::,::trade.sellorders.qty::,::trade.sellorders.ask::,::trade.sellorders.order_time::,::trade.sellorders.tid::)"
    "select oid, cid, ask from trade.sellorders where (ask<::trade.sellorders.ask:: or ask >=::trade.sellorders.ask < $1::) and tid =::trade.sellorders.tid::"
    ,
    
    "select * from trade.sellorders where status = 'open' and tid = ::trade.sellorders.tid::"
    "update trade.sellorders set status = 'filled'  where sid = ::trade.sellorders.sid:: and ask<::trade.sellorders.ask:: and status = 'open' and tid = ::trade.sellorders.tid:: "
    "update trade.sellorders set ask = ::trade.sellorders.ask:: , qty=::trade.sellorders.qty:: where cid = ::trade.sellorders.cid:: and sid= ::trade.sellorders.sid:: and ask <::trade.sellorders.ask:: and status = 'open' and tid =::trade.sellorders.tid:: "
    
    "select cid, ask, sid from trade.sellorders where cid >::trade.sellorders.cid:: and tid = ::trade.sellorders.tid::"
    "update trade.sellorders set status = 'cancelled' where order_time >::trade.sellorders.order_time:: and status = 'open' and tid =::trade.sellorders.tid:: "
    "update trade.sellorders set ask = ::trade.sellorders.ask:: where cid = ::trade.sellorders.cid:: and sid= ::trade.sellorders.sid:: and status = 'open' and qty >::trade.sellorders.qty:: and tid =::trade.sellorders.tid:: "
    "select oid, sid, ask, status, cid from trade.sellorders where status = 'open' and tid =::trade.sellorders.tid::"
    ,
    
    "select oid, sid, ask, status, cid from trade.sellorders where status =::trade.sellorders.status:: and cid =::trade.sellorders.cid:: and tid =::trade.sellorders.tid::"
    "select oid, sid, ask, cid, status from trade.sellorders where ((status =::trade.sellorders.status:: and ask <::trade.sellorders.ask::) OR (status =::trade.sellorders.status:: and ask >::trade.sellorders.ask::))  and cid IN (::trade.sellorders.cid::, ::trade.sellorders.cid < $1::, ::trade.sellorders.cid < $1::, ::trade.sellorders.cid > $1::, ::trade.sellorders.cid > $2::) and tid =::trade.sellorders.tid::"
    "delete from trade.sellorders where cid=::trade.sellorders.cid:: and sid=::trade.sellorders.sid:: and tid=::trade.sellorders.tid::"
    "select * from trade.sellorders where cid=::trade.sellorders.cid:: and sid=::trade.sellorders.sid:: and tid=::trade.sellorders.tid::"
    ,
     
    "select oid, sid, ask, cid, order_time, status from trade.sellorders where (((order_time >::trade.sellorders.order_time:: and ask <::trade.sellorders.ask::) and (status =::trade.sellorders.status:: and ask >::trade.sellorders.ask::))  OR cid IN (::trade.sellorders.cid::, ::trade.sellorders.cid > $1::, ::trade.sellorders.cid > $1::, ::trade.sellorders.cid > $1::, ::trade.sellorders.cid > $1::)) and tid =::trade.sellorders.tid::"
    "select oid, cid, order_time, sid from trade.sellorders where (order_Time < ::trade.sellorders.order_time::) and tid =::trade.sellorders.tid:: "
    "select * from  trade.sellorders where oid IN (::trade.sellorders.oid::, ::trade.sellorders.oid > $1::, ::trade.sellorders.oid < $1::, ::trade.sellorders.oid > $2::, ::trade.sellorders.oid > $3::) and tid =::trade.sellorders.tid::"
    ,
    
    "delete from trade.sellorders where oid=::trade.sellorders.oid:: and tid=::trade.sellorders.tid::"
    "select * from trade.sellorders where oid=::trade.sellorders.oid:: and tid=::trade.sellorders.tid::"
    ,
    
    "delete from trade.sellorders where status IN ('cancelled', 'filled') and tid=::trade.sellorders.tid::"
    "select * from trade.sellorders where status IN ('cancelled', 'filled') and tid=::trade.sellorders.tid::"
    ,
    
    "delete from trade.sellorders where tid=::trade.sellorders.tid::"
    "select * from trade.sellorders where tid=::trade.sellorders.tid::"
    ,

// groups for txhistory
	"insert into trade.txhistory values (::trade.txhistory.cid::,::trade.txhistory.oid::,::trade.txhistory.sid::,::trade.txhistory.qty::,::trade.txhistory.price::,::trade.txhistory.ordertime::,::trade.txhistory.type::,::trade.txhistory.tid::)"
	"insert into trade.txhistory(cid, oid, sid, qty, price, ordertime, type, tid) values (::trade.txhistory.cid::,::trade.txhistory.oid::,::trade.txhistory.sid::,::trade.txhistory.qty::,::trade.txhistory.price::,::trade.txhistory.ordertime::,::trade.txhistory.type::,::trade.txhistory.tid::)"
	"delete from trade.txhistory where (type ='buy' or qty<::trade.txhistory.qty::) and price>::trade.txhistory.price:: and tid=::trade.txhistory.tid::"
	"select * from trade.txhistory where tid = ::trade.txhistory.tid::"
	,
  
    "select sid, count(*) from trade.txhistory  where cid>::trade.txhistory.cid:: and sid<::trade.txhistory.sid:: and tid =::trade.txhistory.tid:: GROUP BY sid HAVING count(*) >=1"
    "update trade.txhistory set type = ::trade.txhistory.type::, oid=::trade.txhistory.oid::  where sid = ::trade.txhistory.sid:: and cid=::trade.txhistory.cid:: and tid = ::trade.txhistory.tid:: "
    "select * from trade.txhistory where  sid = ::trade.txhistory.sid:: and cid=::trade.txhistory.cid:: and tid = ::trade.txhistory.tid:: "
    "select oid, cid, sid, type, tid from trade.txhistory where cid >::trade.txhistory.cid:: and sid <::trade.txhistory.sid:: and qty >::trade.txhistory.qty:: and orderTime<::trade.txhistory.ordertime:: and tid = ::trade.txhistory.tid::"
    ,    
    
    "select cid, max(price) from trade.txhistory  where tid =::trade.txhistory.tid:: GROUP BY cid, type"
    "update trade.txhistory set qty = ::trade.txhistory.qty::, cid=::trade.txhistory.cid::  where  oid=::trade.txhistory.oid:: and tid =::trade.txhistory.tid:: "
    "select * from trade.txhistory where  oid=::trade.txhistory.oid:: and tid =::trade.txhistory.tid::"
    "delete from trade.txhistory where oid=::trade.txhistory.oid:: and (price <::trade.txhistory.price:: or qty>::trade.txhistory.qty::) and type = 'buy' and tid=::trade.txhistory.tid::"
    "select * from trade.txhistory where  oid=::trade.txhistory.oid:: and (price <::trade.txhistory.price:: or qty>::trade.txhistory.qty::) and type = 'buy' and tid=::trade.txhistory.tid::"
    ,
    
    "select cid, sum(qty*price) as amount from trade.txhistory  where sid<::trade.txhistory.sid:: and tid =::trade.txhistory.tid:: GROUP BY cid, type ORDER BY amount"
	"delete from trade.txhistory where cid=::trade.txhistory.cid:: and sid=::trade.txhistory.sid:: and tid=::trade.txhistory.tid::"
    "select * from trade.txhistory where cid=::trade.txhistory.cid:: and sid=::trade.txhistory.sid:: and tid=::trade.txhistory.tid::"
    ,
    
    "delete from trade.txhistory where oid=::trade.txhistory.oid:: and (price >::trade.txhistory.price:: or qty<::trade.txhistory.qty::) and cid >=::trade.txhistory.cid:: and ordertime <::trade.txhistory.ordertime:: and tid=::trade.txhistory.tid::"
    "select * from trade.txhistory where oid=::trade.txhistory.oid:: and (price >::trade.txhistory.price:: or qty<::trade.txhistory.qty::) and cid >=::trade.txhistory.cid:: and ordertime <::trade.txhistory.ordertime:: and tid=::trade.txhistory.tid::"
    ,
    
    "delete from trade.txhistory where (type ='sell' or ordertime>::trade.txhistory.ordertime::) and sid=::trade.txhistory.sid:: and cid <::trade.txhistory.cid:: and tid=::trade.txhistory.tid::"
    "select * from trade.txhistory where (type ='sell' or ordertime>::trade.txhistory.ordertime::) and sid=::trade.txhistory.sid:: and cid <::trade.txhistory.cid:: and tid=::trade.txhistory.tid::"
    ;
    
