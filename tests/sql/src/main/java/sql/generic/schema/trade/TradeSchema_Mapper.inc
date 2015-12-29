trade.customers.cid = ::format 999999
trade.customers.cust_name = ::random [15] len
trade.customers.addr = ::random [30] len
##
TRADE.SECURITIES.SEC_ID = ::format 999999
TRADE.SECURITIES.symbol = ::random [5]
TRADE.SECURITIES.exchange = ::valuelist {nasdaq,nye,amex,lse,fse,hkse,tse}
TRADE.SECURITIES.PRICE = ::format 9999.9999
##
TRADE.NETWORTH.CID = ::foreignkey trade.customers.cid repeat 1
TRADE.NETWORTH.loanlimit = ::format 99999999
TRADE.NETWORTH.availloan = ::format 99999.99999
TRADE.NETWORTH.cash= ::format 999999.99999
##
TRADE.PORTFOLIO.CID = ::foreignkey trade.customers.cid repeat 3
TRADE.PORTFOLIO.SID =::foreignkey TRADE.SECURITIES.SEC_ID repeat 1
TRADE.PORTFOLIO.AVAILQTY = ::random [5] len
TRADE.PORTFOLIO.QTY = ::format 99999999
##
trade.sellorders.oid = ::format 99999
TRADE.sellorders.CID = ::foreignkey TRADE.PORTFOLIO.CID repeat 1
TRADE.sellorders.SID = ::foreignkey TRADE.PORTFOLIO.SID repeat 1
TRADE.sellorders.status = ::valuelist {cancelled,filled,open}
##
trade.buyorders.oid = ::format 999
trade.buyorders.cid = ::foreignkey trade.customers.cid repeat 10
trade.buyorders.sid = ::foreignkey TRADE.SECURITIES.SEC_ID repeat 1
trade.buyorders.qty = ::random [4]
trade.buyorders.bid = ::format 99999.999
##
trade.txhistory.type = ::valuelist {buy,sell}
trade.txhistory.price =::format 999.99
