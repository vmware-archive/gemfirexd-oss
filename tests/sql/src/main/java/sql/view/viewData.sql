-- Populate the data to base tables for testing views, the table definitions are as following:
-- create table trade.securities (sec_id int not null, symbol varchar(10) not null, price decimal (30, 20), exchange varchar(10) not null, tid int, constraint sec_pk primary key (sec_id), constraint sec_uq unique (symbol, exchange), constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))
-- create table trade.customers (cid int not null, cust_name varchar(100), since date, addr varchar(100), tid int, primary key (cid))
-- create table trade.networth (cid int not null, cash decimal (30, 20), securities decimal (30, 20), loanlimit int, availloan decimal (30, 20),  tid int, constraint netw_pk primary key (cid), constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete restrict, constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), constraint availloan_ck check (loanlimit>=availloan and availloan >=0))
-- create table trade.portfolio (cid int not null, sid int not null, qty int not null, availQty int not null, subTotal decimal(30,20), tid int, constraint portf_pk primary key (cid, sid), constraint cust_fk foreign key (cid) references trade.customers (cid) on delete restrict, constraint sec_fk foreign key (sid) references trade.securities (sec_id), constraint qty_ck check (qty>=0), constraint avail_ch check (availQty>=0 and availQty<=qty)) 
-- create table trade.sellorders (oid int not null constraint orders_pk primary key, cid int, sid int, qty int, ask decimal (30, 20), order_time timestamp, status varchar(10) default 'open', tid int, constraint portf_fk foreign key (cid, sid) references trade.portfolio (cid, sid) on delete restrict, constraint status_ch check (status in ('cancelled', 'open', 'filled')))
-- create table trade.buyorders(oid int not null constraint buyorders_pk primary key, cid int, sid int, qty int, bid decimal (30, 20), ordertime timestamp, status varchar(10), tid int, constraint bo_cust_fk foreign key (cid) references trade.customers (cid) on delete restrict, constraint bo_sec_fk foreign key (sid) references trade.securities (sec_id), constraint bo_qty_ck check (qty>=0))
-- create table trade.txhistory(cid int, oid int, sid int, qty int, price decimal (30, 20), ordertime timestamp, type varchar(10), tid int,  constraint type_ch check (type in ('buy', 'sell')))
-- create table emp.employees (eid int not null constraint employees_pk primary key, emp_name varchar(100), since date, addr varchar(100), ssn varchar(9))
-- create table trade.trades (tid int, cid int, eid int, tradedate date, primary Key (tid), foreign key (cid) references trade.customers (cid), constraint emp_fk foreign key (eid) references emp.employees (eid));

insert into trade.securities values (201, 'VMW', 100.00, 'nasdaq', 0);
insert into trade.securities values (202, 'ORCL', 28.00, 'nasdaq', 0);
insert into trade.securities values (203, 'IBM', 200.00, 'nasdaq', 0);
insert into trade.securities values (204, 'BCS', 15.00, 'nye', 0);
insert into trade.securities values (205, 'C', 4.00, 'nye', 0);
insert into trade.securities values (206, 'SYB', 60.00, 'nye', 0);

insert into trade.customers values (1001, 'Gfxdtest1', '2011-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1002, 'Gfxdtest2', '2012-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1011, 'Gfxdtest11', '2012-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1012, 'Gfxdtest12', '2011-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1021, 'Gfxdtest21', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1022, 'Gfxdtest22', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1031, 'Gfxdtest31', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1032, 'Gfxdtest32', '2009-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1041, 'Gfxdtest41', '2009-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (1042, 'Gfxdtest42', '2012-02-28', '400 Amherst St. NH', 0);

insert into trade.customers values (2003, 'Gfxdtest3', '2011-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2004, 'Gfxdtest4', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2013, 'Gfxdtest13', '2011-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2014, 'Gfxdtest14', '2012-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2023, 'Gfxdtest23', '2009-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2024, 'Gfxdtest24', '2009-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2033, 'Gfxdtest33', '2011-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2034, 'Gfxdtest34', '2012-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2043, 'Gfxdtest43', '2011-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (2044, 'Gfxdtest44', '2010-02-28', '400 Amherst St. NH', 0);

insert into trade.customers values (3005, 'Gfxdtest5', '2012-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3006, 'Gfxdtest6', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3015, 'Gfxdtest15', '2011-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3016, 'Gfxdtest16', '2012-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3025, 'Gfxdtest25', '2008-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3026, 'Gfxdtest26', '2009-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3035, 'Gfxdtest35', '2011-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3036, 'Gfxdtest36', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3045, 'Gfxdtest45', '2012-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (3046, 'Gfxdtest46', '2010-02-28', '400 Amherst St. NH', 0);

insert into trade.customers values (4007, 'Gfxdtest7', '2006-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4008, 'Gfxdtest8', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4017, 'Gfxdtest17', '2002-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4018, 'Gfxdtest18', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4027, 'Gfxdtest27', '2001-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4028, 'Gfxdtest28', '2012-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4037, 'Gfxdtest37', '2004-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4038, 'Gfxdtest38', '2006-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4047, 'Gfxdtest47', '2005-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (4048, 'Gfxdtest48', '2006-02-28', '400 Amherst St. NH', 0);

insert into trade.customers values (5009, 'Gfxdtest9', '2002-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5019, 'Gfxdtest19', '2003-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5029, 'Gfxdtest29', '2004-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5039, 'Gfxdtest39', '2005-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5049, 'Gfxdtest49', '2006-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5059, 'Gfxdtest59', '2007-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5069, 'Gfxdtest69', '2008-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5079, 'Gfxdtest79', '2009-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5089, 'Gfxdtest89', '2010-02-28', '400 Amherst St. NH', 0);
insert into trade.customers values (5099, 'Gfxdtest99', '2011-02-28', '400 Amherst St. NH', 0);

insert into trade.networth values (1001, 1000, 10000, 10000, 10000, 0);
insert into trade.networth values (1002, 2000, 20000, 10000, 10000, 0);
insert into trade.networth values (1011, 1001, 10034, 10000, 10000, 0);
insert into trade.networth values (1012, 2070, 20000, 10000, 10000, 0);
insert into trade.networth values (1021, 1080, 10000, 10000, 10000, 0);
insert into trade.networth values (1022, 20030, 20000, 10000, 10000, 0);
insert into trade.networth values (1031, 1040, 10000, 10000, 10000, 0);
insert into trade.networth values (1032, 20100, 20000, 10000, 10000, 0);
insert into trade.networth values (1041, 1400, 10000, 10000, 10000, 0);
insert into trade.networth values (1042, 23000, 20000, 10000, 10000, 0);

insert into trade.networth values (2003, 3000, 30000, 10000, 10000, 0);
insert into trade.networth values (2004, 4020, 40000, 10000, 10000, 0);
insert into trade.networth values (2013, 3120, 30000, 10000, 10000, 0);
insert into trade.networth values (2014, 40010, 40000, 10000, 10000, 0);
insert into trade.networth values (2023, 3020, 30000, 10000, 10000, 0);
insert into trade.networth values (2024, 4040, 40000, 10000, 10000, 0);
insert into trade.networth values (2033, 320, 30000, 10000, 10000, 0);
insert into trade.networth values (2034, 4020, 40000, 10000, 10000, 0);
insert into trade.networth values (2043, 3030, 30000, 10000, 10000, 0);
insert into trade.networth values (2044, 4011, 40000, 10000, 10000, 0);

insert into trade.networth values (3005, 5200, 50000, 10000, 10000, 0);
insert into trade.networth values (3006, 6120, 60000, 10000, 10000, 0);
insert into trade.networth values (3015, 5010, 50000, 10000, 10000, 0);
insert into trade.networth values (3016, 6230, 60000, 10000, 10000, 0);
insert into trade.networth values (3025, 5120, 50000, 10000, 10000, 0);
insert into trade.networth values (3026, 6230, 60000, 10000, 10000, 0);
insert into trade.networth values (3035, 5040, 50000, 10000, 10000, 0);
insert into trade.networth values (3036, 6770, 60000, 10000, 10000, 0);
insert into trade.networth values (3045, 5010, 50000, 10000, 10000, 0);
insert into trade.networth values (3046, 16000, 60000, 10000, 10000, 0);

insert into trade.networth values (4007, 7000, 70000, 10000, 10000, 0);
insert into trade.networth values (4008, 8000, 80000, 10000, 10000, 0);
insert into trade.networth values (4017, 7100, 70000, 10000, 10000, 0);
insert into trade.networth values (4018, 1200, 80000, 10000, 10000, 0);
insert into trade.networth values (4027, 2300, 70000, 10000, 10000, 0);
insert into trade.networth values (4028, 4500, 80000, 10000, 10000, 0);
insert into trade.networth values (4037, 7100, 70000, 10000, 10000, 0);
insert into trade.networth values (4038, 8500, 80000, 10000, 10000, 0);
insert into trade.networth values (4047, 7600, 70000, 10000, 10000, 0);
insert into trade.networth values (4048, 8100, 80000, 10000, 10000, 0);

insert into trade.networth values (5009, 9120, 90000, 10000, 10000, 0);
insert into trade.networth values (5019, 9040, 90000, 10000, 10000, 0);
insert into trade.networth values (5029, 910, 90000, 10000, 10000, 0);
insert into trade.networth values (5039, 9010, 90000, 10000, 10000, 0);
insert into trade.networth values (5049, 1200, 90000, 10000, 10000, 0);
insert into trade.networth values (5059, 1600, 90000, 10000, 10000, 0);
insert into trade.networth values (5069, 9100, 90000, 10000, 10000, 0);
insert into trade.networth values (5079, 12300, 90000, 10000, 10000, 0);
insert into trade.networth values (5089, 3400, 90000, 10000, 10000, 0);
insert into trade.networth values (5099, 9500, 90000, 10000, 10000, 0);


insert into trade.buyorders values (10001, 1001, 201, 50, 99, '2012-02-28 12:52:16.424', 'open', 0);
insert into trade.buyorders values (10002, 1001, 202, 50, 27, '2012-02-28 12:52:16.434', 'open', 0);
insert into trade.buyorders values (10003, 1002, 203, 50, 199, '2012-02-28 12:52:16.425', 'open', 0);
insert into trade.buyorders values (10004, 1002, 204, 50, 14, '2012-02-28 12:52:16.435', 'open', 0);
insert into trade.buyorders values (10005, 2003, 205, 50, 3, '2012-02-28 12:52:16.426', 'open', 0);
insert into trade.buyorders values (10006, 2003, 201, 50, 98, '2012-02-28 12:52:16.436', 'open', 0);
insert into trade.buyorders values (10007, 2004, 202, 50, 26, '2012-02-28 12:52:16.427', 'open', 0);
insert into trade.buyorders values (10008, 2004, 203, 50, 198, '2012-02-28 12:52:16.437', 'open', 0);
insert into trade.buyorders values (10009, 1001, 204, 50, 13, '2012-02-28 12:52:16.428', 'open', 0);

insert into trade.txhistory values (1001, 10001, 201, 50, 99.99, '2012-02-28 12:52:16.424', 'buy', 0);
insert into trade.txhistory values (2003, 10003, 203, 50, 199.99, '2012-02-28 12:52:16.425', 'buy', 0);
insert into trade.txhistory values (3005, 10005, 205, 50, 3.99, '2012-02-28 12:52:16.426', 'buy', 0);

insert into emp.employees values (5001, 'Gfxdtest1', '2001-01-01', '1 Boston Rd, Boston MA', '111223333');
insert into emp.employees values (5002, 'Gfxdtest2', '2001-01-01', '1 Boston Rd, Boston MA', '222223333');
insert into emp.employees values (5003, 'Gfxdtest3', '2001-01-01', '1 Boston Rd, Boston MA', '333223333');
insert into emp.employees values (5004, 'Gfxdtest4', '2001-01-01', '1 Boston Rd, Boston MA', '444223333');
insert into emp.employees values (5005, 'Gfxdtest5', '2001-01-01', '1 Boston Rd, Boston MA', '555223333');
insert into emp.employees values (5006, 'Gfxdtest6', '2001-01-01', '1 Boston Rd, Boston MA', '666223333');
insert into emp.employees values (5007, 'Gfxdtest7', '2001-01-01', '1 Boston Rd, Boston MA', '777223333');
insert into emp.employees values (5008, 'Gfxdtest8', '2001-01-01', '1 Boston Rd, Boston MA', '888223333');
insert into emp.employees values (5009, 'Gfxdtest9', '2001-01-01', '1 Boston Rd, Boston MA', '999223333');

insert into trade.trades values (300, 1001, 5001, CURRENT_DATE);
insert into trade.trades values (301, 1001, 5001, CURRENT_DATE);
insert into trade.trades values (302, 1002, 5002, CURRENT_DATE);
insert into trade.trades values (303, 2003, 5003, CURRENT_DATE);
insert into trade.trades values (304, 1001, 5001, CURRENT_DATE);
insert into trade.trades values (305, 3005, 5005, CURRENT_DATE);
insert into trade.trades values (306, 3006, 5006, CURRENT_DATE);
insert into trade.trades values (307, 1002, 5002, CURRENT_DATE);
insert into trade.trades values (308, 4008, 5008, CURRENT_DATE);
insert into trade.trades values (309, 5009, 5009, CURRENT_DATE);

insert into trade.trades values (310, 4007, 5007, '2006-02-28');
insert into trade.trades values (311, 1001, 5001, '2010-02-28');
insert into trade.trades values (312, 1002, 5002, '2010-02-28');
insert into trade.trades values (313, 2003, 5003, '2010-02-28');
insert into trade.trades values (314, 2004, 5004, '2010-02-28');
insert into trade.trades values (315, 3005, 5005, '2010-02-28');
insert into trade.trades values (316, 3006, 5006, '2010-02-28');
insert into trade.trades values (317, 4008, 5008, '2010-02-28');
insert into trade.trades values (318, 4008, 5008, '2010-02-28');
insert into trade.trades values (319, 5009, 5009, '2010-02-28');

insert into trade.trades values (320, 4007, 5007, '2011-11-28');
insert into trade.trades values (321, 1001, 5001, '2011-02-28');
insert into trade.trades values (322, 1002, 5002, '2012-02-28');
insert into trade.trades values (323, 2003, 5003, '2010-02-28');
insert into trade.trades values (324, 2004, 5004, '2011-02-28');
insert into trade.trades values (325, 2003, 5003, '2009-02-28');
insert into trade.trades values (326, 3006, 5006, '2011-02-28');
insert into trade.trades values (327, 4007, 5007, '2012-02-28');
insert into trade.trades values (328, 4008, 5008, '2012-02-28');
insert into trade.trades values (329, 5009, 5009, '2011-02-28');

insert into trade.trades values (330, 4007, 5007, '2009-02-28');
insert into trade.trades values (331, 1001, 5001, '2010-02-28');
insert into trade.trades values (332, 1002, 5002, '2011-02-28');
insert into trade.trades values (333, 2003, 5003, '2009-02-28');
insert into trade.trades values (334, 2004, 5004, '2012-02-28');
insert into trade.trades values (335, 3005, 5005, '2009-02-28');
insert into trade.trades values (336, 3006, 5006, '2010-02-28');
insert into trade.trades values (337, 4007, 5007, '2011-02-28');
insert into trade.trades values (338, 4008, 5008, '2011-02-28');
insert into trade.trades values (339, 5009, 5009, '2009-02-28');

insert into trade.trades values (340, 4008, 5008, '2012-01-28');
insert into trade.trades values (341, 1001, 5001, '2009-02-28');
insert into trade.trades values (342, 1002, 5002, '2012-02-28');
insert into trade.trades values (343, 2003, 5003, '2011-02-28');
insert into trade.trades values (344, 2004, 5004, '2010-02-28');
insert into trade.trades values (345, 3005, 5005, '2010-02-28');
insert into trade.trades values (346, 3005, 5005, '2011-02-28');
insert into trade.trades values (347, 4007, 5007, '2009-02-28');
insert into trade.trades values (348, 4008, 5008, '2012-02-28');
insert into trade.trades values (349, 5009, 5009, '2010-02-28');

insert into trade.trades values (350, 1001, 5001, '2008-03-28');
insert into trade.trades values (351, 1001, 5001, '2006-02-28');
insert into trade.trades values (352, 1001, 5001, '2006-03-28');
insert into trade.trades values (353, 1001, 5001, '2007-04-28');
insert into trade.trades values (354, 1001, 5001, '2008-05-28');
insert into trade.trades values (355, 1001, 5001, '2006-06-28');
insert into trade.trades values (356, 2003, 5003, '2011-02-28');
insert into trade.trades values (357, 2004, 5004, '2008-02-28');
insert into trade.trades values (358, 3005, 5005, '2006-02-28');
insert into trade.trades values (359, 5009, 5009, '2010-02-28');

insert into trade.trades values (360, 1001, 5001, '2006-01-28');
insert into trade.trades values (361, 1001, 5001, '2006-02-28');
insert into trade.trades values (362, 4007, 5007, '2009-02-28');
insert into trade.trades values (363, 4007, 5007, '2009-03-28');
insert into trade.trades values (364, 4007, 5007, '2011-12-28');
insert into trade.trades values (365, 3005, 5005, '2010-02-28');
insert into trade.trades values (366, 3005, 5005, '2011-07-28');
insert into trade.trades values (367, 4007, 5007, '2009-07-28');
insert into trade.trades values (368, 4008, 5008, '2012-02-28');
insert into trade.trades values (369, 5009, 5009, '2010-02-28');

insert into trade.trades values (370, 5009, 5009, '2010-01-28');
insert into trade.trades values (371, 3005, 5005, '2011-07-28');
insert into trade.trades values (372, 3005, 5005, '2011-06-28');
insert into trade.trades values (373, 3005, 5005, '2011-08-28');
insert into trade.trades values (374, 3005, 5005, '2012-09-28');
insert into trade.trades values (375, 3005, 5005, '2010-07-28');
insert into trade.trades values (376, 3005, 5005, '2012-03-28');
insert into trade.trades values (377, 3005, 5005, '2011-07-28');
insert into trade.trades values (378, 3005, 5005, '2011-07-28');
insert into trade.trades values (379, 3005, 5005, '2011-07-28');
