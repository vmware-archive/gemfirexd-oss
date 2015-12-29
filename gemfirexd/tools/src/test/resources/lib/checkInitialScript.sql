-- sample customers table
create table trade.customers (
  cid int not null,
  cust_name varchar(100),
  addr varchar(100),
  tid int,
  primary key (cid),
  constraint cust_uk unique (tid)
);

-- sample portfolio table having foreign key constraint on customers table
create table trade.portfolio (
  cid int not null,
  sid int not null,
  qty int not null,
  availQty int not null,
  tid int not null,
  constraint portf_pk primary key (cid, sid),
  constraint cust_fk foreign key (cid)
    references trade.customers (cid) on delete restrict,
  constraint qty_ck check (qty >= 0),
  constraint avail_ck check (availQty <= qty)
);
