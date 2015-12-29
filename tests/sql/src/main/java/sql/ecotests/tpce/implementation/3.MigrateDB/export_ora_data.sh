#!/bin/bash
export ORACLE_HOME=/usr/app/oracle/product/11.2.0/client_1
export PATH=$PATH:$ORACLE_HOME/bin
result=`sqlplus -l -s tpcegfxd/tpcegfxd@orclrh46<<_EOF_
set serveroutput off;
set termout off;
set verify off
set echo off
set newpage none
set pagesize 0
set feedback off
set linesize 32000
set heading off
set trimspool on
set colsep '|'
spool /s2qa/tangc/samples/tpce/implementation/3.MigrateDB/ora_customer.dat
select * from customer;
spool off
exit;
_EOF_`
