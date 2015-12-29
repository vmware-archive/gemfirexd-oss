SET CURRENT SCHEMA=SEC_OWNER;

insert into SECM_ERROR_CODES (ERROR_CODE, DESCRIPTION) values ('IPAY_MSG_PERSIST_ERROR', 'Could not persist BO transaction received from back-office');
insert into SECM_ERROR_CODES (ERROR_CODE, DESCRIPTION) values ('IPAY_MSG_PARSER_ERROR', 'Could not parse BO transaction.');
insert into SECM_ERROR_CODES (ERROR_CODE, DESCRIPTION) values ('STRPROC_ERROR', 'Could not invoke matching stored procedure.');
insert into SECM_ERROR_CODES (ERROR_CODE, DESCRIPTION) values ('OFAC_RES_ERROR', 'Received error response from Fircosoft.');

insert into SECM_ERROR_CODES (ERROR_CODE, DESCRIPTION) values ('SINGLE_MATCH_MONITORING', 'Send alert when percentage of single matched transactions is below the configured threshold value. ');
insert into SECM_ERROR_CODES (ERROR_CODE, DESCRIPTION) values ('ORPHAN_MONITORING', 'Send alert when percentage of Orphan transactions is below the configured threshold value. ');
