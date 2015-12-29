SET CURRENT SCHEMA=SEC_OWNER;

insert into SECM_ERROR_MESSAGE (ERROR_CODE, ERROR_MESSAGE, LOCALE) values ('IPAY_MSG_PERSIST_ERROR', 'Could not persist BO transaction received from $backOffice on queue, $boInQueue', 'en');
insert into SECM_ERROR_MESSAGE (ERROR_CODE, ERROR_MESSAGE, LOCALE) values ('IPAY_MSG_PARSER_ERROR', 'Could not parse BO transaction with OFAC message id $ofacMsgId', 'en');
insert into SECM_ERROR_MESSAGE (ERROR_CODE, ERROR_MESSAGE, LOCALE) values ('STRPROC_ERROR', 'Could not invoke matching stored procedure, OFAC message id - $ofacMsgId', 'en');
insert into SECM_ERROR_MESSAGE (ERROR_CODE, ERROR_MESSAGE, LOCALE) values ('OFAC_RES_ERROR', 'Received error response from Fircosoft. OFAC message id - $ofacMsgId', 'en');
