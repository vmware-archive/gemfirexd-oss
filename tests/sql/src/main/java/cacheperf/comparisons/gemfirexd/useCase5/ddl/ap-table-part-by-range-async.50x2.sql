DROP TABLE betting_tickets;
DROP TABLE terminal_accounts;
DROP TABLE OLTP_AP;
DROP TABLE ticket_sources;
DROP TABLE ticket_pools;
DROP TABLE physical_locations;
DROP TABLE physical_location_groups;
DROP TABLE source_types;
DROP TABLE err_logs;
DROP TABLE hist_betting_ticket_settlements;
DROP TABLE terminals;

create diskstore PersistentDiskStore 'data' autocompact false;

CREATE TABLE OLTP_AP (
  OLTP_AP_ID INTEGER DEFAULT 0 NOT NULL,
  OLTP_AP_ITEM INTEGER,
  PRIMARY KEY (OLTP_AP_ID)
) REPLICATE;


--TICKET_SOURCE aims to contains the Betting Ticket with the source inputting the ticket.
CREATE TABLE ticket_sources (
   ticket_id INTEGER NOT NULL, 
   source_id INTEGER NOT NULL, 
   PRIMARY KEY (source_id)
) REPLICATE;

CREATE TABLE terminals
(
   terminal_id INTEGER NOT NULL, 
   terminal_ref_id VARCHAR(256) NOT NULL, -- this is the reference ID given by terminal
   status_current INTEGER NOT NULL, 
   status_update_time TIMESTAMP NOT NULL,
   physical_location_group_id INTEGER NOT NULL, -- for AB, this is NULL
   msn INTEGER DEFAULT 0 NOT NULL, 
   logger_seq INTEGER DEFAULT 0 NOT NULL,
   PRIMARY KEY (terminal_id)
) 
PARTITION BY RANGE(terminal_id)
(
VALUES BETWEEN 0 AND 25,
VALUES BETWEEN 25 AND 50
)
REDUNDANCY 1
PERSISTENT 'PersistentDiskStore' ASYNCHRONOUS; 

CREATE TABLE ticket_pools (
	tsn	VARCHAR(64) not NULL,
	pool_id	INTEGER not NULL,
	source_id INTEGER not NULL,
	PRIMARY KEY (tsn, pool_id)
)
PARTITION BY RANGE(source_id)
(
VALUES BETWEEN 0 AND 25,
VALUES BETWEEN 25 AND 50
)
COLOCATE WITH (terminals)
REDUNDANCY 1
PERSISTENT 'PersistentDiskStore' ASYNCHRONOUS;

CREATE INDEX ticket_pools_tree_udx ON ticket_pools(pool_id);


CREATE TABLE betting_tickets (
   --ticket_id INTEGER NOT NULL, -- not favor to VoltDB
   source_id INTEGER NOT NULL, -- source_id is the foreign key to link up terminal and ticket
   --pool_id INTEGER NOT NULL, -- Foreign Key for Pool ID, actually there is no pool table
   --tsn VARCHAR(64) UNIQUE NOT NULL, -- should it be large or up to 256 byte? TODO review later
   tsn VARCHAR(64) NOT NULL, -- should it be large or up to 256 byte? TODO review later
   lock_by INTEGER DEFAULT 0 NOT NULL, -- default 0, if the ticket is working by a AP, the field expected to be the identity of the AP
   lock_time TIMESTAMP DEFAULT '1980-01-01 00:00:00' NOT NULL,
   status INTEGER DEFAULT 0 NOT NULL, -- 0 = NA, 1 = sold,  3 = paid, 4 = partial paid, 5 = cancel, 6 = forfeit
   dividend_status INTEGER DEFAULT 0 NOT NULL, -- 0 = NA, 1 = dividend partially determined, 2 = dividend confirmed
   dividend FLOAT DEFAULT 0 NOT NULL, 
   paid_status INTEGER DEFAULT 0 NOT NULL, -- 0 = NA, 1 = partial paid, 2 = payment settled 
   paid_amount FLOAT DEFAULT 0 NOT NULL,
   paid_time TIMESTAMP DEFAULT '1980-01-01 00:00:00'  NOT NULL, 
   cancel_time TIMESTAMP DEFAULT '1980-01-01 00:00:00'  NOT NULL,
   -- DB store the JSON directly for fasten the access
   --sold_time TIMESTAMP NOT NULL, --VoltDB
   -- sold_time DATETIME NOT NULL, -- SQL Server
   --paid_time TIMESTAMP NOT NULL, --VoltDB
   -- paid_time DATETIME NOT NULL, -- SQL Server
   --final_paid TINYINT NOT NULL, -- No boolean in VoltDB, so using TINYINT instead 
   --cancelled TINYINT NOT NULL,
   --div_caled TINYINT NOT NULL, 
   --unit_bet INTEGER NOT NULL, 
   --total_bets INTEGER NOT NULL, 
   --total_cost INTEGER NOT NULL,
   --allup_forumula VARCHAR(256) NOT NULL, -- no allup, empty string is expected
   --bet_lines_display VARCHAR(256) NOT NULL, 
   --bet_lines_raw VARCHAR(256) NOT NULL, 
   --total_rebate INTEGER NOT NULL, 
   --total_refund INTEGER NOT NULL, 
   --total_forfeit INTEGER NOT NULL
   --net_payout INTEGER NOT NULL   
   ticket_json VARCHAR(2048) NOT NULL,
   PRIMARY KEY (tsn) -- TODO: review the Primary key when model become more mature 
) 
PARTITION BY RANGE(source_id)
(
VALUES BETWEEN 0 AND 25,
VALUES BETWEEN 25 AND 50
)
COLOCATE WITH (terminals)
REDUNDANCY 1
PERSISTENT 'PersistentDiskStore' ASYNCHRONOUS;

-- for lookup TSN 
-- CREATE INDEX betting_tickets_udx ON betting_tickets(tsn);

CREATE TABLE terminal_accounts
(
   terminal_id INTEGER NOT NULL, 
   account_type INTEGER NOT NULL, 
   outstanding_amount FLOAT NOT NULL, 
   PRIMARY KEY (terminal_id, account_type)
) 
PARTITION BY RANGE(terminal_id)
(
VALUES BETWEEN 0 AND 25,
VALUES BETWEEN 25 AND 50
)
COLOCATE WITH (terminals)
REDUNDANCY 1
PERSISTENT 'PersistentDiskStore' ASYNCHRONOUS;

CREATE TABLE source_types (
   source_id INTEGER NOT NULL, 
   source_type INTEGER NOT NULL, -- 1 = terminal, expect this field is expandable
   PRIMARY KEY (source_id)
) REPLICATE;

CREATE TABLE physical_location_groups
(
   location_group_id INTEGER NOT NULL, 
   location_group_label VARCHAR(256),
   PRIMARY KEY (location_group_id)
) REPLICATE;

CREATE TABLE physical_locations
(
   location_group_id INTEGER NOT NULL, -- Foreign key map to physical_location_groups
   location_id INTEGER NOT NULL, 
   location_label VARCHAR(256),
   PRIMARY KEY (location_id)
) REPLICATE;

CREATE TABLE hist_betting_ticket_settlements
(
   tsn VARCHAR(64) NOT NULL,
   settle_type INTEGER NOT NULL, --1 = payout, 2 = cancel 
   ticket_sold_at_source_id INTEGER NOT NULL,
   ticket_settled_at_source_id INTEGER NOT NULL,
   account_type INTEGER NOT NULL,   
   settle_amount FLOAT NOT NULL, 
   settle_time TIMESTAMP NOT NULL, 
   PRIMARY KEY (tsn, settle_type, ticket_sold_at_source_id, settle_time)
) REPLICATE;

CREATE TABLE err_logs
(
   source_id INTEGER NOT NULL,
   log_time TIMESTAMP NOT NULL, 
   err_msg VARCHAR(4096)
) REPLICATE;

CREATE INDEX ulx_tree_err_time on err_logs(log_time DESC); 
