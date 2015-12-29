CREATE TABLE account_balances
(
   account_id           BIGINT NOT NULL,
   balance_type         SMALLINT NOT NULL,
   account_balance      BIGINT NOT NULL,
   locked_by            INT NOT NULL DEFAULT -1,
   PRIMARY KEY (account_id, balance_type)
) PARTITION BY COLUMN (account_id) REDUNDANCY 1 PERSISTENT;

CREATE TABLE account_balance_transaction_log
(
   seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   account_id           BIGINT NOT NULL, 
   ticket_transaction_log_seq_no BIGINT NOT NULL, 
   transaction_amount   BIGINT NOT NULL, 
   transaction_type     SMALLINT NOT NULL, 
   created_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (seq_no)
) PARTITION BY COLUMN (seq_no) REDUNDANCY 1 PERSISTENT;

CREATE TABLE account_type_lookup 
( account_type_id       SMALLINT NOT NULL PRIMARY KEY, 
  account_type_desc     VARCHAR(50) NOT NULL
) REPLICATE PERSISTENT; 

INSERT INTO account_type_lookup (account_type_id, account_type_desc) VALUES (1, 'Terminal Cash Balance');
INSERT INTO account_type_lookup (account_type_id, account_type_desc) VALUES (2, 'Cash Betting Sale');

CREATE TABLE bank_accounts
(
   bank_account_id      BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   bank_type            SMALLINT NOT NULL, -- Normal account or a GBA or a NBA
   bank_account_number  CHAR(50) NOT NULL, 
   bank_number          CHAR(20) NOT NULL, 
   branch_number        CHAR(10) NOT NULL, 
   guarantee_amount     BIGINT NOT NULL,   
   user_id              BIGINT NOT NULL,
   PRIMARY KEY(bank_account_id)
) REPLICATE PERSISTENT;

CREATE INDEX bank_accounts_idx01 ON bank_accounts (bank_account_number);

CREATE TABLE betting_user_accounts
(
   account_id           BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   account_number       CHAR(50) NOT NULL, 
   account_type         SMALLINT NOT NULL, 
   account_status       SMALLINT NOT NULL DEFAULT 0, 
   last_activity_date   TIMESTAMP NOT NULL,
   bank_gurantee_type   SMALLINT NOT NULL DEFAULT -1,
   account_register_date TIMESTAMP NOT NULL,
   betting_user_id      BIGINT NOT NULL, -- foreign key to betting_users.user_id
   PRIMARY KEY (account_id)
) PARTITION BY COLUMN (account_id) REDUNDANCY 1 PERSISTENT;

CREATE UNIQUE INDEX betting_user_accounts_udx01 ON betting_user_accounts(account_number); 

CREATE TABLE account_allowed_bet_type
(
   account_id           BIGINT NOT NULL, 
   allow_bet_type       SMALLINT NOT NULL,
   FOREIGN KEY (account_id) REFERENCES betting_user_accounts (account_id)
) PARTITION BY COLUMN (account_id) COLOCATE WITH (betting_user_accounts) REDUNDANCY 1 PERSISTENT;

CREATE INDEX account_allowed_bet_type_idx01 ON account_allowed_bet_type (account_id);

CREATE TABLE betting_user_service_level
(
   user_id              BIGINT NOT NULL, 
   service_type         SMALLINT NOT NULL, 
   service_rank         SMALLINT NOT NULL, 
   spoken_language      SMALLINT NOT NULL DEFAULT 0, 
   additional_info      CHAR(100) NULL, 
   monitor_voice_access SMALLINT NOT NULL DEFAULT 0
) REPLICATE PERSISTENT;

CREATE INDEX betting_user_service_level_idx01 ON betting_user_service_level (user_id, service_type);

CREATE TABLE betting_users
(
   user_id              BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY, 
   user_number          CHAR(100) NOT NULL, -- External Reference for Display and communication
   user_type            INT NOT NULL, 
   user_surname_eng     CHAR(20) NOT NULL, 
   user_othername_eng   CHAR(20) NOT NULL, 
   user_surname_chi     CHAR(20) NULL, 
   user_othername_chi   CHAR(20) NULL, 
   address              VARCHAR(255) NULL, 
   mobile               CHAR(20) NULL, 
   security_code        CHAR(50) NOT NULL, -- should be encrypted
   force_change_security_code SMALLINT DEFAULT 0, 
   language             SMALLINT DEFAULT 0,
   user_register_date   TIMESTAMP NOT NULL, 
   salutation           SMALLINT NOT NULL,
   PRIMARY KEY (user_id)
) REPLICATE PERSISTENT;

CREATE UNIQUE INDEX betting_users_udx01 ON betting_users (user_number);
CREATE INDEX betting_users_idx02 ON betting_users (mobile);

CREATE TABLE event_lookup
(
   event_id             INT NOT NULL PRIMARY KEY, 
   event_category       SMALLINT NOT NULL, 
   event_desc           CHAR(50) NOT NULL
) REPLICATE PERSISTENT;

INSERT INTO event_lookup (event_id, event_category, event_desc) VALUES (101, 1, 'cash betting'); 
INSERT INTO event_lookup (event_id, event_category, event_desc) VALUES (102, 1, 'ticket sold'); 
INSERT INTO event_lookup (event_id, event_category, event_desc) VALUES (103, 1, 'ticket paid'); 
INSERT INTO event_lookup (event_id, event_category, event_desc) VALUES (104, 1, 'ticket cancelled'); 
INSERT INTO event_lookup (event_id, event_category, event_desc) VALUES (105, 1, 'ticket partial paid'); 

CREATE TABLE source_message_logs
(
   seq_no               BIGINT GENERATED ALWAYS AS IDENTITY,
   transaction_id       CHAR(32) NOT NULL,
   message_content      BLOB(256) NOT NULL,
   arrival_time         TIMESTAMP NOT NULL,
   source_id            CHAR(10) NOT NULL,
   event_id             INT NOT NULL,
   msn                  SMALLINT NOT NULL default 0,
   responded_ap         SMALLINT NOT NULL,
   PRIMARY KEY (seq_no)
) PARTITION BY COLUMN (seq_no) REDUNDANCY 1 PERSISTENT;

CREATE UNIQUE INDEX source_message_logs_clx01 ON source_message_logs (transaction_id, source_id); 

CREATE TABLE source_message_results
(
   seq_no               BIGINT GENERATED ALWAYS AS IDENTITY,
   transaction_id       CHAR(32) NOT NULL,
   return_message       VARCHAR(256) NOT NULL,
   responded_ap         INT NOT NULL,
   ce_seq_no            BIGINT default 0,
   created_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (seq_no)
) PARTITION BY COLUMN (seq_no) REDUNDANCY 1 PERSISTENT;

CREATE INDEX source_message_results_clx01 ON source_message_results ( transaction_id );

CREATE TABLE source_type_lookup
(
   source_type_id       SMALLINT NOT NULL,
   source_type_desc     VARCHAR(50),
   PRIMARY KEY (source_type_id)
) REPLICATE PERSISTENT;

INSERT INTO source_type_lookup VALUES (1, 'Terminal');

CREATE TABLE source_types
(
   source_id            CHAR(10) NOT NULL,
   ref_id               INT NOT NULL, -- terminal_id for terminal operations 
   source_type_id       SMALLINT NOT NULL
   --PRIMARY KEY (source_id)
   --,FOREIGN KEY (ref_id) REFERENCES terminals (terminal_id)
) REPLICATE PERSISTENT;

CREATE INDEX source_types_clx01 ON source_types (ref_id); 
CREATE INDEX source_types_clx02 ON source_types (source_id, source_type_id);

CREATE TABLE staff_duty_log
(
   seq_no                   BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   staff_id                 BIGINT NOT NULL, 
   terminal_id              BIGINT NOT NULL, 
   action_type              SMALLINT NOT NULL, 
   terminal_transaction_log_seq_no BIGINT NOT NULL, 
   created_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (seq_no)
) REPLICATE PERSISTENT;

CREATE TABLE terminal_accounts
(
   terminal_id          INT NOT NULL,
   account_type_id      SMALLINT NOT NULL,
   outstanding_amount   BIGINT NOT NULL DEFAULT 0,
   PRIMARY KEY (terminal_id, account_type_id)
) REPLICATE PERSISTENT;

CREATE TABLE terminal_balance_transaction_log
(
   seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   terminal_id          BIGINT NOT NULL, 
   ticket_transaction_log_seq_no BIGINT NOT NULL,
   transaction_amount   BIGINT NOT NULL, 
   transaction_type     SMALLINT NOT NULL, 
   created_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (seq_no)
) PARTITION BY COLUMN (seq_no) REDUNDANCY 1 PERSISTENT;

CREATE TABLE terminals
(
   terminal_id          BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   terminal_status_update_time TIMESTAMP NOT NULL,
   terminal_status      SMALLINT NOT NULL,
   PRIMARY KEY (terminal_id)
   -- ,foreign key (terminal_id) REFERENCES source_types (ref_id)
) PARTITION BY COLUMN (terminal_id) PERSISTENT;

CREATE UNIQUE INDEX terminal_udx01 ON terminals(terminal_id);

CREATE TABLE terminal_status_lookup
(
   terminal_status_id   SMALLINT NOT NULL,
   terminal_status_desc VARCHAR(50) NOT NULL,
   PRIMARY KEY (terminal_status_id)
) REPLICATE PERSISTENT;

INSERT INTO terminal_status_lookup VALUES (1, 'Online');
INSERT INTO terminal_status_lookup VALUES (0, 'Offline');

CREATE TABLE ticket_dividend_status_lookup
(
   ticket_dividend_status_id          SMALLINT NOT NULL,
   ticket_dividend_status_desc        VARCHAR(50) NOT NULL,
   PRIMARY KEY (ticket_dividend_status_id)
) REPLICATE PERSISTENT;

INSERT INTO ticket_dividend_status_lookup VALUES (1, 'NA');
INSERT INTO ticket_dividend_status_lookup VALUES (2, 'Partial Dividend');
INSERT INTO ticket_dividend_status_lookup VALUES (3, 'Full Dividend');

CREATE TABLE tickets_in_account
(
   seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   account_id           BIGINT NOT NULL,
   ticket_id            BIGINT NOT NULL,
   si_id                INT NOT NULL DEFAULT -1,
   PRIMARY KEY (seq_no)
) PARTITION BY COLUMN (seq_no) REDUNDANCY 1 PERSISTENT;

CREATE INDEX tickets_in_account_clx01 ON tickets_in_account (account_id); 
CREATE INDEX tickets_in_account_clx02 ON tickets_in_account (ticket_id); 

CREATE TABLE tickets_in_pool
(
   seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   pool_id              BIGINT NOT NULL,
   tsn                  CHAR(20) NULL,
   ticket_id            BIGINT NULL,
   PRIMARY KEY (seq_no)
) PARTITION BY COLUMN (seq_no) REDUNDANCY 1 PERSISTENT;

CREATE INDEX tickets_in_pool_clx01 ON tickets_in_pool (pool_id); 

CREATE TABLE tickets
(
   ticket_id            BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   ticket_type          SMALLINT NOT NULL, 
   tsn                  CHAR(20) NOT NULL,
   base_investment      BIGINT NOT NULL,
   total_investment     BIGINT NOT NULL,
   parent_ticket_id     BIGINT NOT NULL DEFAULT -1, -- Default when it is the initial sell of a ticket
   current_ticket_status SMALLINT NOT NULL DEFAULT 1,
   current_responded_ap SMALLINT NOT NULL,
   current_responded_terminal INT NOT NULL,
   paid_status          SMALLINT NOT NULL DEFAULT 6,
   paid_amount          BIGINT NOT NULL DEFAULT 0,
   dividend_status      SMALLINT NOT NULL DEFAULT 9,
   dividend_amount      BIGINT NOT NULL DEFAULT 0,
   locked_by            BIGINT NOT NULL DEFAULT -1,  -- this field is used when pay and cancel ticket
   ticket_json          BLOB(1024) NOT NULL,
   PRIMARY KEY (ticket_id)
) PARTITION BY COLUMN (ticket_id) REDUNDANCY 1 PERSISTENT;

CREATE UNIQUE INDEX tickets_udx01 ON tickets (tsn);

CREATE TABLE ticket_status_lookup
(
   ticket_status_id     SMALLINT NOT NULL,
   ticket_status_desc   VARCHAR(50) NOT NULL,
   PRIMARY KEY (ticket_status_id)
) REPLICATE PERSISTENT;

INSERT INTO ticket_status_lookup VALUES (1, 'ticket sold');
INSERT INTO ticket_status_lookup VALUES (2, 'ticket partial paid');
INSERT INTO ticket_status_lookup VALUES (3, 'ticket paid');
INSERT INTO ticket_status_lookup VALUES (4, 'ticket canceled');
INSERT INTO ticket_status_lookup VALUES (5, 'ticket forfeited');

CREATE TABLE ticket_transaction_log
(
   seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   transaction_id       CHAR(32) NOT NULL,
   transaction_type     SMALLINT NOT NULL,
   ticket_id            BIGINT NOT NULL default 0,
   responded_ap         SMALLINT NOT NULL,
   responded_terminal   INT NOT NULL,
   event_info           VARCHAR(255) NOT NULL,
   created_time         TIMESTAMP default current_TIMESTAMP NOT NULL,
   PRIMARY KEY (seq_no)
) PARTITION BY COLUMN (seq_no) REDUNDANCY 1 PERSISTENT;

CREATE UNIQUE INDEX ticket_transaction_log_udx01 ON ticket_transaction_log (transaction_id); 

CREATE TABLE ticket_type_lookup
(
   ticket_type_id       INT NOT NULL GENERATED ALWAYS AS IDENTITY,
   ticket_type_desc     CHAR(50) NOT NULL,
   PRIMARY KEY (ticket_type_id)
) REPLICATE PERSISTENT;

INSERT INTO ticket_type_lookup (ticket_type_desc) VALUES ('Cash Bet');
INSERT INTO ticket_type_lookup (ticket_type_desc) VALUES ('Account Bet');
INSERT INTO ticket_type_lookup (ticket_type_desc) VALUES ('CV');
INSERT INTO ticket_type_lookup (ticket_type_desc) VALUES ('Partial Pay');

CREATE VIEW mv_source_id_and_terminal AS
SELECT source_id, terminal_id, terminal_status_update_time, terminal_status, source_type_id
FROM source_types, terminals
WHERE source_types.ref_id = terminals.terminal_id;
