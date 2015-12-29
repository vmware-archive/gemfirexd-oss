CREATE DISKSTORE UseCase5DiskStore 'data' autocompact false;

CREATE TABLE terminals
(
   terminal_id                 BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   terminal_status_update_time TIMESTAMP NOT NULL,
   terminal_status             SMALLINT NOT NULL,
   PRIMARY KEY (terminal_id)
   -- ,FOREIGN KEY(terminal_id) REFERENCES source_types (ref_id)
) PARTITION BY (terminal_id) REDUNDANCY 1 PERSISTENT 'UseCase5DiskStore' ASYNCHRONOUS;

CREATE UNIQUE INDEX terminal_udx01 ON terminals(terminal_id);

CREATE TABLE source_types
(
   source_id            CHAR(10) NOT NULL,
   ref_id               INT NOT NULL, -- terminal_id for terminal operations 
   source_type_id       SMALLINT NOT NULL,
   PRIMARY KEY (ref_id)
   -- ,FOREIGN KEY (ref_id) REFERENCES terminals (terminal_id)
) PARTITION BY (ref_id) COLOCATE WITH (terminals) REDUNDANCY 1 PERSISTENT 'UseCase5DiskStore' ASYNCHRONOUS;

CREATE INDEX source_types_clx01 ON source_types (ref_id); 
CREATE INDEX source_types_clx02 ON source_types (source_id, source_type_id);

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
) REPLICATE PERSISTENT;

CREATE UNIQUE INDEX tickets_udx01 ON tickets (tsn);

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
) REPLICATE PERSISTENT;

CREATE UNIQUE INDEX ticket_transaction_log_udx01 ON ticket_transaction_log (transaction_id); 

CREATE TABLE source_message_results
(
   seq_no               BIGINT GENERATED ALWAYS AS IDENTITY,
   transaction_id       CHAR(32) NOT NULL,
   return_message       VARCHAR(256) NOT NULL,
   responded_ap         INT NOT NULL,
   ce_seq_no            BIGINT default 0,
   created_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (seq_no)
) REPLICATE PERSISTENT;

CREATE INDEX source_message_results_clx01 ON source_message_results ( transaction_id );

CREATE TABLE tickets_in_pool
(
   seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   pool_id              BIGINT NOT NULL,
   tsn                  CHAR(20) NULL,
   ticket_id            BIGINT NULL,
   PRIMARY KEY (seq_no)
) REPLICATE PERSISTENT;

CREATE INDEX tickets_in_pool_clx01 ON tickets_in_pool (pool_id); 

CREATE TABLE terminal_balance_transaction_log
(
   seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
   terminal_id          BIGINT NOT NULL, 
   ticket_transaction_log_seq_no BIGINT NOT NULL,
   transaction_amount   BIGINT NOT NULL, 
   transaction_type     SMALLINT NOT NULL, 
   created_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   PRIMARY KEY (seq_no)
) REPLICATE PERSISTENT;

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
) REPLICATE PERSISTENT;

CREATE UNIQUE INDEX source_message_logs_clx01 ON source_message_logs (transaction_id, source_id); 

CREATE VIEW mv_source_id_and_terminal AS
SELECT source_id, terminal_id, terminal_status_update_time, terminal_status, source_type_id
FROM source_types, terminals
WHERE source_types.ref_id = terminals.terminal_id;
