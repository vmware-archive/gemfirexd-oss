-- This version of schema implements the server group in addition to v2.
-- v4 is changed from v3:
-- last_trade to REPLICATE SERVER GROUPS(MarketGroup, CustomerGroup); -- distributed join limitation
-- v5 is changed from v4:
-- sector, industry, company, company_competitor should be replicated to CustomerGroup as well to support
-- BrokerVolume query
-- create a table holding_summary_tbl to work around 46865, the holding_summary issue
-- connect client '10.118.32.4:1527;user=app;password=app';
-- =========================================
-- Foreign Key is implicitly indexed in GemFireXD
-- Partition/Replicate Strategies:
-- 1. Limitted by partition/colocate on parent/child/grandchild tables
-- 2. Limitted by distributed join
-- 3. Dimension tables are recommended to replicate
-- Two server groups:
-- 1. MarketGroup: include all markets tables
-- 2. CustomerGroup: include all customer and broker tables
-- 3. Replicated tables exchange, security and dimension tables are included in two server groups

SET SCHEMA TPCEGFXD;

--
-- Alter table add contraints back
--
ALTER TABLE address ADD CONSTRAINT fk_address_zip_code_zc_code
FOREIGN KEY (ad_zc_code) REFERENCES zip_code (zc_code);

-- c_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
ALTER TABLE customer ADD CONSTRAINT fk_customer_status_type_st_id
FOREIGN KEY (c_st_id) REFERENCES status_type (st_id);

-- c_ad_id BIGINT NOT NULL REFERENCES address (ad_id),
ALTER TABLE customer ADD CONSTRAINT fk_customer_address_ad_id
FOREIGN KEY (c_ad_id) REFERENCES address (ad_id);

-- ex_ad_id BIGINT NOT NULL REFERENCES address (ad_id),
ALTER TABLE exchange ADD CONSTRAINT fk_exchange_address_ad_id
FOREIGN KEY (ex_ad_id) REFERENCES address (ad_id);

-- in_sc_id CHAR(2) NOT NULL REFERENCES sector (sc_id),
ALTER TABLE industry ADD CONSTRAINT fk_industry_sector_sc_id
FOREIGN KEY (in_sc_id) REFERENCES sector (sc_id);

-- co_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
ALTER TABLE company ADD CONSTRAINT fk_company_status_type_st_id
FOREIGN KEY (co_st_id) REFERENCES status_type (st_id);

-- co_in_id CHAR(2) NOT NULL REFERENCES industry (in_id),
ALTER TABLE company ADD CONSTRAINT fk_company_industry_in_id
FOREIGN KEY (co_in_id) REFERENCES industry (in_id);

-- co_ad_id BIGINT NOT NULL REFERENCES address (ad_id),
ALTER TABLE company ADD CONSTRAINT fk_company_address_ad_id
FOREIGN KEY (co_ad_id) REFERENCES address (ad_id);

-- cp_co_id BIGINT NOT NULL REFERENCES company (co_id),
ALTER TABLE company_competitor ADD CONSTRAINT fk_company_c_company_co_id
FOREIGN KEY (cp_co_id) REFERENCES company (co_id);

-- cp_comp_co_id BIGINT NOT NULL REFERENCES company (co_id),
ALTER TABLE company_competitor ADD CONSTRAINT fk_company_comp_company_co_id
FOREIGN KEY (cp_comp_co_id) REFERENCES company (co_id);

-- cp_in_id CHAR(2) NOT NULL REFERENCES industry (in_id),
ALTER TABLE company_competitor ADD CONSTRAINT fk_company_c_industry_in_id
FOREIGN KEY (cp_in_id) REFERENCES industry (in_id);

-- s_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
ALTER TABLE security ADD CONSTRAINT fk_security_status_type_st_id
FOREIGN KEY (s_st_id) REFERENCES status_type (st_id);

-- s_ex_id CHAR(6) NOT NULL REFERENCES exchange (ex_id),
ALTER TABLE security ADD CONSTRAINT fk_security_exchange_ex_id
FOREIGN KEY (s_ex_id) REFERENCES exchange (ex_id);

-- s_co_id BIGINT NOT NULL REFERENCES company (co_id),
ALTER TABLE security ADD CONSTRAINT fk_security_company_co_id
FOREIGN KEY (s_co_id) REFERENCES company (co_id);

-- dm_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
ALTER TABLE daily_market ADD CONSTRAINT fk_daily_m_security_s_symb
FOREIGN KEY (dm_s_symb) REFERENCES security (s_symb);

-- fi_co_id BIGINT NOT NULL REFERENCES company (co_id),
ALTER TABLE financial ADD CONSTRAINT fk_financial_company_co_id
FOREIGN KEY (fi_co_id) REFERENCES company (co_id);

-- lt_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
ALTER TABLE last_trade ADD CONSTRAINT fk_last_trade_security_s_symb
FOREIGN KEY (lt_s_symb) REFERENCES security (s_symb);

-- nx_ni_id BIGINT NOT NULL REFERENCES news_item (ni_id),
ALTER TABLE news_xref ADD CONSTRAINT fk_xref_news_item_ni_id
FOREIGN KEY (nx_ni_id) REFERENCES news_item (ni_id);

-- nx_co_id BIGINT NOT NULL REFERENCES company (co_id),
ALTER TABLE news_xref ADD CONSTRAINT fk_xref_company_co_id
FOREIGN KEY (nx_co_id) REFERENCES company (co_id);

-- b_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
ALTER TABLE broker ADD CONSTRAINT fk_broker_status_type_st_id
FOREIGN KEY (b_st_id) REFERENCES status_type (st_id);

-- ca_b_id BIGINT NOT NULL REFERENCES broker (b_id),
ALTER TABLE customer_account ADD CONSTRAINT fk_customer_a_broker_b_id
FOREIGN KEY (ca_b_id) REFERENCES broker (b_id);

-- ca_c_id BIGINT NOT NULL REFERENCES customer (c_id),
ALTER TABLE customer_account ADD CONSTRAINT fk_customer_a_customer_c_id
FOREIGN KEY (ca_c_id) REFERENCES customer (c_id);

-- ap_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
ALTER TABLE account_permission ADD CONSTRAINT fk_account_p_customer_account_ca_id
FOREIGN KEY (ap_ca_id) REFERENCES customer_account (ca_id);

-- cx_tx_id CHAR(4) NOT NULL REFERENCES taxrate (tx_id),
ALTER TABLE customer_taxrate ADD CONSTRAINT fk_customer_t_taxrate_tx_id
FOREIGN KEY (cx_tx_id) REFERENCES taxrate (tx_id);

-- cx_c_id BIGINT NOT NULL REFERENCES customer (c_id),
ALTER TABLE customer_taxrate ADD CONSTRAINT fk_customer_t_customer_c_id
FOREIGN KEY (cx_c_id) REFERENCES customer (c_id);

-- t_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
ALTER TABLE trade ADD CONSTRAINT fk_trade_status_type_st_id
FOREIGN KEY (t_st_id) REFERENCES status_type (st_id);

-- t_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id),
ALTER TABLE trade ADD CONSTRAINT fk_trade_trade_type_tt_id
FOREIGN KEY (t_tt_id) REFERENCES trade_type (tt_id);

-- t_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
ALTER TABLE trade ADD CONSTRAINT fk_trade_security_s_symb
FOREIGN KEY (t_s_symb) REFERENCES security (s_symb);

-- t_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
ALTER TABLE trade ADD CONSTRAINT fk_trade_customer_a_ca_id
FOREIGN KEY (t_ca_id) REFERENCES customer_account (ca_id);

-- se_t_id BIGINT NOT NULL REFERENCES trade (t_id),
ALTER TABLE settlement ADD CONSTRAINT fk_settlement_trade_t_id
FOREIGN KEY (se_t_id) REFERENCES trade (t_id);

-- th_t_id BIGINT NOT NULL REFERENCES trade (t_id),
ALTER TABLE trade_history ADD CONSTRAINT fk_trade_history_trade_t_id
FOREIGN KEY (th_t_id) REFERENCES trade (t_id);

-- th_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
ALTER TABLE trade_history ADD CONSTRAINT fk_trade_history_status_type_st_id
FOREIGN KEY (th_st_id) REFERENCES status_type (st_id);

-- hs_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
ALTER TABLE holding_summary_tbl ADD CONSTRAINT fk_holding_s_t_customer_a_ca_id
FOREIGN KEY (hs_ca_id) REFERENCES customer_account (ca_id);

-- hs_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
ALTER TABLE holding_summary_tbl ADD CONSTRAINT fk_holding_s_t_security_s_symb
FOREIGN KEY (hs_s_symb) REFERENCES security (s_symb);

-- h_t_id BIGINT NOT NULL REFERENCES trade (t_id),
ALTER TABLE holding ADD CONSTRAINT fk_holding_trade_t_id
FOREIGN KEY (h_t_id) REFERENCES trade (t_id);

-- h_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
ALTER TABLE holding ADD CONSTRAINT fk_holding_customer_account_ca_id
FOREIGN KEY (h_ca_id) REFERENCES customer_account (ca_id);

-- h_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
ALTER TABLE holding ADD CONSTRAINT fk_holding_security_s_symb
FOREIGN KEY (h_s_symb) REFERENCES security (s_symb);

-- hh_h_t_id BIGINT NOT NULL REFERENCES trade (t_id),
ALTER TABLE holding_history ADD CONSTRAINT fk_holding_h_trade_t_id
FOREIGN KEY (hh_h_t_id) REFERENCES trade (t_id);

-- hh_t_id BIGINT NOT NULL REFERENCES trade (t_id),
ALTER TABLE holding_history ADD CONSTRAINT fk_holding_history_trade_t_id
FOREIGN KEY (hh_t_id) REFERENCES trade (t_id);

-- wl_c_id BIGINT NOT NULL REFERENCES customer (c_id),
ALTER TABLE watch_list ADD CONSTRAINT fk_watch_list_customer_c_id
FOREIGN KEY (wl_c_id) REFERENCES customer (c_id);

-- wi_wl_id BIGINT NOT NULL REFERENCES watch_list (wl_id),
ALTER TABLE watch_item ADD CONSTRAINT fk_watch_item_watch_list_wl_id
FOREIGN KEY (wi_wl_id) REFERENCES watch_list (wl_id);

-- wi_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
ALTER TABLE watch_item ADD CONSTRAINT fk_watch_item_security_s_symb
FOREIGN KEY (wi_s_symb) REFERENCES security (s_symb);

-- ct_t_id BIGINT NOT NULL REFERENCES trade (t_id),
ALTER TABLE cash_transaction ADD CONSTRAINT fk_cash_t_trade_t_id
FOREIGN KEY (ct_t_id) REFERENCES trade (t_id);

-- ch_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id),
ALTER TABLE charge ADD CONSTRAINT fk_charge_trade_t_tt_id
FOREIGN KEY (ch_tt_id) REFERENCES trade_type (tt_id);

-- cr_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id),
ALTER TABLE commission_rate ADD CONSTRAINT fk_commission_r_trade_t_tt_id
FOREIGN KEY (cr_tt_id) REFERENCES trade_type (tt_id);

-- cr_ex_id CHAR(6) NOT NULL REFERENCES exchange (ex_id),
ALTER TABLE commission_rate ADD CONSTRAINT fk_commission_r_exchange_ex_id
FOREIGN KEY (cr_ex_id) REFERENCES exchange (ex_id);

-- tr_t_id BIGINT NOT NULL REFERENCES trade (t_id),
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_r_trade_t_id
FOREIGN KEY (tr_t_id) REFERENCES trade (t_id);

-- tr_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id),
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_r_trade_type_tt_id
FOREIGN KEY (tr_tt_id) REFERENCES trade_type (tt_id);

-- tr_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_r_security_s_symb
FOREIGN KEY (tr_s_symb) REFERENCES security (s_symb);

-- tr_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_req_customer_account_ca_id
FOREIGN KEY (tr_ca_id) REFERENCES customer_account (ca_id);

-- tr_b_id BIGINT NOT NULL REFERENCES broker (b_id),
ALTER TABLE trade_request ADD CONSTRAINT fk_trade_request_broker_b_id
FOREIGN KEY (tr_b_id) REFERENCES broker (b_id);

--
-- create indexes
--

CREATE INDEX i_c_tax_id ON customer (c_tax_id);

CREATE INDEX i_co_name ON company (co_name);

CREATE INDEX i_security ON security (s_co_id, s_issue);


