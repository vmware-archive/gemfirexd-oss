

--
-- create indexes
--

SET SCHEMA TPCEGFXD;


CREATE INDEX i_tr_s_symb ON trade_request (tr_s_symb);

CREATE INDEX i_c_tax_id ON customer (c_tax_id);

CREATE INDEX i_co_name ON company (co_name);

CREATE INDEX i_security ON security (s_co_id, s_issue);


