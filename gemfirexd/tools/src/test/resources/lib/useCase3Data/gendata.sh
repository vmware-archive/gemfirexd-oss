#!/bin/sh

# $1 - hostname:port
# $2 - number of rows
java -cp .:/soubhikc1/builds/gemfirexd_rebrand_Dec13/build-artifacts/linux/product-gfxd/lib/gemfirexd-client.jar S2DataGenerator APP.TX_PL_USER_POSN_MAP,APP.TF_PL_POSITION_PTD,APP.TF_PL_POSITION_YTD,APP.TF_PL_ADJ_REPORT,APP.TD_TRADER_SCD,APP.TL_SOURCE_SYSTEM,APP.TD_POSN_EXTENDED_KEY,APP.TD_INSTRUMENT_SCD $1 $2 /soubhikc1/poc/useCase3/try/mapping
