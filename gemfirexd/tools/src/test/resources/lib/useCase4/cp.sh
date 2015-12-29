#!/bin/bash

# java -cp .:/soubhikc1/builds/gemfirexd_rebrand_Dec13/build-artifacts/linux/product-gfxd/lib/gemfirexd-client.jar DataGenerator APP.QUOTE,APP.ACCOUNTPROFILE,APP.ACCOUNT,APP.HOLDING,APP.ORDERS localhost:55443 100,100,100,100000,1000000 mapping

cp QUOTE.dat /soubhikc1/projects/gemfirexd_rebrand_Dec13/gemfirexd/GemFireXDTests/lib/useCase4/QUOTE-$1k.dat
cp HOLDING.dat /soubhikc1/projects/gemfirexd_rebrand_Dec13/gemfirexd/GemFireXDTests/lib/useCase4/HOLDING-$1k.dat
cp ACCOUNTPROFILE.dat /soubhikc1/projects/gemfirexd_rebrand_Dec13/gemfirexd/GemFireXDTests/lib/useCase4/ACCOUNTPROFILE-$1k.dat
cp ACCOUNT.dat /soubhikc1/projects/gemfirexd_rebrand_Dec13/gemfirexd/GemFireXDTests/lib/useCase4/ACCOUNT-$1k.dat
cp ORDERS.dat /soubhikc1/projects/gemfirexd_rebrand_Dec13/gemfirexd/GemFireXDTests/lib/useCase4/ORDERS-$1k.dat

