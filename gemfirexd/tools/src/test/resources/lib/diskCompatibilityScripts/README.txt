Following scripts creates 1 locator and 4 data nodes gfxd cluster (each with 4GB memory). 
Then, install and execute DAP. 

Steps are given below:
1) Edit 'setenv' to set build locations and execute it.
	. ./setenv

2) Upgrade sqlfire diskstore
    ./upgrade_diskstores.sh

3) Start the cluster
	./start-gfxd-cluster.sh

4) Create schema
	./exec-create-temp-schema.sh

5) Import data
	./exec-import-temp-data.sh
	
6) Do DDL/DML operations

7) Install/replace/remove DAP JAR
	./exec-install-dap-jar.sh

8) Create DAP in sqlFire 
	./exec-create-dap.sh

9) Execute DAP
	./exec-dap.sh

10) Stop the cluster	
	./stop-gfxd-cluster.sh