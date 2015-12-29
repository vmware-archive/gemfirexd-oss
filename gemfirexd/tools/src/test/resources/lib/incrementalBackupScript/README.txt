Following scripts creates 1 locator and 4 data nodes gfxd cluster (each with 4GB memory). 
Then, install and execute DAP. 

Steps are given below:
1) Edit 'setenv' to set build locations and execute it.
	. ./setenv

2) Start the cluster
	./start-gfxd-cluster.sh

3) Create schema
	./exec-create-schema.sh

4) Import data
	./exec-import-data.sh
	
5) Do DDL/DML operations

6) Install/replace/remove DAP JAR
	./exec-install-dap-jar.sh

7) Create DAP in sqlFire 
	./exec-create-dap.sh

8) Execute DAP
	./exec-dap.sh

9) Stop the cluster	
	./stop-gfxd-cluster.sh