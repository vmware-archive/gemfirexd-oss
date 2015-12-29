// recoveryDelay is never, startupDelay is never
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= -1 startupDelay= -1
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= -1 startupDelay= -1

// recoveryDelay is never, startupDelay is immediate
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= -1 startupDelay= 0
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= -1 startupDelay= 0 

// recoveryDelay is never, startupDelay is delayed
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= -1 startupDelay= 60000 
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= -1 startupDelay= 60000 

//
// recoveryDelay is immediate, startupDelay is never
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= 0 startupDelay= -1
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= 0 startupDelay= -1

// recoveryDelay is immediate, startupDelay is immediate
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= 0 startupDelay= 0
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= 0 startupDelay= 0 

// recoveryDelay is immediate, startupDelay is delayed
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= 0 startupDelay= 60000 
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= 0 startupDelay= 60000 

//
// recoveryDelay is timed delay, startupDelay is never
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= 60000 startupDelay= -1
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= 60000 startupDelay= -1

// recoveryDelay is delayed, startupDelay is immediate
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= 60000 startupDelay= 0
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= 60000 startupDelay= 0 

// recoveryDelay is delayed, startupDelay is delayed
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = 0
   totalNumVMsToStop = 9
   recoveryDelay= 60000 startupDelay= 60000 
recovDelay/recovDelay.conf
   A=accessor accessorHosts=1 accessorVMsPerHost=1 accessorThreadsPerVM=1
   B=dataStore dataStoreHosts=10 dataStoreVMsPerHost=1 dataStoreThreadsPerVM=2
   redundantCopies = nonZero
   totalNumVMsToStop = 9
   recoveryDelay= 60000 startupDelay= 60000 

