hydra.RegionPrms-names              = // non-persistent regions
                                      replicate
                                      replicateWithOverflow
                                      PR
                                      PRWithOverflow
                                      // accessors for the above persistent regions
                                      emptyReplicate1
                                      emptyReplicate2
                                      prAccessor1
                                      prAccessor2

                                      // colocated regions
                                      prColocatedAnchor
                                      prColocatedAnchorWithOverflow
                                      prColocatedFollower1
                                      prColocatedFollowerWithOverflow2
                                      prColocatedFollower3
                                      prColocatedFollowerWithOverflow4
                                      prColocatedFollower5
                                      prColocatedFollowerWithOverflow6
                                      prColocatedFollower7
                                      prColocatedFollowerWithOverflow8
                                      // accessors for the above persistent colocated regions
                                      prAccessor3
                                      prAccessor4
                                      prAccessor5
                                      prAccessor6
                                      prAccessor7
                                      prAccessor8
                                      prAccessor9
                                      prAccessor10
                                      prAccessor11
                                      prAccessor12

                                      // clientRegions for non-persistence
                                      clientReplicate
                                      clientReplicateWithOverflow
                                      clientPR
                                      clientPRWithOverflow

                                      clientPrColocatedAnchor
                                      clientPrColocatedAnchorWithOverflow
                                      clientPrColocatedFollower1
                                      clientPrColocatedFollowerWithOverflow2
                                      clientPrColocatedFollower3
                                      clientPrColocatedFollowerWithOverflow4
                                      clientPrColocatedFollower5
                                      clientPrColocatedFollowerWithOverflow6
                                      clientPrColocatedFollower7
                                      clientPrColocatedFollowerWithOverflow8

                                      // persistent regions
                                      persistReplicate
                                      persistReplicateWithOverflow
                                      persistPR
                                      persistPRWithOverflow
                                      // accessors for the above persistent regions
                                      persistEmptyReplicate1
                                      persistEmptyReplicate2
                                      persistPrAccessor1
                                      persistPrAccessor2

                                      // persistent colocated regions
                                      persistPrColocatedAnchor
                                      persistPrColocatedAnchorWithOverflow
                                      persistPrColocatedFollower1
                                      persistPrColocatedFollowerWithOverflow2
                                      persistPrColocatedFollower3
                                      persistPrColocatedFollowerWithOverflow4
                                      persistPrColocatedFollower5
                                      persistPrColocatedFollowerWithOverflow6
                                      persistPrColocatedFollower7
                                      persistPrColocatedFollowerWithOverflow8
                                      // accessors for the above persistent colocated regions
                                      persistPrAccessor3
                                      persistPrAccessor4
                                      persistPrAccessor5
                                      persistPrAccessor6
                                      persistPrAccessor7
                                      persistPrAccessor8
                                      persistPrAccessor9
                                      persistPrAccessor10
                                      persistPrAccessor11
                                      persistPrAccessor12

                                      // client regions for persistence
                                      clientPersistReplicate
                                      clientPersistReplicateWithOverflow
                                      clientPersistPR
                                      clientPersistPRWithOverflow

                                      clientPersistPrColocatedAnchor
                                      clientPersistPrColocatedAnchorWithOverflow
                                      clientPersistPrColocatedFollower1
                                      clientPersistPrColocatedFollowerWithOverflow2
                                      clientPersistPrColocatedFollower3
                                      clientPersistPrColocatedFollowerWithOverflow4
                                      clientPersistPrColocatedFollower5
                                      clientPersistPrColocatedFollowerWithOverflow6
                                      clientPersistPrColocatedFollower7
                                      clientPersistPrColocatedFollowerWithOverflow8
;

hydra.RegionPrms-regionName         = // non-persistent regions
                                      replicate_region1
                                      replicate_region2
                                      PR_region1
                                      PR_region2
                                      replicate_region1
                                      replicate_region2
                                      PR_region1
                                      PR_region2

                                      colAnchorPR_1
                                      colAnchorPR_2
                                      colPR_3
                                      colPR_4
                                      colPR_5
                                      colPR_6
                                      colPR_7
                                      colPR_8
                                      colPR_9
                                      colPR_10
                                      colAnchorPR_1
                                      colAnchorPR_2
                                      colPR_3
                                      colPR_4
                                      colPR_5
                                      colPR_6
                                      colPR_7
                                      colPR_8
                                      colPR_9
                                      colPR_10

                                      // client regions for non-persistence
                                      replicate_region1
                                      replicate_region2
                                      PR_region1
                                      PR_region2

                                      colAnchorPR_1
                                      colAnchorPR_2
                                      colPR_3
                                      colPR_4
                                      colPR_5
                                      colPR_6
                                      colPR_7
                                      colPR_8
                                      colPR_9
                                      colPR_10

                                      // persistent regions
                                      persist_replicate_region1
                                      persist_replicate_region2
                                      persist_PR_region1
                                      persist_PR_region2
                                      persist_replicate_region1
                                      persist_replicate_region2
                                      persist_PR_region1
                                      persist_PR_region2

                                      persist_colAnchorPR_1
                                      persist_colAnchorPR_2
                                      persist_colPR_3
                                      persist_colPR_4
                                      persist_colPR_5
                                      persist_colPR_6
                                      persist_colPR_7
                                      persist_colPR_8
                                      persist_colPR_9
                                      persist_colPR_10
                                      persist_colAnchorPR_1
                                      persist_colAnchorPR_2
                                      persist_colPR_3
                                      persist_colPR_4
                                      persist_colPR_5
                                      persist_colPR_6
                                      persist_colPR_7
                                      persist_colPR_8
                                      persist_colPR_9
                                      persist_colPR_10

                                      // client regions for persistence
                                      persist_replicate_region1
                                      persist_replicate_region2
                                      persist_PR_region1
                                      persist_PR_region2

                                      persist_colAnchorPR_1
                                      persist_colAnchorPR_2
                                      persist_colPR_3
                                      persist_colPR_4
                                      persist_colPR_5
                                      persist_colPR_6
                                      persist_colPR_7
                                      persist_colPR_8
                                      persist_colPR_9
                                      persist_colPR_10
;

hydra.RegionPrms-dataPolicy         = // non-persistent regions
                                      replicate
                                      replicate
                                      partition
                                      partition
                                      empty
                                      empty
                                      partition
                                      partition

                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition

                                      // client regions
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal

                                      // persistent regions
                                      persistentReplicate
                                      persistentReplicate
                                      persistentPartition
                                      persistentPartition
                                      empty
                                      empty
                                      partition
                                      partition

                                      persistentPartition
                                      persistentPartition
                                      persistentPartition
                                      persistentPartition
                                      persistentPartition
                                      persistentPartition
                                      persistentPartition
                                      persistentPartition
                                      persistentPartition
                                      persistentPartition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition
                                      partition

                                      // client regions
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
                                      normal
;
hydra.RegionPrms-diskStoreName      = none none none none none none none none none none
                                      none none none none none none none none none none
                                      none none none none none none none none none none
                                      none none none none none none none none none none
                                      none none
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      none
                                      none
                                      none
                                      none

                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
;

hydra.RegionPrms-scope              = ack
                                      ack
                                      default
                                      default
                                      ack
                                      ack
                                      default
                                      default

                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default

                                      // client regions
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default

                                      // persistent regions
                                      ack
                                      ack
                                      default
                                      default
                                      ack
                                      ack
                                      default
                                      default

                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default

                                      // client regions
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
                                      default
;

hydra.RegionPrms-cacheListeners     = util.SummaryLogListener
                                      event.LogListener
                                      util.SilenceListener;

hydra.ResourceManagerPrms-evictionHeapPercentage=80;
hydra.RegionPrms-evictionAttributes = none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruHeapPercentage default localDestroy,
                                      none,
                                      none,
                                      none,
                                      lruHeapPercentage default localDestroy,

                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruMemorySize 10 default localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruMemorySize 10 default localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,

                                      // client regions
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,

                                      // persistent regions
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      none,
                                      none,
                                      lruEntryCount 30 overflowToDisk,

                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,
                                      none,
                                      lruEntryCount 30 overflowToDisk,

                                      // client regions
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy,
                                      lruEntryCount 100 localDestroy
;

hydra.RegionPrms-partitionName      = none 
                                      none 
                                      PR 
                                      PR
                                      none
                                      none
                                      PRaccessor
                                      PRaccessor

                                      PR
                                      PR
                                      PRcolocate1
                                      PRcolocate1
                                      PRcolocate1
                                      PRcolocate1
                                      PRcolocate2
                                      PRcolocate2
                                      PRcolocate2
                                      PRcolocate2
                                      PRaccessor
                                      PRaccessor
                                      PRaccessorColocate1
                                      PRaccessorColocate1
                                      PRaccessorColocate1
                                      PRaccessorColocate1
                                      PRaccessorColocate2
                                      PRaccessorColocate2
                                      PRaccessorColocate2
                                      PRaccessorColocate2

                                      // client regions
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none

                                      // persistent regions
                                      none 
                                      none 
                                      PR 
                                      PR
                                      none
                                      none
                                      PRaccessor
                                      PRaccessor

                                      PR
                                      PR
                                      persistPRcolocate1
                                      persistPRcolocate1
                                      persistPRcolocate1
                                      persistPRcolocate1
                                      persistPRcolocate2
                                      persistPRcolocate2
                                      persistPRcolocate2
                                      persistPRcolocate2
                                      PRaccessor
                                      PRaccessor
                                      persistPRaccessorColocate1
                                      persistPRaccessorColocate1
                                      persistPRaccessorColocate1
                                      persistPRaccessorColocate1
                                      persistPRaccessorColocate2
                                      persistPRaccessorColocate2
                                      persistPRaccessorColocate2
                                      persistPRaccessorColocate2

                                      // client regions
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
;

hydra.RegionPrms-poolName           = none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none

                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none

                                      // client regions
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript

                                      // persistent regions
                                      none
                                      none
                                      none
                                      none
                                      // accessors for the above persistent regions
                                      none
                                      none
                                      none
                                      none

                                      // persistent colocated regions
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      // accessors for the above persistent colocated regions
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none
                                      none

                                      // client regions for persistence
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
                                      edgeDescript
;

hydra.DiskStorePrms-names = ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10;
hydra.DiskStorePrms-diskDirNum = ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO
                                 ONEOF 1 2 3 4 5 FOENO;
hydra.DiskStorePrms-autoCompact = ONEOF true false FOENO;
hydra.DiskStorePrms-allowForceCompaction = ONEOF true false FOENO;
hydra.DiskStorePrms-compactionThreshold = ONEOF 0 20 50 80 100 FOENO;
hydra.DiskStorePrms-maxOplogSize = RANGE 1 10 EGNAR;

hydra.PartitionPrms-names           = PRaccessor 
                                      PR      
                                      PRcolocate1    
                                      PRcolocate2    
                                      PRaccessorColocate1 
                                      PRaccessorColocate2

                                      persistPRcolocate1    
                                      persistPRcolocate2    
                                      persistPRaccessorColocate1 
                                      persistPRaccessorColocate2
;
hydra.PartitionPrms-localMaxMemory  = 0          
                                      default 
                                      default        
                                      default        
                                      0                   
                                      0

                                      default
                                      default
                                      0
                                      0
;
hydra.PartitionPrms-colocatedWith   = none       
                                      none    
                                      colAnchorPR_1  
                                      colAnchorPR_2  
                                      colAnchorPR_1       
                                      colAnchorPR_2

                                      persist_colAnchorPR_1  
                                      persist_colAnchorPR_2  
                                      persist_colAnchorPR_1       
                                      persist_colAnchorPR_2
;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};

