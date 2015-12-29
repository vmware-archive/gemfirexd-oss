hydra.RegionPrms-names              = // persistent regions
                                      persistReplicate
                                      persistReplicateWithOverflow
                                      persistPR
                                      persistPRWithOverflow
                                      // accessors for the above persistent regions
                                      emptyReplicate1
                                      emptyReplicate2
                                      prAccessor1
                                      prAccessor2

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
                                      prAccessor3
                                      prAccessor4
                                      prAccessor5
                                      prAccessor6
                                      prAccessor7
                                      prAccessor8
                                      prAccessor9
                                      prAccessor10
                                      prAccessor11
                                      prAccessor12;

hydra.RegionPrms-regionName         = replicate_region1
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
                                      colPR_10;

hydra.RegionPrms-dataPolicy         = persistentReplicate
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
                                      partition;

hydra.RegionPrms-diskStoreName      = ONEOF ds1 ds2 ds3 ds4 ds5 ds6 ds7 ds8 ds9 ds10 FOENO
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
                                      none;

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
                                      default;

hydra.RegionPrms-diskSynchronous    = ONEOF true false FOENO;

hydra.RegionPrms-cacheListeners     = util.SummaryLogListener;

hydra.RegionPrms-evictionAttributes = none,
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
                                      lruEntryCount 30 overflowToDisk;

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
                                      PRaccessorColocate2;

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
hydra.DiskStorePrms-autoCompact = true;
hydra.DiskStorePrms-maxOplogSize = 1; // frequent new logs

hydra.PartitionPrms-names           = PRaccessor PR      PRcolocate1    PRcolocate2    PRaccessorColocate1 PRaccessorColocate2;
hydra.PartitionPrms-localMaxMemory  = 0          default default        default        0                   0;
hydra.PartitionPrms-colocatedWith   = none       none    colAnchorPR_1  colAnchorPR_2  colAnchorPR_1       colAnchorPR_2;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};

hydra.CachePrms-pdxPersistent = true;
hydra.CachePrms-pdxDiskStoreName = ds1;

pdx.PdxPrms-shutDownAll = ${shutDownAll};
