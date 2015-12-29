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

hydra.RegionPrms-dataPolicy         = replicate
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
                                      partition;

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

hydra.RegionPrms-cacheListeners     = util.SummaryLogListener;

hydra.RegionPrms-evictionAttributes = none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      none,
                                      none,
                                      lruEntryCount 100 localDestroy,

                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy,
                                      none,
                                      lruEntryCount 100 localDestroy;

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

hydra.PartitionPrms-names           = PRaccessor PR      PRcolocate1    PRcolocate2    PRaccessorColocate1 PRaccessorColocate2;
hydra.PartitionPrms-localMaxMemory  = 0          default default        default        0                   0;
hydra.PartitionPrms-colocatedWith   = none       none    colAnchorPR_1  colAnchorPR_2  colAnchorPR_1       colAnchorPR_2;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};

