//------------------------------------------------------------------------------
// cpu
//------------------------------------------------------------------------------

statspec clients *client* UseCase5Stats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=transactions
;
statspec cpuClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=transactions
;
expr vmCPUClient = cpuClient / clients ops = mean?
;

statspec servers *server* UseCase5Stats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=transactions
;
statspec cpuServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=transactions
;
expr vmCPUServer = cpuServer / servers ops = mean?
;
