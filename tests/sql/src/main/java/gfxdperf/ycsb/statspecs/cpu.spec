//------------------------------------------------------------------------------
// main: cpu
//------------------------------------------------------------------------------

statspec clients *client* YCSBStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=main
;
statspec cpuClient *client* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=main
;
expr vmCPUClient = cpuClient / clients ops = mean?
;

statspec servers *server* YCSBStats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=main
;
statspec cpuServer *server* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=main
;
expr vmCPUServer = cpuServer / servers ops = mean?
;
