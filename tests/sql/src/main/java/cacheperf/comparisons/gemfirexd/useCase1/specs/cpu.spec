//------------------------------------------------------------------------------
// cpu
//------------------------------------------------------------------------------

statspec inboundClients *inclient* UseCase1Stats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=inbound
;
statspec cpuInboundClient *inclient* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=inbound
;
expr CPU_Inbound_Client = cpuInboundClient / inboundClients ops = mean?
;

statspec outboundClients *outclient* UseCase1Stats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=outbound
;
statspec cpuOutboundClient *outclient* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=outbound
;
expr CPU_Outbound_Client = cpuOutboundClient / outboundClients ops = mean?
;

statspec meServers *data_*me* UseCase1Stats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=inbound
;
statspec cpuMEServer *data_*me* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=inbound
;
expr CPU_ME_Server = cpuMEServer / meServers ops = mean?
;

statspec etlServers *data_*etl* UseCase1Stats * vmCount
filter=none combine=combineAcrossArchives ops=mean! trimspec=inbound
;
statspec cpuETLServer *data_*etl* SystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean! trimspec=inbound
;
expr CPU_ETL_Server = cpuETLServer / etlServers ops = mean?
;
