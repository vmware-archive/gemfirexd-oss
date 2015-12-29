// Default System Statistics
// $JTESTS/perffmwk/statistics.spec

// memory usage
statspec SolarisRssSize * SolarisProcessStats * rssSize
  filter=none combine=raw ops=min,max,mean trimspec=default
;
statspec LinuxRssSize * LinuxProcessStats * rssSize
  filter=none combine=raw ops=min,max,mean trimspec=default
;
statspec freeMemory * VMStats vmStats freeMemory
  filter=none combine=raw ops=min,max,mean trimspec=default
;
statspec maxMemory * VMStats vmStats maxMemory
  filter=none combine=raw ops=min,max,mean trimspec=default
;
statspec totalMemory * VMStats vmStats totalMemory
  filter=none combine=raw ops=min,max,mean trimspec=default
;
