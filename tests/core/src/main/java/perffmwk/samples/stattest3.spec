// Statistics Specification
// perffmwk/samples/stattest1.conf
// Mon Aug 11 17:12:23 PDT 2003

//==============================================================================
// User-Defined Statistics

statspec loops gemfire* perffmwk.samples.SampleVMStatistics VM-* loops
  filter=none combine=raw ops=max-min trimspec=default
;
statspec opTime gemfire* perffmwk.samples.SampleThreadStatistics Thread-0 operationTime
  filter=persecond combine=raw ops=mean trimspec=default
;
statspec ops gemfire* perffmwk.samples.SampleThreadStatistics Thread-0 operations
  filter=persecond combine=raw ops=min,max,mean,stddev trimspec=default
;

//==============================================================================
// Key System Statistics

include $JTESTS/perffmwk/statistics.spec
;
