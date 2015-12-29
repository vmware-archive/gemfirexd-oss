//
// general purpose histogram with bins ranging from 1 millisecond to 10 seconds
//

perffmwk.HistogramStatsPrms-enable = ${enableHistogram};

perffmwk.HistogramStatsPrms-binVals = 1000000   // 1 millisecond
                                      2000000
                                      5000000
                                      10000000
                                      100000000
                                      1000000000
                                      10000000000  // 10 seconds
                                      ;
