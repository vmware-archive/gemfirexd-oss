--------------------------------------------------------------------------------
How to run single-host tests:

1) Reserve <some_big_host> for exclusive use.

2) Run test.bt on host <some_big_host> before your changes.
   Save the results to <beforeDir>

3) Run test.bt on host <some_big_host> after your changes.
   Save the results to <afterDir>

4) Run compareperf.sh on <beforeDir> and <afterDir>.

5) Look at perfreport.txt to see if changes were for the better (positive).
   In particular, see if putsPerSecond is around 1000 (this is the throttled
   feed rate).  And especially look at the updateLatency.

--------------------------------------------------------------------------------
How to run multi-host tests:

1) Reserve the set of hosts for exclusive use.

2) Divide the hosts amongst feeds, bridges, and edges in the local.conf below.

3) Run test.bt as above using the local.conf.

--------------------------------------------------------------------------------
SAMPLE: local.conf for test.bt (1 feed, 2 bridge, and 10/50/100 edge hosts)
--------------------------------------------------------------------------------
// This sample uses bensa for the single feed host, bensb and bensc for the two
// bridge hosts, and maps the 10, 50, 100 edge hosts onto five other bensleys.
// Change the hostnames to ones you have reserved.
hydra.HostPrms-hostNames =
  fcn
      "hydra.TestConfigFcns.pool(\"bensa\", ${feedHosts})"
  ncf
  fcn
      "hydra.TestConfigFcns.pool(\"bensb bensc\", ${bridgeHosts})"
  ncf
  fcn
      "hydra.TestConfigFcns.pool(\"bensd bense bensf bensg bensh\", ${edgeHosts})"
  ncf
  ;
hydra.HostPrms-resourceDirBaseMapFileName = /home/lises/bin/hostmap.prop;
