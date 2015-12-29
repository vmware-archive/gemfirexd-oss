
This directory contains configuration files that define hydra and GemFire test
configurations.  Most of the configurations defined here are intended to provide
common setup information that can used with a number of different specific
tests.  They do not define any test tasks.

See the hydratest package for example tests that are used to test hydra itself.
See the hydra/samples package for example tests that use both hydra and GemFire.

To verify that you've set up your hydra environment correctly, run the test
"hydratest/checkhydra.conf".  To verify that you have the needed access to
GemFire, run "hydratest/checksystem1.conf".

To run a hydra test, first read the hydra package javadocs, then follow the
instructions in the hydra.MasterController or batterytest.Batterytest javadocs.

//------------------------------------------------------------------------------
// SELECTING A GEMFIRE TOPOLOGY INCLUDE FILE
//------------------------------------------------------------------------------

These files use client-managed locators and have scaling knobs for WAN sites,
hosts, VMs, and threads:

        topology_2_locator.inc
        topology_3_locator.inc
        topology_4_locator.inc
        topology_hct_locator.inc
        topology_p2p_locator.inc
        topology_p2p_1_locator.inc
        topology_p2p_2_locator.inc
        topology_wan_hct_locator.inc
        topology_wan_p2p_locator.inc

Tests using these include files can be configured with multiple locators,
assign tasks to locators, dynamically stop/start locators, and be extended
for split-brain testing.

These files are the same except for using master-managed locators, and are
pretty much obsolete:

        topology_2.inc
        topology_3.inc
        topology_4.inc
        topology_hct.inc
        topology_p2p.inc
        topology_p2p_1.inc
        topology_p2p_2.inc
        topology_wan_hct.inc
        topology_wan_p2p.inc

Tests using these include files have a single master-managed locator per
non-loner distributed system, and cannot be extended for split-brain tests.

These files are obsolete and should not be used by new tests:

        systemparams1.inc
        systemparams2.inc
        systemparams3.inc
        systemparams4.inc
        systemparamsN.inc
