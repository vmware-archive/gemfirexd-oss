// hydra parameters to make friendly toward java debuggers.

// besides including the default mess of vm options, this will start a 
// JPDA server socket in the client vms, the port should be stated on 
// the first line in the bgexec log.
hydra.VmPrms-extraVMArgs = -Xdebug "-Xrunjdwp:transport=dt_socket,suspend=n,server=y";

// if stepping through routines, one doesn't want hydra to give up on us while we think... 
hydra.Prms-maxClientShutdownWaitSec = 360000;
hydra.Prms-maxClientStartupWaitSec = 360000;
hydra.Prms-maxEndTaskResultWaitSec = 360000;
hydra.Prms-maxHostAgentShutdownWaitSec = 360000;
hydra.Prms-maxHostAgentStartupWaitSec = 360000;
hydra.Prms-maxResultWaitSec = 360000;

