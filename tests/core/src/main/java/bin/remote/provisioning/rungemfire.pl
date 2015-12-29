#! /bin/perl -w
# -------------------------
# rungemfire.pl -- provision remote system(s), run tests, and then
# retrieve results.
#
# Most people should make a copy of runremote.sh and modify it, instead
# of using this script directly.
#
# Detailed documentation of this program is in rungemfire.txt.
# -------------------------

use Cwd;

my(
  $SCRATCHPARENT,	# parent directory for results on SUT
  $bt,			# the batterytest (.bt) file to run
  $CONF,		# the local.conf
  $copyJDK,		# whether to copy from JDKSOURCE to JDK
  $copyBUILD,		# whether to copy to $REMOTE_BUILD
  $hostList,		# list of hosts to manage, space-separated
  $JDKSOURCE,		# path to a local JDK (if $copyJDK)
  $OSBUILD,		# local build artifacts directory (if $copyBUILD)
  $MASTER,              # hydra master machine
#  $nukePattern,		# pattern to nuke with
  $noHostVerify,	# if true, don't verify hosts
  $noRemoteVerify,	# if true, don't disturb SUT for verification purposes
  $noExecute,		# if 1, only print parameters
  $RESULTDIR, 		# where to put the test results when done
  $JDK,			# JDK on SUT
  $REMOTE_BUILD,	# build artifacts on SUT
  $SCRATCH,		# list of scratch folders, space-separated
  $USER,		# username (for master and other machines)
  $verbose,		# more messages
  $NTPHOST,		# host against which to synchronize time
  );

my($status);		# for the system() command
my($PWD) = cwd();
my($SCP) = "scp -B -q";
my($SSH) = "ssh -o BatchMode=yes";
my($ZIP) = "zip -qr";
my($UNZIP) = "unzip -qo";
my($startTime) = time;
my(@hostList);
my(@scratchList);

&setDefaults();
&getArgs();
@hostList = split(/ /, $hostList);
@scratchList = split(/ /, $SCRATCH);

my($ok) = &validateArgs();
&displayConfiguration();
if (!$ok) {
  print "Errors detected in configuration; exiting\n";
  exit 1;	# don't print time summary
  }
if ($noExecute) {
  print "Configuration analysis only; exiting\n";
  exit 0;	# don't print time summary
  }

# Start by fixing clock skew.
foreach $each (@hostList) {
  my($status) = &serviceExists($each, "ntpd");
  if ($status == 0) {
    print "  [$each: ntpd verified]\n" if $verbose;
    next;
    }

  print "$each: ntpd not detected, attempting restart\n";
  my($cmd);

  # Yank clock back to correct
  $cmd = "$SSH $USER\@$each sudo /usr/sbin/ntpdate -su $NTPHOST";
  $status = &exec($cmd);
  if ($status == 0) {
    print "  [$each: ntpdate successful]\n" if $verbose;
    }
  else {
    print "WARNING: unable to ntpdate on $each\n";
    }

  # Restart the drift daemon
  my($f) = "/etc/init.d/xntpd"; # SuSE 9
  if (&remoteFileExists($each, $f)) {
    $f = "/etc/init.d/ntp"; # SuSE 10
    if (&remoteFileExists($each, $f)) {
      print "  WARNING: don't know how to restart xntp on $each\n";
      $f = "";
      }
    }

  if ($f ne "") {
    $cmd = "$SSH $USER\@$each sudo $f restart";
    $status = &exec($cmd);
    if ($status == 0) {
      print "  [$each: xntpd restart successful]\n" if $verbose;
      }
    else {
      print "WARNING: unable to restart xntpd on $each (sudo $f restart)\n";
      }
    }
  }

if ($copyJDK) {
  my($x, $y) = &basename($JDK);
  if (&putTree($JDKSOURCE, $MASTER, $x, $y)) {
    print "Error creating remote JDK\n";
    &exit(1);
    }
  }

if ($copyBUILD) {
  my($x, $y) = &basename($REMOTE_BUILD);
  if (&putTree($OSBUILD, $MASTER, $x, $y)) {
    print "Error creating remote REMOTE_BUILD\n";
    &exit(1);
    }
  }

my($resultParentDir, $resultName) = &basename($RESULTDIR);

# Create scratch directories
my($each);
foreach $each (@scratchList) {
  my($cmd);
  $status = &remoteMkdir($MASTER, $each);
  if ($status != 0) {
    print "Unable to create scratch directory $each\n";
    &exit(1);
    }
  print "  [Created directory $MASTER:$each\n" if $verbose;
  }

# Run the test
&runRemote();
# Keep going, in spite of errors.

# Kill any errant processes
#foreach $each (@hostList) {
#  &remoteNuke($each, $nukePattern, 9);
#  }

# Retrieve the results
&getTree($MASTER, "$SCRATCHPARENT/$resultName", $resultParentDir, $resultName);

# Clean up all debris
foreach $each (@scratchList) {
  &removeRemoteDir($MASTER, $each);
  print "  [Removed $MASTER:$each\n" if $verbose;
  }

&removeRemoteDir($MASTER, "$SCRATCHPARENT/$resultName");
print "  [Removed $MASTER:$SCRATCHPARENT/$resultName]\n" if $verbose;

&exit(0);

# ----------------------------------
# Set default parameters
sub setDefaults() {
  $USER = $ENV{"USER"};
  if (!defined($USER) || $USER eq "") {
    $USER = "user";
    }

  my($disk);
  $disk = $PWD;
  if ($disk !~ m#^/export/([^/]+)/.*#) {
    $disk = "mydisk";
    }
  else {
    $disk = $1;
    }

  $bt = "test.bt";
  $CONF = "local.conf";
  $copyJDK = 0;
  $copyBUILD = 0;
  $RESULTDIR = "/export/$disk/users/$USER/results";

  $JDKSOURCE = "/gcm/where/jdk/1.5.0_05/x86.linux";
  $OSBUILD = "/export/$disk/users/$USER/gemfire_obj";

  # $hostList = "bensa bensc bensd bense bensf bensb";
  # $MASTER = "bensa";
  # $REMOTE_BUILD = "/export/bensa1/users/$USER/gemfire_obj";

  $hostList = "ptesta ptestc ptestd pteste ptestf ptestb";
  $MASTER = "ptesta";
  $REMOTE_BUILD = "/export/ptesta1/users/build/gemfire_obj";

  $noExecute = 0;
  $noHostVerify = 1;	# too expensive, leave it off
  $noRemoteVerify = 0;
  $verbose = 0;


# Although we could make it user-sensitive,
# it's better to use predefined directories that are
# chmod 777 (for sharing)

#  $SCRATCHPARENT = "/export/bensa1/users/$USER";
#  $JDK = "/export/bensa1/users/$USER/j2sdk1.4.2_09";
#  $SCRATCH = "/export/bensa1/users/$USER/work/scratch";
#  $SCRATCH .= " /export/bensc1/users/$USER/work/scratch";
#  $SCRATCH .= " /export/bensd1/users/$USER/work/scratch";
#  $SCRATCH .= " /export/bense1/users/$USER/work/scratch";
#  $SCRATCH .= " /export/bensf1/users/$USER/work/scratch";

#  On bensleys, use jpenney (runqueue.pl runs with this uid)
#  $SCRATCHPARENT = "/export/bensa1/users/jpenney/perfresults";
#  $JDK = "/export/bensa1/users/jpenney/j2sdk1.4.2_09";
#  $SCRATCH = "/export/bensa1/users/jpenney/perfresults/scratch";
#  $SCRATCH .= " /export/bensc1/users/jpenney/perfresults/scratch";
#  $SCRATCH .= " /export/bensd1/users/jpenney/perfresults/scratch";
#  $SCRATCH .= " /export/bense1/users/jpenney/perfresults/scratch";
#  $SCRATCH .= " /export/bensf1/users/jpenney/perfresults/scratch";

  # On ptest, use well-defined defaults
  $SCRATCHPARENT = "/export/ptesta1/users/build/perfresults";
  $JDK = "/export/ptesta1/users/build/jdk1.5.0_05";
  $SCRATCH = "/export/ptesta1/users/build/perfresults/scratch";
  $SCRATCH .= " /export/ptestc1/users/build/perfresults/scratch";
  $SCRATCH .= " /export/ptestd1/users/build/perfresults/scratch";
  $SCRATCH .= " /export/pteste1/users/build/perfresults/scratch";
  $SCRATCH .= " /export/ptestf1/users/build/perfresults/scratch";
  $SCRATCH .= " /export/ptestb1/users/build/perfresults/scratch";

  my($y, $x) = &basename($JDK);
#  $nukePattern = $x;
  }


# ----------------------------------
# Obligatory usage message
sub usage() {
  print "Usage: $0 <args>\n";
  print "\n";
  print "Arguments:\n";
  print "  -n\n";
  print "    Only print parameters, do not execute\n";
  print "  -v\n";
  print "    Verbose\n";
  print "  BT=<fname>          (default \"$bt\")\n";
  print "    BT file for BatteryTest\n";
  print "  CONF=<fname>        (default \"$CONF\")\n";
  print "    local.conf file for Hydra\n";
  print "  copyBUILD\n";
  print "    copy from OSBUILD to REMOTE_BUILD\n";
  print "  copyJDK\n";
  print "    copy from JDKSOURCE to JDK\n";
  print "  hostlist=<space separated string>\n";
  print "    list of hosts to manage, default = \"$hostList\"\n";
  print "  JDK=<dirname>       (default \"$JDK\")\n";
  print "    JDK to use on SUT\n";
  print "  JDKSOURCE=<dirname>  (default \"$JDKSOURCE\")\n";
  print "    JDK source on this system to copy (if copyjdk)\n";
  print "  MASTER=hostname     (default \"$MASTER\")\n";
  print "    (Hydra) master for running test\n";
  print "  nohostverify\n";
  print "    Skip host ping verification, since it is slow\n";
  print "  noremoteverify\n";
  print "    Don't perform verification steps that disturb SUT\n";
#  print "  nuke=<pattern>\n";
#  print "    Pattern to nuke errant processes (default \"$NUKE\"\n";
  print "  OSBUILD=<dirname>  (default \"$OSBUILD\")\n";
  print "    obj source on this system to copy (if copyBUILD)\n";
  print "  RESULTDIR=<dirname> (default \"$RESULTDIR\)\n";
  print "    Result directory on your system where test results will be put\n";
  print "  REMOTE_BUILD=<dirname>       (default: \"$REMOTE_BUILD\")\n";
  print "    Product on SUT to test\n";
  print "  SCRATCH=<dirname>   (default \"$SCRATCH\")\n";
  print "    list of scratch folders on individual systems\n";
  print "  SCRATCHPARENT=<dirname>      (default \"$SCRATCHPARENT\")\n";
  print "    parent directory for test results on SUT)\n";
  print "  USER=username       (default \"$USER\")\n";
  print "    username on master for running test\n";

  exit 1;	# don't print time summary
  }

# ----------------------------------
# Parse all arguments
sub getArgs() {
  # TODO: need to parameterize NTPHOST

  $NTPHOST = "192.168.50.1";
  if ($#ARGV == -1) {
    &usage();
    }
  for (;;) {
    my($arg) = shift @ARGV;
    last unless defined($arg);

    if ($arg =~ /^-h$/i || $arg =~ /^--help$/i || $arg =~ /^help$/i ) {
      &usage();
      }
    if ($arg =~ /^-n$/i) {
      $noExecute = 1;
      next;
      }
    if ($arg =~ /^-v$/i) {
      $verbose = 1;
      next;
      }
    if ($arg =~ /^bt=(.*)$/i) {
      $bt = $1;
      next;
      }
    if ($arg =~ /^CONF=(.*)$/i) {
      $CONF = $1;
      next;
      }
    if ($arg =~ /^copyJDK$/i) {
      $copyJDK = 1;
      next;
      }
    if ($arg =~ /^copyBUILD$/i) {
      $copyBUILD = 1;
      next;
      }
    if ($arg =~ /^hostList=(.*)$/i) {
      $hostList = $1;
      next;
      }
    if ($arg =~ /^JDK=(.*)$/i) {
      $JDK = $1;
      next;
      }
    if ($arg =~ /^JDKSOURCE=(.*)$/i) {
      $JDKSOURCE = $1;
      next;
      }
    if ($arg =~ /^MASTER=(.*)$/i) {
      $MASTER = $1;
      next;
      }
    if ($arg =~ /^nohostverify$/i) {
      $noHostVerify = !$noHostVerify; # flip the bit
      next;
      }
    if ($arg =~ /^noremoteverify$/i) {
      $noRemoteVerify = !$noRemoteVerify; # flip the bit
      next;
      }
#    if ($arg =~ /^NUKE=(.*)$/i) {
#      $nukePattern = $1;
#      next;
#      }
    if ($arg =~ /^OSBUILD=(.*)$/i) {
      $OSBUILD = $1;
      next;
      }
    if ($arg =~ /^RESULTDIR=(.*)$/i) {
      $RESULTDIR = $1;
      next;
      }
    if ($arg =~ /^REMOTE_BUILD=(.*)$/i) {
      $REMOTE_BUILD = $1;
      next;
      }
    if ($arg =~ /^SCRATCH=(.*)$/i) {
      $SCRATCH = $1;
      next;
      }
    if ($arg =~ /^SCRATCHPARENT=(.*)$/i) {
      $SCRATCHPARENT = $1;
      next;
      }
    if ($arg =~ /^USER=(.*)$/i) {
      $USER = $1;
      next;
      }

    print "Unknown argument \"$arg\"\n";
    &usage();
    }
  }

# ----------------------------------
sub validateArgs() {
  my($ok) = 1;
  my($masterExists) = 1;

  if ($noHostVerify) {
      print "  [SKIPPED: host checks since noHostVerify]\n" if $verbose;
    }
  else {
    if (&hostExists($MASTER)) {
      print "Remote host $MASTER does not exist\n";
      $masterExists = 0;
      $ok = 0;
      }
    else {
      print "  [ok: host $MASTER: exists]\n" if $verbose;
      }

    my($each);
    foreach $each (@hostList) {
      if (&hostExists($each)) {
	print "Remote host $each does not exist\n";
	$ok = 0;
	}
      else {
	print "  [ok: host $each: exists]\n" if $verbose;
	}
      }
    }

  if ($noRemoteVerify) {
    print "  [SKIPPED: remote SCRATCHPARENT check since noRemoteVerify]\n" if $verbose;
    }
  elsif (!$masterExists) {
      print "  [SKIPPED: remote SCRATCHPARENT check since $MASTER is missing]\n"
	  if $verbose;
    }
  elsif (&remoteDirExists($MASTER, $SCRATCHPARENT)) {
    print "Remote SCRATCHPARENT $MASTER:$SCRATCHPARENT does not exist\n";
    $ok = 0;
    }
  elsif (&remoteWriteable($MASTER, $SCRATCHPARENT)) {
    print "Remote SCRATCHPARENT $MASTER:$SCRATCHPARENT not writeable\n";
    $ok = 0;
    }
  else {
    print "  [ok: SCRATCHPARENT $MASTER:$SCRATCHPARENT exists]\n" if $verbose;
    }

  if (! -r $bt) {
    print "BT file \"$bt\" does not exist (or is not readable).\n";
    $ok = 0;
    }
  else {
    print "  [ok: bt $bt exists]\n" if $verbose;
    }
  if (! -r $CONF) {
    print "CONF file \"$CONF\" does not exist (or is not readable).\n";
    $ok = 0;
    }
  else {
    print "  [ok: CONF $CONF exists]\n" if $verbose;
    }
  if ($copyJDK) {
    my($d, $b) = &basename($JDK);

    if ($noRemoteVerify) {
      print "  [SKIPPED: parent of JDK check since noRemoteVerify]\n"
	  if $verbose;
      }
    elsif (!$masterExists) {
      print "  [SKIPPED: parent of JDK check since $MASTER is missing]\n"
	  if $verbose;
      }
    elsif (&remoteDirExists($MASTER, $d)) {
      print "Parent directory for target JDK $MASTER:$d does not exist\n";
      $ok = 0;
      }
    elsif (&remoteWriteable($MASTER, $d)) {
      print "Parent directory for target JDK $MASTER:$d not writeable\n";
      $ok = 0;
      }
    else {
      print "  [ok: JDK parent $MASTER:$d exists]\n" if $verbose;
      }
    if (! -d $JDKSOURCE) {
       print "JDKSOURCE \"$JDKSOURCE\" is not a directory.\n";
       $ok = 0;
       }
    else {
      print "  [ok: JDKSOURCE $JDKSOURCE exists]\n" if $verbose;
      }
    }
  else {
    if ($noRemoteVerify) {
      print "  [SKIPPED: JDK check since noRemoteVerify]\n" if $verbose;
      }
    elsif (!$masterExists) {
      print "  [SKIPPED: JDK check since $MASTER is missing]\n" if $verbose;
      }
    elsif (&remoteDirExists($MASTER, $JDK)) {
      print "JDK $MASTER:$JDK does not exist\n";
      $ok = 0;
      }
    else {
      print "  [ok: JDK $MASTER:$JDK exists]\n" if $verbose;
      }
    }
  if ($copyBUILD) {
    my($d, $b) = &basename($REMOTE_BUILD);

    if ($noRemoteVerify) {
      print "  [SKIPPED: parent of REMOTE_BUILD check since noRemoteVerify]\n"
	  if $verbose;
      }
    elsif (!$masterExists) {
      print "  [SKIPPED: parent of REMOTE_BUILD check since $MASTER is missing]\n"
	  if $verbose;
      }
    elsif (&remoteDirExists($MASTER, $d)) {
      print "Parent directory for target REMOTE_BUILD $MASTER:$d does not exist\n";
      $ok = 0;
      }
    elsif (&remoteWriteable($MASTER, $d)) {
      print "Parent directory for target REMOTE_BUILD  $MASTER:$d not writeable\n";
      $ok = 0;
      }
    else {
      print "  [ok: parent of REMOTE_BUILD $MASTER:$d exists]\n" if $verbose;
      }
    if (! -d $OSBUILD) {
       print "OSBUILD \"$OSBUILD\" is not a directory.\n";
       $ok = 0;
       }
    else {
      print "  [ok: OSBUILD $OSBUILD exists]\n" if $verbose;
      }
    }
  else {
    if ($noRemoteVerify) {
      print "  [SKIPPED: REMOTE_BUILD check since noRemoteVerify]\n" if $verbose;
      }
    elsif (!$masterExists) {
      print "  [SKIPPED: REMOTE_BUILD check since $MASTER is missing]\n" if $verbose;
      }
    elsif (&remoteDirExists($MASTER, $REMOTE_BUILD)) {
      print "REMOTE_BUILD $MASTER:$REMOTE_BUILD does not exist\n";
      $ok = 0;
      }
    else {
      print "  [ok: REMOTE_BUILD $MASTER:$REMOTE_BUILD exists]\n" if $verbose;
      }
    }
  my($d, $b) = &basename($RESULTDIR);
  if (! -d $d) {
    print "Parent of RESULTDIR $d is not a directory.\n";
    $ok = 0;
    }
  else {
    print "  [ok: RESULTDIR parent $d exists]\n" if $verbose;
    }

  # TODO verify that $SCRATCH is OK

  return $ok;
  }


# ----------------------------------
sub displayConfiguration() {
  print "---------------------------------\n";
  if ($noExecute) {
    print "Resultant configuration: (will not be run)\n";
    }
  else {
    print "Resultant configuration: (WILL be run)\n";
    }
  print "SCRATCHPARENT = $SCRATCHPARENT\n";
  print "BT            = $bt\n";
  print "CONF          = $CONF\n";
  print "hostList      = ";
  my ($each);
  foreach $each (@hostList) {
    print "$each ";
    }
  print "\n";
  if ($copyJDK) {
    print "--> Copy JDK from $JDKSOURCE to $JDK\n";
    }
  else {
    print "JDK        = $JDK\n";
    }
  print "MASTER       = $MASTER\n";
  print "USER         = $USER\n";
#  print "NUKE          = $nukePattern\n";
  if ($copyBUILD) {
    print "--> Copy REMOTE_BUILD from $OSBUILD to $REMOTE_BUILD\n";
    }
  else {
    print "REMOTE_BUILD = $REMOTE_BUILD\n";
    }
  print "RESULTDIR    = $RESULTDIR\n";
  print "SCRATCH      = $SCRATCH\n";
  print "---------------------------------\n";
  }


# ----------------------------------
# Basename function
#  Arguments: $arg -- a filename
#  Returns ($dirname, $basename) -- separated without the intervening slash
sub basename() {
  my($arg) = @_;
  my($zdir, $zbase);

  if ($arg !~ m#/#) {
    return ("", $arg);
    }
  if ($arg !~ m#^(.*)/([^/]+)#) {
    print "basename: bogus filename \"$arg\"\n";
    &exit(1);		# should not happen
    }
  $zdir = $1;
  $zbase = $2;

  return ($zdir, $zbase);
  }

# ----------------------------------
# Execute a command
# Arguments: $cmd - command to execute
# Returns: 0 on success
sub exec() {
  my($cmd) = @_;
  my($status) = system "$cmd";
  return -1 if $status == 0xff00;
  if ($status > 0x80) {
    $status >>= 8;
    return $status;
    }
  return 0 if $status == 0;
  return 1;
  }

# ----------------------------------
# Delete a remote file
# Args: $zhost - the host on which the file lives;
#       $zname - name of the  file on that host.
# Returns: status, 0 on success
sub removeRemoteFile() {
  my($zhost, $zname) = @_;

  my($cmd);
  $cmd  = "  if [ -f \\\"$zname\\\" ]; then ";
  $cmd .= "    chmod ugo+rwx \\\"$zname\\\"";
  $cmd .= ";   rm -f \\\"$zname\\\"";
  $cmd .= "; fi";
  $cmd .= "; if [ -f \\\"$zname\\\" ]; then ";
  $cmd .= "    echo \"File $zname still exists\"";
  $cmd .= ";   exit 1";
  $cmd .= "; fi";
  $cmd = "$SSH $USER\@$zhost \"$cmd\"";

  my($status) = &exec($cmd);
  return $status;
  }

# ----------------------------------
# Delete a remote directory
# Args: $zhost - the host on which the directory lives;
#       $zname - name of the  directory on that host.
# Returns: status, 0 on success
sub removeRemoteDir () {
  my($zhost, $zname) = @_;
  my($zdir, $zbase) = &basename($zname);

  my($cmd);
  $cmd  = "  if [ -d \\\"$zname\\\" ]; then ";
  $cmd .= "    chmod -R ugo+rwx \\\"$zname\\\"";
  $cmd .= ";   rm -r \\\"$zname\\\"";
  $cmd .= "; fi";
  $cmd .= "; if [ -d \\\"$zname\\\" ]; then ";
  $cmd .= "    echo \\\"Directory $zname still exists\\\"";
  $cmd .= ";   exit 1";
  $cmd .= "; fi";
  $cmd = "$SSH $USER\@$zhost \"$cmd\"";

  my($status) = &exec($cmd);
  return $status;
  }

# ----------------------------------
# Copy a directory to a remote host
# Args: $localDir - directory on this host;
#       $remoteDir - parent directory for this tree on remote host
#       $remoteName - name to give to top-level folder after it is copied
# Returns: status, 0 on success
sub putTree() {
  my($localDir, $remoteHost, $remoteDir, $remoteName) = @_;
  my($treeZipName) = "putTree-$$.zip";

  print "Copying $localDir to $remoteHost as $remoteDir/$remoteName...\n";
  unlink "/tmp/$treeZipName"; # no error checking

  my($treeDir, $treeName) = &basename($localDir);

  &removeRemoteDir($remoteHost, "$remoteDir/$treeName"); # no error checking

  if (&removeRemoteFile($MASTER, "$remoteDir/$treeZipName")) {
    print "Failed removing $MASTER:$remoteDir/$treeZipName";
    return 1;
    }
  print "  [removed $MASTER:$remoteDir/$treeZipName]\n" if $verbose;

  if (&removeRemoteDir($MASTER, "$remoteDir/$remoteName")) {
    print "Failed removing remote directory $MASTER:$remoteDir/$remoteName\n";
    return 1;
    }
  print "  [removed $MASTER:$remoteDir/$remoteName, if any]\n" if $verbose;

  my($d, $b) = &basename($localDir);
  chdir($d) || die "Cannot switch to directory $d: !";

  my($status) = &exec("$ZIP /tmp/$treeZipName \"$b\"");
  if ($status != 0) {
    print "Error zipping $localDir\n";
    unlink "/tmp/$treeZipName"; # no error checking
    return 1;
    }
  print "  [zipped to /tmp/$treeZipName]\n" if $verbose;

  $status = &exec("$SCP /tmp/$treeZipName \"$USER\@$remoteHost:$remoteDir/$treeZipName\"");
  if ($status != 0) {
    print "Error copying zip to remote host\n";
    unlink "/tmp/$treeZipName"; # no error checking
    return 1;
    }
  print "  [created  $remoteHost:$remoteDir/$treeZipName]\n" if $verbose;

  if (1 != unlink "/tmp/$treeZipName") {
    print "could not remove /tmp/$treeZipName???\n";
    &removeRemoteFile($remoteHost, "$remoteDir/$treeZipName");
    return 1;
    }
  print "  [removed /tmp/$treeZipName]\n" if $verbose;

  $status = &exec("$SSH $USER\@$remoteHost \"cd \\\"$remoteDir\\\"; $UNZIP $treeZipName\"");
  if ($status != 0) {
    print "unzip failure\n";
    &removeRemoteFile($remoteHost, "$remoteDir/treeZipName");
    &removeRemoteDir($remoteHost, "$remoteDir/$treeName");
    return 1;
    }
  print "  [Unzipped $remoteHost:$remoteDir/$treeZipName]\n" if $verbose;

  $status = &removeRemoteFile($remoteHost, "$remoteDir/$treeZipName");
  if ($status != 0) {
    print "Could not remove remote zip\n";
    &removeRemoteDir($remoteHost, "$remoteDir/$treeName");
    return 1;
    }
  print "  [Removed $remoteHost:$remoteDir/$treeZipName]\n" if $verbose;

  if ($treeName ne $remoteName) {
    $status = &exec("$SSH $USER\@$remoteHost \"cd \\\"$remoteDir\\\"; mv \\\"$treeName\\\" \\\"$remoteName\\\"\"");
    if ($status != 0) {
      print "mv failure\n";
      &removeRemoteDir($remoteHost, "$remoteDir/$treeName");
      return 1;
      }
    print "  [Renamed $remoteHost:$remoteDir/$treeName to $remoteName]\n" if $verbose;
    }

  $status = &exec("$SSH $USER\@$remoteHost \"chmod -R ug+w \\\"$remoteDir/$remoteName\\\"\"");
  if ($status != 0) {
    print "WARNING: error making directory group writable (continuing)\n";
    # but keep going?
    }

  # trace "Successful copy of $localDir to $remoteHost:$remoteDir/$remoteName"
  chdir($PWD) || die "Cannot return to $PWD: $!";
  return 0;
  }

# ---------------------------------
# Run a Hydra test on a remote machine.
# ---------------------------------
sub runRemote() {
  my($TEST_BASE) = "$REMOTE_BUILD/tests";

  # Take care of result directory
  my($status) = &removeRemoteDir($MASTER, "$SCRATCHPARENT/$resultName");
  if ($status != 0) {
    print "Could not remove directory $SCRATCHPARENT/$resultName\n";
    return 1;
    }
  print "  [Removed directory $MASTER:$SCRATCHPARENT/$resultName]\n" 
      if $verbose;

  $status = &remoteMkdir($MASTER, "$SCRATCHPARENT/$resultName");
  if ($status != 0) {
    print "Unable to create root directory $SCRATCHPARENT/$resultName\n";
    return 1;
    }
  print "  [Created directory $MASTER:$SCRATCHPARENT/$resultName]\n" 
      if $verbose;

  # copy over the BT
  $status = &exec(
      "$SCP \"$bt\" \"$USER\@$MASTER:$SCRATCHPARENT/$resultName/test.bt\"");
  if ($status != 0) {
    print "Unable to copy $bt to remote host\n";
    return 1;
    }
  print "  [Copied $bt to $MASTER:$SCRATCHPARENT/$resultName/test.bt]\n" 
      if $verbose;

  # the local.conf...
  $status = &exec(
      "$SCP \"$CONF\" \"$USER\@$MASTER:$SCRATCHPARENT/$resultName/local.conf\"");
  if ($status != 0) {
    print "Unable to copy $CONF to remote host\n";
    return 1;
    }
  print "  [Copied $CONF to $MASTER:$SCRATCHPARENT/$resultName/local.conf]\n"
      if $verbose;

  my($nuke)= "-DnukeHungTest=true";
# my($nuke) = "-DnukeHungTest=false";

  my($launcher) = "/tmp/runRemote-$$";
  open(F, ">$launcher") || die "Cannot write $launcher: $!";

  print F "#! /bin/bash\n";
  print F "set -e\n";
  print F "umask 002\n";
  print F "gf=\"$REMOTE_BUILD/product\"\n";
  print F "jt=\"$REMOTE_BUILD/tests/classes\"\n";
  print F "lib=\"$REMOTE_BUILD/hidden/lib\"\n";
  print F "tf=\"$SCRATCHPARENT/$resultName/test.bt\"\n";
  print F "rd=\"$SCRATCHPARENT/$resultName\"\n";
  print F "cd \"\$gf\"\n";
  print F ". bin/setenv.sh\n";
  print F "if [ \"\$HOSTTYPE.\$OSTYPE\" = i686.cygwin ]; then\n";
  print F "  export GEMFIRE=\`cygpath -w \"\$gf\"\`\n";
  print F "  junk=\`cygpath -w \"\$jt\"\`\n";
  print F "  export JTESTS=\"\$junk\"\n";
  print F "  export CLASSPATH=\"\$CLASSPATH;\$junk\"\n";
  print F "  lib=\`cygpath -w \"\$lib\"\`\n";
  print F "  tf=\`cygpath -w \"\$tf\"\`\n";
  print F "  rd=\`cygpath -w \"\$rd\"\`\n";
  print F "else\n";
  print F "  export GEMFIRE=\"\$gf\"\n";
  print F "  export JTESTS=\"\$jt\"\n";
  print F "  export CLASSPATH=\"\$CLASSPATH:\$jt\"\n";
  print F "fi\n";
  print F "export PATH=\"\$PATH:./hidden/lib\"\n";
  print F "set -xv\n";
  print F "cd \"\$rd\"\n";
  print F "\"$JDK\"/bin/java \\\n";
  print F "  -Djava.library.path=\"\$lib\" \\\n";
  print F "  $nuke \\\n";
  print F "  -DJTESTS=\"\$JTESTS\" \\\n";
  print F "  -DGEMFIRE=\"\$GEMFIRE\" \\\n";
  print F "  -DtestFileName=\"\$tf\" \\\n";
  print F "  -Dtests.results.dir=\"\$rd\" \\\n";
  print F "  -Dbt.result.dir=\"\$rd\" \\\n";
  print F "  -DnumTimesToRun=1 batterytest.BatteryTest\n";
  print F "\n";
  close F;

  $status = &exec(
      "$SCP $launcher \"$USER\@$MASTER:$SCRATCHPARENT/$resultName/launch.sh\"");
  if ($status != 0) {
    print "Unable to copy $launcher to remote host\n";
    unlink $launcher;
    return 1;
    }
  print "  [Copied $launcher to $MASTER:$SCRATCHPARENT/$resultName/launch.sh]\n"
      if $verbose;
  unlink $launcher;

  # Hack, hack
  my($kind) = `$SSH $USER\@$MASTER echo '\$HOSTTYPE.\$OSTYPE'`;
  if ($kind eq "i686.cygwin") {
    $status = &exec("$SCP //n080-fil01/NT_build_resources/usr/servio/gskill.exe $USER\@$MASTER:$SCRATCHPARENT/$resultName/gskill.exe");
    if ($status != 0) {
      print "Unable to copy gskill.exe to remote host\n";
      return 1;
      }
    print "  [Copied gskill.exe to $MASTER:$SCRATCHPARENT/$resultName]\n"
        if $verbose;

    my($renuke) = "/tmp/renuke-$$.sh";
    open(F, ">$renuke") || die "Cannot write $renuke: $!";

    print F "#! /bin/bash\n";
    print F "sed -e 's#//n080-fil01/NT_build_resources/usr/servio#'$SCRATCHPARENT/$resultName'# <\$1/nukerun.bat | \\\n";
    print F "sed -e 's^\@rem .*^#!/bin/bash^' >nuke.sh\n";
    close F;

    $status = &exec(
        "$SCP $renuke $USER\@$MASTER:$SCRATCHPARENT/$resultName/renuke.sh");
    if ($status != 0) {
      print "Unable to copy renuke.sh to remote host\n";
      return 1;
      }

    unlink $renuke;
    print "  [Copied renuke.sh to $MASTER:$SCRATCHPARENT/$resultName]\n"
         if $verbose;
    } # cygwin


  my($now) = scalar localtime;
  print "  [$now: Launching test...]\n" if $verbose;
  $status = &exec("$SSH $USER\@$MASTER \"cd \\\"$SCRATCHPARENT/$resultName\\\"; bash launch.sh >launch.log 2>&1\"");
  if ($status != 0) {
    print "Remote execution failure\n";
    return 1;
    }
  $now = scalar localtime;
  print "  [$now: Finished running test]\n" if $verbose;

  # echo "Execution successful, time to check results"
  return 0;
  }

# # ---------------------------------
# # Kill processes matching a given string pattern on a remote host
# # Args: $remoteHost - machine on which processes live
# #       $pattern - string (from "ps x") to match;
# #       $sig - signal to use in "kill" command
# # Returns: status, 0 on success
# sub remoteNuke () {
#   my($remoteHost, $pattern, $sig) = @_;
#   my($cmd);
# 
#   $cmd = "      set +e";
#   $cmd = "$cmd; pids=\\`ps x | grep '$pattern\' |";
#   $cmd = "$cmd     grep -v grep | sed -e 's/^\\(.....\\).*/\\\\1/'\\\`";
# #  $cmd = "$cmd; echo \\\"pids = \$pids\\\"";
#   $cmd = "$cmd; if [ \\\"\\\$pids\\\" = \\\"\\\" ]; then ";
#   $cmd = "$cmd    echo \\\"$remoteHost: No processes found\\\"";
#   $cmd = "$cmd;   exit 0";
#   $cmd = "$cmd; fi";
#   $cmd = "$cmd; for each in \\\$pids; do ";
#   $cmd = "$cmd    echo \\\"$remoteHost: Killing process \\\$each\\\"";
#   $cmd = "$cmd;   kill -$sig \\\$each";
#   $cmd = "$cmd;  done";
#   $cmd = "$SSH $USER\@$remoteHost \"$cmd\"";
# 
#   my($status) = &exec($cmd);
#   if ($status != 0) {
#     print "remoteNuke failure\n";
#     return 1;
#     }
#   return 0;
#   }

# ---------------------------------
sub zipRemoteDir() {
  my($zhost, $zname, $treeZipName) = @_;
  my($zdir, $zbase) = &basename($zname);

  my($cmd) = "      set -e";
  $cmd = "$cmd; cd \"$zdir\"" ;
  $cmd = "$cmd; $ZIP \"$treeZipName\" \"$zbase\"" ;
  my($status) = &exec("$SSH $USER\@$zhost \"$cmd\"");

  if ($status != 0) {
    print "remote zip failure\n";
    return 1;
    }
  return 0;
  }


# ---------------------------------
sub getTree() {
  my($remoteHost, $remoteName, $localDir, $localName) = @_;
  my($h) = `hostname`;
  chomp($h);
  my($treeZipName) = "getTree-$h-$$.zip";
  my($junk, $treeName) = &basename($remoteName);
  chdir($localDir) || die "Cannot chdir to $localDir: $!";
  my($status) = &zipRemoteDir($remoteHost, $remoteName, "/tmp/$treeZipName");
  if ($status != 0) {
    return 1;
    }
  print "  [zipped $remoteHost:/tmp/$treeZipName]\n" if $verbose;

  $status = &exec("$SCP $USER\@$remoteHost:/tmp/$treeZipName $treeZipName");
  if ($status != 0) {
    print "scp failure\n";
    &removeRemoteFile($remoteHost, "/tmp/$treeZipName");
    return 1;
    }
  print "  [created  /tmp/$treeZipName]\n" if $verbose;

  $status = &removeRemoteFile($remoteHost, "/tmp/$treeZipName");
  if ($status != 0) {
    print "could not remove $remoteHost:/tmp/$treeZipName???\n";
    unlink $treeZipName;
    return 1;
    }
  print "  [removed $remoteHost:$treeZipName]\n" if $verbose;

  if (-d "$localDir/$treeName") {
    print "$localDir/$treeName exists, will rename it to...\n";
    my($count) = 1;
    while (-d "$localDir/$treeName-$count") {
      $count ++;
      }
    if (rename($treeName, "$treeName-$count")) {
      # $treeName = "$treeName-$count";
      print "  ...$localDir/$treeName\n";
      }
    else {
      # Very weird...
      die "rename of $localDir/$treeName to $treeName-$count failed: $!";
      }
    }
  $status = &exec("$UNZIP \"$treeZipName\"");
  if ($status != 0) {
    print "unzip failure\n";
    unlink $treeZipName;
    &exec("chmod -R ugo+rwx \"$treeName\""); # no error checking
    unlink $treeName;
    return 1;
    }
  print "  [unzipped $treeZipName]\n" if $verbose;

  unlink $treeZipName;
  if ($treeName ne $localName) {
    if (rename($treeName, $localName)) {
      print "  [renamed $treeName to $localName]\n" if $verbose;
      }
    else {
      warn "rename $treeName to $localName failed: $!";
      }
    }


  $status = &exec("chmod -R ug+w \"$localDir/$localName\"");
  if ($status != 0) {
    print "WARNING: error making directory group writable (continuing)\n";
    # but keep going?
    }

  chdir($PWD) || die "Cannot chdir to $PWD: $!";
  # echo "Successful copy of $localDir to $remoteHost:$remoteDir/$remoteName"
  return 0;
  }

# ---------------------------------
sub exit() {
  my($status) = @_;
  my($endTime) = time;

  print "Elapsed = " . ($endTime - $startTime) . " sec\n";
  my($now) = scalar localtime;
  print "Finished at $now\n";
  exit $status;
  }


# ---------------------------------

# Determine whether given directory exists on given host
# Arguments: $zhost - host to query
#            $zname - name to query
# Returns: 0 if the directory exists
sub remoteDirExists () {
  my($zhost, $zname) = @_;

  my($cmd);
  $cmd = "      set -e";
  $cmd = "$cmd; if [ -d \\\"$zname\\\" ]; then ";
  $cmd = "$cmd    exit 0";
  $cmd = "$cmd; else";
  $cmd = "$cmd    exit 1";
  $cmd = "$cmd; fi";
  $cmd = "$SSH $USER\@$zhost \"$cmd\"";

  my($status) = &exec($cmd);
  return $status;
  }

# ---------------------------------

# Determine whether given file exists on given host
# Arguments: $zhost - host to query
#            $zname - name to query
# Returns: 0 if the directory exists
sub remoteFileExists () {
  my($zhost, $zname) = @_;

  my($cmd);
  $cmd = "      set -e";
  $cmd = "$cmd; if [ -f \\\"$zname\\\" ]; then ";
  $cmd = "$cmd    exit 0";
  $cmd = "$cmd; else";
  $cmd = "$cmd    exit 1";
  $cmd = "$cmd; fi";
  $cmd = "$SSH $USER\@$zhost \"$cmd\"";

  my($status) = &exec($cmd);
  return $status;
  }

# ---------------------------------
# Indicate whether a linux box has the given service (ntp) running
# Arguments: $zhost - host to check,
#            $service - service to check for
# Returns:   0 if the service exists
sub serviceExists() {
  my($zhost, $service) = @_;

  my($cmd);
  $cmd  =  "if ! \\\`ps -ef | grep -v grep | grep $service 2>&1 >/dev/null\\\`; then";
  $cmd .= "    echo \\\"service $service not found\\\"";
  $cmd .= ";   exit 1";
  $cmd .= "; else";
  $cmd .= "    exit 0";
  $cmd .= "; fi";
  $cmd = "$SSH $USER\@$zhost \"$cmd\"";

  my($status) = &exec($cmd);
  return $status;
  }

# ---------------------------------
# Indicate whether a given host exists
# Arguments: $zhost - host to check,
# Returns:   0 if the host exists
sub hostExists() {
  my($zhost) = @_;

  my($response) = `/bin/ping -q -w 5 $zhost 2>&1`;
  my($status) = $?;

  return 0 if $status == 0;

  print $response;
  return 1;
  }

# ---------------------------------

# Determine whether given file or directory is writebale
# Arguments: $zhost - host to query
#            $zname - name to query
# Returns: 0 if it is writeable
sub remoteWriteable () {
  my($zhost, $zname) = @_;

  my($cmd);
  $cmd = "      set -e";
  $cmd = "$cmd; if [ -w \\\"$zname\\\" ]; then ";
  $cmd = "$cmd    exit 0";
  $cmd = "$cmd; else";
  $cmd = "$cmd    exit 1";
  $cmd = "$cmd; fi";
  $cmd = "$SSH $USER\@$zhost \"$cmd\"";

  my($status) = &exec($cmd);
  return $status;
  }

# ---------------------------------
# Create a directory
# Arguments: $zhost - host to work with
#            $zname - directory to create
# Returns: 0 on success
sub remoteMkdir() {
  my($zhost, $zname) = @_;

  my($cmd);
  $cmd =   "set -e";
  $cmd .=  "; mkdir -p \\\"$zname\\\"";
  $cmd .=  "; chmod ug+w \\\"$zname\\\"";
  $cmd = "$SSH $USER\@$zhost \"$cmd\"";

  my($status) = &exec($cmd);
  return $status;
  }
