#! /bin/perl -w
# ---------------------------------------------
# Generate summary of a batterytest run
#
# Arguments: the root directory of the test run
# Output: a TSV file to stdout; redirect for proper operation.
# ---------------------------------------------

# Constant file names
my($confName); #  = 'useCase17.prop';
my($statName) = 'perfreport.txt';

# The oneliner.txt summary
my(%oneLineProgress, %oneLineElapsed);

# The list of directories
my(@dirs);

# The top-level directory (specified by user)
my($top);

# ---------------------------------------------

if ($#ARGV < 1) {
  print STDERR "Usage: $0 <topdir> <propfile> [<dirs>]\n";
  exit 1;
  }
$top = $ARGV[0];
if (! -d $top) {
  print STDERR "$top does not seem to be a directory\n";
`ls $top`;
  exit 1;
  }
$confName = $ARGV[1];

if ($#ARGV == 1) {
  @dirs = &getDirs($top);
  }
else {
  shift @ARGV;
  shift @ARGV;
  @dirs = @ARGV;
  }

if ($#dirs == -1) {
  print STDERR "No directories found in $top; bailing\n";
  exit 1;
  }


my(%knownAttributes) = &getAttributes(@dirs);
if (!%knownAttributes) {
  print STDERR "No attributes found _anywhere_; bailing\n";
  exit 1;
  }
%knownAttributes = &pruneAttributes(%knownAttributes);

my(%knownStats) = &getStats(@dirs);
if (!%knownStats) {
  print STDERR "No statistics found _anywhere_; bailing\n";
  exit 1;
  }

&getOneLiners($top);

&printHeaders();
foreach $each (sort(@dirs)) {
  &analyze($top, $each);
  }
exit 0;

# ---------------------------------------------
# Get list of directories
#
# Arguments: $root - folder in which to search for directories
# Returns list of directories
# ---------------------------------------------
sub getDirs() {
  my($root) = @_;
  my(@result) = ();

  opendir(DIRS, $root) || die "opendir($root): $!";
  my($each);
  for (;;) {
    $each = readdir(DIRS);
    last unless defined($each);
    next if $each eq '.';
    next if $each eq '..';
    next unless -d "$root/$each";
    push @result, $each;
    }
  closedir(DIRS);
  return @result;
  }

# ---------------------------------------------
# Print heading line on table
# ---------------------------------------------
sub printHeaders() {
  print "dirName\tstatus\telapsed\t";

  my($each);
  foreach $each (sort(keys(%knownAttributes))) {
    print "$each\t";
    }

  foreach $each (sort(keys(%knownStats))) {
    print "$each\t";
    }
  print "\n";
  }

# ---------------------------------------------
# Read the oneliner.txt from the root directory and
# collect the status and elapsed time information.
#
# Updates %oneLineProgress and %oneLineStatus.
# ---------------------------------------------
sub getOneLiners() {
  my($top) = @_;

  open(F, "<$top/oneliner.txt") || die "Cannot open oneliner.txt: $!";
  my($line);
  for(;;) {
    $line = <F>;
    last unless defined($line);
    chomp($line);

    my(@pieces) = split(/ +/, $line);
    my($progress) = $pieces[1];
    my($elapsed) = $pieces[2];
    my($name) = $pieces[3];
    $name =~ s#.*/([^/]+)#$1#;

    $oneLineProgress{$name} = $progress;
    $oneLineElapsed{$name} = $elapsed;
    }
  close F;
  }

# ---------------------------------------------
# Analyze and print summary of a single folder
# Arguments: $top -  root directory
#   $dir - the directory under $top to analyze
# ---------------------------------------------
sub analyze() {
  my($top, $dir) = @_;
  my(%atts) = &getAttributesForDir("$top/$dir");
  my(%stats) = &getStatsForDir("$top/$dir");

  print "$dir\t";
  if (!defined($oneLineProgress{$dir})) {
    print "?\t?\t";
    }
  else {
    print "$oneLineProgress{$dir}\t";
    print "$oneLineElapsed{$dir}\t";
    }
  my($each, $val);
  foreach $each (sort(keys(%knownAttributes))) {
    $val = $atts{$each};
    if (!defined($val)) {
#      print STDERR "$dir: failed to find attribute: $each\n";
      print "\t";
      next;
      }
    if ($val =~ /^[-=].*/) {
      $val = "'$val'";
      }
    $val =~ s/\t/ /g;
    print "$val\t";
    }

  foreach $each (sort(keys(%knownStats))) {
    $val = $stats{$each};
    if (defined($val)) {
      print "$val\t";
      }
    else {
#      print STDERR "$dir: failed to find statistic: $each\n";
      print "\t";
      }
    }

  print "\n";
  }


# ---------------------------------------------
# Collect all known attributes across all directories
# Args: the list of directories
# Returns: hash with all the keys
# ---------------------------------------------
sub getAttributes() {
  my(@dirs) = @_;

  my(%result) = ();
  my(%dirAtts);
  my($each);
  
  foreach $each (@dirs) {
    %dirAtts = &getAttributesForDir($each);
    next if !%dirAtts;

    my($each2);
    foreach $each2 (keys(%dirAtts)) {
      $result{$each2} = 1; # value not important
      }
    }
  return %result;
  }

# ---------------------------------------------
# Read the configuration attributes on the given directory
# Arguments: $dir - the directory to read from
# Returns: %result - list of attributes
# ---------------------------------------------
sub getAttributesForDir() {
  my($dir) = @_;
  my(%result) = ();

  if (!open(F, "<$dir/$confName")) {
    print STDERR "Cannot open $dir/$confName: $!\n";
    return undef;
    }
  my($line, $key, $value);
  for (;;) {
    $line = <F>;
    last if !defined($line);
    chomp($line);
    next unless $line =~ /([^=]+)=([^=]+)/;
    $key = $1;
    $value = $2;
    $result{$key} = $value;
    }
  close F;

  return %result;
  }

# ---------------------------------------------
# Collect all known stats across all directories
# Args: the list of directories
# Returns: hash with all the keys
# ---------------------------------------------
sub getStats() {
  my(@dirs) = @_;

  my(%result) = ();
  my(%dirStats);
  my($each);
  
  foreach $each (@dirs) {
    %dirStats = &getStatsForDir($each);
    next if !%dirStats;

    my($each2);
    foreach $each2 (keys(%dirStats)) {
      $result{$each2} = 1; # value not important
      }
    }
  return %result;
  }


# ---------------------------------------------
# Read the statistics report
# Arguments: $dir - the directory of the test run
# Returns: %result - the statistics
# ---------------------------------------------
sub getStatsForDir() {
  my($dir) = @_;
  my(%result) = ();

  if (!open(F, "<$dir/$statName")) {
    print STDERR "Cannot open $dir/$statName: $!\n";
    return %result;
    }
  my($line, $key, $value);

  # Skip down to statistics
  for (;;) {
    $line = <F>;
    if (!defined($line)) {
      print STDERR "$dir/$statName: premature end of file (1)\n";
      close F;
      return %result;
      }
    chomp($line);
    last if $line =~ /Statistics Values/;
    }
  $line = <F>;  # lots of equals
  if (!defined($line)) {
    print STDERR "$dir/$statName: premature end of file (2)\n";
    close F;
    return %result;
    }

  for (;;) { # Now look for statistics
    $line = <F>;
    last if !defined($line);
    chomp($line);

    my($majkey);
    if ($line !~ /^([^ ]+) \*.*/) {
      print STDERR "$dir/$statName: Ill-formed statistics key line = '$line\n";
      close F;
      return %result;
      }
    $majkey = $1;

    $line = <F>; # filter=...
    if (!defined($line)) {
      print STDERR "$dir/$statName: premature end of file (3)\n";
      close F;
      return %result;
      }

    $line = <F>; # lots of hyphens
    if (!defined($line)) {
      print STDERR "$dir/$statName: premature end of file (4)\n";
      close F;
      return %result;
      }

    $line = <F>; # ==>
    if (!defined($line)) {
      print STDERR "$dir/$statName: premature end of file (5)\n";
      close F;
      return %result;
      }
    chomp($line);

    if ($line =~ /No matches were found for this specification/) {
      $line = <F>; # read trailing delimiter, lots of equals
      next;
      }

    if ($line !~ /^==> /) {
      print STDERR "$dir/$statName: Ill-formed statistics value line = '$line'\n";
      close F;
      return %result;
      }
    my(@vals) = split(/ /, $line);
    shift(@vals); # don't need the arrow.
    foreach $each (@vals) {
      if ($each !~ /([^=]+)=([^=]+)/) {
        print STDERR "$dir/$statName: Ill-formed statistics value line = '$line' (looking at '$each')\n";
        close F;
        return %result;
	}
      my($subkey) = $1;
      $value = $2;
      $result{"$majkey.$subkey"} = $value;
      }

    $line = <F>; # samples=...
    if (!defined($line)) {
      print STDERR "$dir/$statName: premature end of file (6)\n";
      close F;
      return %result;
      }

    my($subsubkey) = 2;
    for (;;) { # multi-valued stats
      $line = <F>;
      if (!defined($line)) {
        print STDERR "$dir/$statName: premature end of file (7)\n";
        close F;
        return %result;
        }
      chomp($line);
      last if $line =~ /^=======/;

      next if $line =~ /No matches were found for this specification/;

      if ($line !~ /^==> /) {
	print STDERR "$dir/$statName: Ill-formed statistics value line = '$line'\n";
	close F;
	return %result;
	}
      my(@vals) = split(/ /, $line);
      shift(@vals); # don't need the arrow.
      foreach $each (@vals) {
	if ($each !~ /([^=]+)=([^=]+)/) {
	  print STDERR "$dir/$statName: Ill-formed statistics value line = '$line' (looking at '$each')\n";
	  close F;
	  return %result;
	  }
	my($subkey) = $1;
	$value = $2;
	if ($subsubkey < 10) {
	  $result{"$majkey.$subkey.0$subsubkey"} = $value;
	  }
	else {
	  $result{"$majkey.$subkey.$subsubkey"} = $value;
	  }
	$subsubkey ++;
	}

      $line = <F>; # samples=...
      if (!defined($line)) {
	print STDERR "$dir/$statName: premature end of file (6)\n";
	close F;
	return %result;
	}
      } # multi-valued stats
    } # Now look for statistics
  close F;

  return %result;
  }

sub pruneAttributes {
  my(%allAttributes) = @_;
  my(%result) = %allAttributes;
  my($each);

  my(@patterns) = (
    "^ .*",
    "^cacheperf\.CachePerfPrms-taskTerminator.*",
    "^cacheperf\.CachePerfPrms-warmupTerminator.*",
    "^source\.date",
    "^hydra\.ClientDescription",
    "^hydra\.ClientPrms.*",
    "^hydra\.GemFireDescription.*",
    "^hydra\.GemFirePrms-hostNames",
    "^hydra\.GemFirePrms-names",
    "^hydra\.HostAgentDescription.*",
    "^hydra\.HostDescription.*",
    "^hydra\.HostPrms.*",
    "^hydra\.HydraThreadGroup\..*",
    "^hydra\.log\.LogPrms.*",
    "^hydra\.MasterDescription",
    "^hydra\.Prms-alwaysDoEndTasks",
    "^hydra\.Prms-checkTaskMethodsExist",
    "^hydra\.Prms-doInitTasksSequentially",
    "^hydra\.Prms-finalClientSleepSec",
    "^hydra\.Prms-haltIfBadResult",
    "^hydra\.Prms-initialClientSleepSec",
    "^hydra\.Prms-maxClientShutdownWaitSec",
    "^hydra\.Prms-maxClientStartupWaitSec",
    "^hydra\.Prms-maxHostAgentShutdownWaitSec",
    "^hydra\.Prms-maxHostAgentStartupWaitSec",
    "^hydra\.Prms-maxResultWaitSec",
    "^hydra\.Prms-randomSeed",
    "^hydra\.Prms-roundRobin",
    "^hydra\.Prms-serialExecution",
    "^hydra\.Prms-sleepBeforeShutdownSec",
    "^hydra\.Prms-startLocatorAgentsBeforeTest",
    "^hydra\.Prms-stopLocatorAgentsAfterTest",
    "^hydra\.Prms-stopSystemsOnError",
    "^hydra\.Prms-testDescription",
    "^hydra\.Prms-testRequirement",
    "^hydra\.Prms-totalTaskTimeSec",
    "^hydra\.Prms-useNFS",
    "^hydra\.RmiRegistryDescription",
    "^hydra\.TestTask.*",
    "^hydra\.VmDescription.*",
    "^hydra\.VmPrms-hostNames.*",
    "^hydra\.VmPrms-names.*",
    "^perffmwk\.PerfReportPrms.*",
    "^TestName\$",
    "^TestUser",



    );
  my($key);
  foreach $key (@patterns) {
    foreach $each (keys(%allAttributes)) {
      if ($each =~ /$key/) {
        delete $result{$each};
        next;
        }
      }
    }

  return %result;
  }
