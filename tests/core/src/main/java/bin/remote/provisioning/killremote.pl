#! /bin/perl -w
# -------------------------
# killremote.pl -- kill all processes on remote hosts matching a given
# pattern in their "ps" string.

use Cwd;

my(
  $hostList,		# list of hosts to manage, space-separated
  $verbose,		# more messages
  $JDK,		# localJDK on SUT
  );

my($status);		# for the system() command
my($PWD) = cwd();
my($SSH) = "ssh -o BatchMode=yes";
my(@hostList);

&setDefaults();
&getArgs();
@hostList = split(/ /, $hostList);
&validateArgs();
&displayConfiguration();


# Kill any errant processes
my($y, $x) = &basename($JDK);
foreach $each (@hostList) {
  &remoteNuke($each, $x, 9);
  }

&exit(0);

# ----------------------------------
# Set default parameters
sub setDefaults() {
  my($USER) = $ENV{"USER"};
  if (!defined($USER) || $USER eq "") {
    $USER = "user";
    }
  $verbose = 0;

  # $hostList = "bensa bensc bensd bense bensf bensb";
  # $JDK = "/export/bensa1/users/$USER/j2sdk1.4.2_09";

  $hostList = "ptesta ptestb ptestc ptestd pteste ptestf";
  $JDK = "/export/ptesta1/users/build/jdk1.5.0_05";
  }


# ----------------------------------
# Obligatory usage message
sub usage() {
  print "Usage: $0 <args>\n";
  print "\n";
  print "Arguments:\n";
  print "  -f run in spite of no arguments (avoids usage message)\n";
  print "  hostList=<space separated string>\n";
  print "  JDK=<dirname>       (default \"$JDK\)\n";
  print "    JDK in use on SUT\n";
  exit 1;	# don't print time summary
  }

# ----------------------------------
# Parse all arguments
sub getArgs() {
  if ($#ARGV == -1) {
    &usage();
    }
  for (;;) {
    my($arg) = shift @ARGV;
    last unless defined($arg);

    if ($arg eq "-h" || $arg eq "--help" || $arg eq "help" ) {
      &usage();
      }
    if ($arg eq "-v") {
      $verbose = 1;
      next;
      }
    if ($arg eq "-f") {
      next;
      }
    if ($arg =~ /^hostList=(.*)$/) {
      $hostList = $1;
      next;
      }
    if ($arg =~ /^JDK=(.*)$/) {
      $JDK = $1;
      next;
      }

    print "Unknown argument \"$arg\"\n";
    &usage();
    }
  }

# ----------------------------------
sub validateArgs() {
  }


# ----------------------------------
sub displayConfiguration() {
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

# ---------------------------------
# Kill processes matching a given string pattern on a remote host
# Args: $remoteHost - machine on which processes live
#       $pattern - string (from "ps x") to match;
#       $sig - signal to use in "kill" command
# Returns: status, 0 on success
sub remoteNuke () {
  my($remoteHost, $pattern, $sig) = @_;
  my($cmd);

  $cmd = "      set +e";
  $cmd = "$cmd; pids=\\`ps x | grep '$pattern\' |";
  $cmd = "$cmd     grep -v grep | sed -e 's/^\\(.....\\).*/\\\\1/'\\\`";
#  $cmd = "$cmd; echo \\\"pids = \$pids\\\"";
  $cmd = "$cmd; if [ \\\"\\\$pids\\\" = \\\"\\\" ]; then ";
  $cmd = "$cmd    echo \\\"$remoteHost: No processes found\\\"";
  $cmd = "$cmd;   exit 0";
  $cmd = "$cmd; fi";
  $cmd = "$cmd; for each in \\\$pids; do ";
  $cmd = "$cmd    echo \\\"$remoteHost: Killing process \\\$each\\\"";
  $cmd = "$cmd;   kill -$sig \\\$each";
  $cmd = "$cmd;  done";
  $cmd = "$SSH $remoteHost \"$cmd\"";

  my($status) = &exec($cmd);
  if ($status != 0) {
    print "remoteNuke failure\n";
    return 1;
    }
  return 0;
  }

# ---------------------------------
sub exit() {
  my($status) = @_;

  exit $status;
  }

