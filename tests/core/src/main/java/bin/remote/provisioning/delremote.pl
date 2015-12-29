#! /bin/perl -w
# -------------------------
# delremote.pl -- Delete a directory on a remote system.
# -------------------------

use Cwd;

my(
  $verbose,		# more messages
  $MASTER,		# host to perform the work
  $USER,		# ssh username to use
  $remoteName,		# name to delete
  );

my($status);		# for the system() command
my($PWD) = cwd();
my($SCP) = "scp -B -q";
my($SSH) = "ssh -o BatchMode=yes";

&setDefaults();
&getArgs();
if ($remoteName eq "") {
  print "No NAME given; exiting\n";
  exit 1;
  }

&removeRemoteDir($MASTER, $remoteName);
exit 0;

# ----------------------------------
# Set default parameters
sub setDefaults() {
  # $USER = $ENV{"USER"};
  # if (!defined($USER) || $USER eq "") {
  #   $USER = "user";
  #   }
  $USER = "build";
  $MASTER = "ptesta";
  $verbose = 0;
  $remoteName = "";
  }


# ----------------------------------
# Obligatory usage message
sub usage() {
  print "Usage: $0 <args>\n";
  print "\n";
  print "Arguments:\n";
  print "  -v\n";
  print "    Verbose\n";
  print "  MASTER=hostname     (default \"$MASTER\")\n";
  print "    (Hydra) master for running test\n";
  print "  name=<name>\n";
  print "    Name of file to delete; no default\n";
  print "  USER=username       (default \"$USER\")\n";
  print "    username on master to ssh as\n";

  exit 1;	# don't print time summary
  }

# ----------------------------------
# Parse all arguments
sub getArgs() {
  # TODO: need to parameterize NTPHOST

  if ($#ARGV == -1) {
    &usage();
    }
  for (;;) {
    my($arg) = shift @ARGV;
    last unless defined($arg);

    if ($arg =~ /^-h$/i || $arg =~ /^--help$/i || $arg =~ /^help$/i ) {
      &usage();
      }
    if ($arg =~ /^-v$/i) {
      $verbose = 1;
      next;
      }
    if ($arg =~ /^MASTER=(.*)$/i) {
      $MASTER = $1;
      next;
      }
    if ($arg =~ /^NAME=(.*)$/i) {
      $remoteName = $1;
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
