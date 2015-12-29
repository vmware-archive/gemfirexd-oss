#! /bin/perl -w
# -------------------------

use Cwd;

my(
  $log,			# logfile for output

  $commandFile,		# file to read for commands
  $old_dev,		# from stat() of commandFile
  $old_ino,		# from stat() of commandFile
  $old_size,		# from stat() of commandFile
  $old_mtime,		# from stat() of commandFile
  $old_ctime,		# from stat() of commandFile
  $commandFilePos,	# tell/seek pointer for efficiency

  $verbose,		# whether to print more messages
  $nextLine,		# next line to be read, 1-based
  $pauseFile,		# file that will cause queue to pause
  );

my($status);		# for the system() command
my($PWD) = cwd();

&setDefaults();
&getArgs();
&validateArgs();

if ($log ne "-") {
  open(LOG, ">>$log") || die "Cannot append to $log: $!";
  # Turn off buffering
  select LOG; $| |= 1; select STDOUT;

  # Note that LOG is closed and reopened during exec
  }

&displayConfiguration();

&processLines();
# NOTREACHED

# ----------------------------------
# Set default parameters
sub setDefaults() {
  $log = "-";	# stdout
  $commandFile = "runqueue.txt";
  $verbose = 0;
  $nextLine = 1;
  $pauseFile = "";

  # Not strictly defaults, initialization:
  $commandFilePos = -1; # invalid
  $old_dev = -1;
  $old_ino = -1;
  $old_size = -1;
  $old_mtime = -1;
  $old_ctime = -1;
  }


# ----------------------------------
# Obligatory usage message
sub usage() {
  print "Usage: $0 <args>\n";
  print "\n";
  print "Arguments:\n";
  print "  log=<fname>           (default \"$log\")\n";
  print "    log file to be appended to\n";
  print "  commandFile=<fname>   (default \"$commandFile\")\n";
  print "    file to read commands from\n";
  print "  nextLine=<int>        (default \"$nextLine\")\n";
  print "    first line to execute from commandFile\n";
  print "  pauseFile=<fname>        (default \"$pauseFile\")\n";
  print "    While this file exists, execution suspends\n";

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
    if ($arg =~ /^log=(.+)$/i) {
      $log = $1;
      next;
      }
    if ($arg =~ /^commandFile=(.+)$/i) {
      $commandFile = $1;
      next;
      }
    if ($arg =~ /^nextLine=(\d+)$/i) {
      $nextLine = $1;
      next;
      }
    if ($arg =~ /^pauseFile=(.+)$/i) {
      $pauseFile = $1;
      next;
      }

    print "Unknown argument \"$arg\"\n";
    &usage();
    }
  }

# ----------------------------------
sub validateArgs() {
  my($ok) = 1;

  if (! -f $commandFile) {
    print "commandFile $commandFile does not exist\n";
    $ok = 0;
    }

  if (!$ok) {
    print "Errors detected in configuration, aborting.\n";
    exit 1;	# don't print time summary
    }
  }


# ----------------------------------
sub displayConfiguration() {
  &log("---------------------------------\n");
  &log("log         = $log\n");
  &log("commandFile = $commandFile\n");
  &log("nextLine    = $nextLine\n");
  &log("pauseFile   = $pauseFile\n");
  &log("---------------------------------\n");
  }

# ----------------------------------
# Execute a command
# Arguments: $cmd - command to execute
# Returns: 0 on success
sub exec() {
  my($cmd) = @_;
  my($logName) = $log;

  $logName = "" if $log eq "-";
  # Close log, then append our output to it
  close LOG;

  my($status) = system "( $cmd ) 2>&1 | tee -ia $log";

  open(LOG, ">>$log") || die "Cannot append to $log: $!";
  # Turn off buffering
  select LOG; $| |= 1; select STDOUT;

  return -1 if $status == 0xff00;
  if ($status > 0x80) {
    $status >>= 8;
    return $status;
    }

  return $status != 0;
  }


# ---------------------------------
sub exit() {
  my($status) = @_;
  my($endTime) = time;

  my($now) = scalar localtime;
  print "Finished at $now\n";
  exit $status;
  }

# ---------------------------------
sub log() {
  my($message) = @_;

  if ($log ne "-") {
    print LOG "runqueue: $message";
    }
  print "runqueue: $message";
  }

# ---------------------------------
# Position $commandFile at the next line, return that line.
sub getNextLine() {
  my($now);
  my($result);

  # Look for file, position.
  my($reported) = 0;
  TRYING: for (;;) {

    if (!open(CMD, "<$commandFile")) {
      &log("commandFile $commandFile is completely unreadable: $!\n");
      &log("[Exiting for safety's sake.  Sorry!]\n");
      &exit(1);
      }
    my($dev, $ino, $mode, $nlink, $uid, $gid, $rdev, $size, $atime,
      $mtime, $ctime, $blksize, $blocks) = stat($commandFile);
    if (!defined($mtime)) {
      &log("stat on $commandFile failed : $!\n");
      &log("[Exiting for safety's sake.  Sorry!]\n");
      &exit(1);
      }

    my($changed) = 0;
    $changed ||= $dev != $old_dev;
    $changed ||= $ino != $old_ino;
    # Don't care about mode, nlink, uid, gid, rdev
    $changed ||= $size != $old_size;
    # Don't care about atime
    $changed ||= $mtime != $old_mtime;
    $changed ||= $ctime != $old_ctime;
    # Don't care about blksize, blocks
    if ($changed) {
      if ($commandFilePos != -1) {
        &log("[Detected change in file $commandFile]\n");
        }
      $commandFilePos = -1;
      $old_dev = $dev;
      $old_ino = $ino;
      $old_size = $size;
      $old_mtime = $mtime;
      $old_ctime = $ctime;
      }

    # Slide forward to given point in file...
    if ($commandFilePos != -1) {
      # Position file based on previous knowledge
      if (!seek(CMD, $commandFilePos, 0)) {
	close CMD;
	&log("File has inexplicably shortened (looking for line $nextLine).\n");
	&log("Exiting for safety's sake.  Sorry.\n");
	&exit(1);
	}
      }
    else { # Position manually (no previous file information)
      my($curLine) = 0;
      while ($curLine < ($nextLine - 1)) {
	my($line);
        $line = <CMD>;
	if (!defined($line)) {
	  close CMD;
	  &log("File has inexplicably shortened (looking for line $nextLine).\n");
	  &log("Exiting for safety's sake.  Sorry.\n");
	  &exit(1);
	  }
	$curLine ++;
	}
      } # position manually

    # Read the line of interest...
    $result = <CMD>;
    if (!defined($result)) {
      close CMD;
      if (!$reported) {
	$reported = 1;
        $now = scalar localtime;
        &log("[$now: exactly at end of $commandFile (looking for line $nextLine); pausing...]\n");
        }
      sleep(60);
      next TRYING;
      }

    # Success!
    last TRYING;
   } # TRYING

  $commandFilePos = tell CMD;
  close CMD;
  
  chomp($result);
  if ($verbose) {
    &log("[parsed line $nextLine: '$result']\n");
    }
  $nextLine ++;
  return $result;
  }

# ---------------------------------
# Look for a touched file, wait until it vanishes
sub checkForPause() {
  return if $pauseFile eq "";	# No pausefile defined?

  while (-f $pauseFile) {
    my($now) = scalar localtime;
    &log("$now: pause file $pauseFile in place, waiting...\n");
    sleep(60);
    }
  }

# ---------------------------------
# Execution loop (never exits)
sub processLines() {
  for (;;) { # forever!
    &checkForPause();
    my($line) = &getNextLine();

    # Allow simple comments
    next if $line =~ /\s*#.*/;
    my($cmd) = $line;

    # Blank lines not interesting
    next if $line =~ /^\s*$/;

    # Allow simple line continuations
    for (;;) { # line continuation
      last unless $cmd =~ /^(.*)\\$/;
      $cmd = $1; # remove trailing backslash
      $line = &getNextLine();
      $cmd .= $line;
      } # line continuation

    my($now) = scalar localtime;
    my($junk) = $nextLine - 1;
    &log("[$now: Executing line $junk: $cmd]\n");
    my($status) = &exec($cmd);
    if ($verbose || $status != 0) {
      &log("[exec status: $status]\n");
      }
    $now = scalar localtime;
    &log("[$now: Finished line $junk]\n");
    &log("--------------------------------------\n");
    } # forever
  }
