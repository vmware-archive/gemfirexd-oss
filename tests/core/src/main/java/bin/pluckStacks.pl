#! /bin/perl -w
# -------------------------------------
# Tired of looking for those "significant" stacks in your gemfire
# bgexec files?  Here's a tool that might help.
#
# Usage:
#  perl pluckStacks.pl bgexec*.log | less
#
# Looks for stacks in the given list of files, writes the results on STDOUT.
#
# This is a very preliminary tool.  Lots of things could be done to
# improve this, including:
#
# - meta-patterns in init_patterns for ints, line numbers, etc.
# - JRockit patterns are still missing.
#
# RISKS:
# - a bad pattern might cause a significant stack to be omitted
# - modifications to the source base may cause patterns to become obsolete
#
# METHOD OF OPERATION:
# 'Full thread dump' messages are searched for in each file.  For each file,
# lines are scanned until the (apparent) end of the dump is reached.
#
# For each thread dump, apparent thread stacks are sorted out.  For each
# thread stack, it is compared against a list of known "uninteresting" thread
# stacks; see KNOWN_PATTERNS in sub init_patterns.
#
# Each thread stack is analyzed on a line-by-line basis; _all_ lines must
# match in the pattern in order for the stack to match.
#
# In addition, if the stack does not contain "com.gemstone" anywhere in it,
# it is regarded as uninteresting.  (This is by default; see
# $omit_foreign_stacks if you don't want this behavior.)
#
# If a stack is "not known", it is printed to STDOUT.
#
# -------------------------------------

# ------------------
# Debugging (mainly)
# 0 - no debugging
# 1 - show regexes being used
# 2 - sort the input patterns
# ------------------
my ($PRINT_PATTERNS) = 0;

# ------------------
# If true, stack must contain "com.gemstone" at least once, otherwise
# it gets elided.
#
# Thanks, klund!  This is a great idea!
# ------------------
my($omit_foreign_stacks) = 1;

# ------------------
# Load the list of patterns
# ------------------
&init_patterns();

# ------------------
# Process input files
# ------------------
for (;;) {
  last if $#ARGV < 0;
  &process_file($ARGV[0]);
  shift @ARGV;
  }
exit 0;

# ------------------
# Current line read from IN
# ------------------
my($current_line);

# ------------------
# All known patterns
# ------------------
my($KNOWN_PATTERNS);

# ------------------
# Analyze stack dumps in a file
#
# Arguments:
#  $in - file to read from
# ------------------
sub process_file {
  my($in) = @_;
  my($announced) = 0;

  open(IN, "<$in") || die "$in: $!";
  for (;;) {
    &get_line();
    last unless defined($current_line);
    next unless &is_start_dump();

    if (!$announced) {
      print STDOUT "[Stacks from $in:]\n";
      $announced = 1;
      }
    print STDOUT $current_line;

    &get_line();
    die "No thread dump after 'Full thread dump'"
        unless defined($current_line);
    die "Line after 'Full thread dump' not empty" 
        unless &is_empty($current_line);
    print STDOUT $current_line;
    &get_line();

    my(@stacks) = &get_stacks();
    @stacks = &elide_stacks(@stacks);
    &print_stacks(@stacks);
    }
  close IN;
  }

# ------------------
# Get the next line.
#
# Arguments: none
# Returns: the current line ($current_line)
# Side effects: modifies $current_line
# ------------------
sub get_line {
  $current_line = <IN>;
  return $current_line;
  }

# ------------------
# Return true if the current line is the start of a thread dump
#
# Arguments: none
# Returns: 1 if $current_line matches the beginning of a thread dump
# ------------------
sub is_start_dump {
  return $current_line =~ /^Full thread dump .*/;
  }

# ------------------
# Easy test to see if we have an empty line
#
# Arguments: $arg - the line to test
# Returns: 1 if the line is empty
# ------------------
sub is_empty {
  my($arg) = @_;

  chomp($arg);
  return $arg eq "";
  }

# ------------------
# Return all thread stacks in current dump.
#
# Arguments: none
# Returns: an array, with each element a multi-line String holding
#          a thread stack
# Notes: Current line must be on first line of dump
# ------------------
sub get_stacks {
  my(@result) = ();
  my($current);

  for (;;) {
    last unless defined($current_line);
    last unless &is_start_stack();
    $current = &next_thread_stack();
    push @result, $current;
    }
  return @result;
  }

# ------------------
# Return true if the current line is the start of a thread stack
#
# Arguments: none
# Returns: 1 if the current line looks like the beginning of a thread stack
# ------------------
sub is_start_stack {
  return $current_line =~ /^"[^"]*" .*prio=\d+ tid=0x[^ ]+ nid=0x[^ ]+ .*/;
  }

# ------------------
# Return next thread stack
#
# Arguments: none
# Returns: a multi-line String with the thread stack, including the
#          blank line at the end.
#
# Notes: Current line must be on the first line of a thread stack
# ------------------
sub next_thread_stack {
  my($result) = "";

  # Save header
  $result = $current_line;
  for (;;) {
    &get_line();
    last if !defined($current_line);
    last if &is_start_stack();
    last if $current_line =~ /^\[/; # GemFire log message

    $result .= $current_line;

    # Optional empty line.  Scarf it (save with stack) if it exists.
    if (&is_empty($current_line)) {
      &get_line(); # point at next unused line
      last;
      }
    }
  return $result;
  }

# ------------------
# Remove uninteresting stacks from list and return interesting ones

# Arguments: @stacks -- all stacks in thread dump
# Returns: @result -- list of interesting stacks (some omitted)
# ------------------
sub elide_stacks {
  my(@stacks) = @_;
  my(@result) = ();

  my($each);
  foreach $each (@stacks) {
    next if &known_stack($each);
    push @result, $each;
  }
  return @result;
}

# ------------------
# Return 1 if the stack matches one we don't care about
#
# Arguments: $stack - stack to test
# Returns: 1 if the stack is boring
# ------------------
sub known_stack {
  my($stack) = @_;
  my($each);

  if ($omit_foreign_stacks) {
    return 1 if $stack !~ /com\.gemstone\./m;
  }
  foreach $each (@KNOWN_PATTERNS) {
    return 1 if &match_stack($stack, $each);
  }
  return 0;
}

# ------------------
# Return 1 if the given stack matches the given pattern
#
# Arguments:
#   $stack - stack to test against
#   $pattern - pattern to match
# ------------------
sub match_stack {
  my($stack, $pattern) = @_;
  my(@stack_lines) = &split_lines($stack);
  my(@pattern_lines) = &split_lines($pattern);

  # Pattern doesn't have empty line at end of stack, but otherwise they
  # should have same number of lines.
  return 0 unless $#stack_lines == $#pattern_lines;

  my($i);
  for ($i = 0; $i <= $#pattern_lines; $i ++) {
    my($str) = $stack_lines[$i];
    my($pat) = $pattern_lines[$i];
    # Explicitly anchor each pattern against beginning- and end-of line.
    return 0 unless $str =~ /^$pat$/;
  }
  return 1;
}

# ------------------
# Split a multi-line string into components
# Yeah, I know, this is simple, but it's clearer if it's broken out...
#
# Arguments: $lines - the lines to split up
# Returns: @lines - the lines, as an array
# ------------------
sub split_lines {
  my($lines) = @_;
  my(@result) = split /\n/, $lines;
  return @result;
  }

# ------------------
# Print a stack list to STDOUT
#
# Arguments: @stacks -- stacks to print
# ------------------
sub print_stacks {
  my(@stacks) = @_;

  my($each);
  foreach $each (@stacks) {
    print STDOUT $each;
    }
  }

# TODO: use this instead of mangling stacks by hand
sub prepPattern() {
  my($pat) = @_;
  my($result) = $pat;

  # Ad-hoc processing...

  $result =~ s/"Bridge Server Acceptor [^"]+"/"Bridge Server Acceptor [^"]+"/;
  $result =~ s/"Cache Server Acceptor [^"]+"/"Cache Server Acceptor [^"]+"/;
  $result =~ s/"Cache Server Selector [^"]+"/"Cache Server Selector [^"]+"/;
  $result =~ s/"Distribution Locator on [^ "]+/"Distribution Locator on [^ "]+/;
  $result =~ s/"Evictor Thread for [^"]+"/"Evictor Thread for [^"]+"/;
#  $result =~ s/"Expiry \d+"/"Expiry \\d+"/;
  $result =~ s/"Function Execution Processor\d+/"Function Execution Processor\\d+/;
  $result =~ s/"Gateway Event Processor from [^"]+"/"Gateway Event Processor from [^"]+"/;
  $result =~ s/"Handshaker [^ ]+ Thread \d+"/"Handshaker [^ ]+ Thread \\d+"/;
  $result =~ s/"Asynchronous disk writer for region [^ ]+"/"Asynchronous disk writer for region [^ ]+"/;
  $result =~ s/"P2P-Handshaker [^ ]+ Thread \d+"/"P2P-Handshaker [^ ]+ Thread \\d+"/;
  $result =~ s/"P2P Listener Thread [^"]+"/"P2P Listener Thread [^"]+"/;
  $result =~ s/"PartitionedRegion Message Processor\d+"/"PartitionedRegion Message Processor\\d+"/;
  $result =~ s/"Pooled High Priority Message Processor \d+/"Pooled High Priority Message Processor \\d+/;
  $result =~ s/"Pooled Message Processor \d+/"Pooled Message Processor \\d+/;
  $result =~ s/"Pooled Serial Message Processor \d+/"Pooled Serial Message Processor \\d+/;
  $result =~ s/"Pooled Waiting Message Processor\d+/"Pooled Waiting Message Processor\\d+/;
  $result =~ s/"Pooled Waiting Message Processor \d+/"Pooled Waiting Message Processor \\d+/;
  $result =~ s/"ServerConnection [^"]+"/"ServerConnection [^"]+"/;
  $result =~ s/"Thread-\d+/"Thread-\\d+/;
  $result =~ s/"[^-]+-edgeDescript"/"[^-]+-edgeDescript"/;
  $result =~ s/"locatoragent_bridgeds[^"]+"/"locatoragent_bridgeds[^"]+"/;
  $result =~ s/"locatoragent_ds[^"]+"/"locatoragent_ds[^"]+"/;
  $result =~ s/"pool-\d+-thread-\d+"/"pool-\\d+-thread-\\d+"/;
  $result =~ s/"poolTimer-[^-]+-\d+"/"poolTimer-[^-]+-\\d+"/;
  $result =~ s/CacheClientProxy Message Dispatcher for [^"]+"/CacheClientProxy Message Dispatcher for [^"]+"/;
  $result =~ s/Lock Grantor for [^"]+"/Lock Grantor for [^"]+"/;
  $result =~ s/ServerMonitor monitoring dead connections on [^"]+"/ServerMonitor monitoring dead connections on [^"]+"/;
  $result =~ s/ServerMonitor monitoring live connections on [^"]+"/ServerMonitor monitoring live connections on [^"]+"/;
  $result =~ s/^"locator request thread[^"]+"/"locator request thread\[^"]+"/;
  $result =~ s/waiting on condition .*/waiting on condition .*/;
#  $result =~ s/Cache Client Update Thread to [^"]+"/Cache Client Update Thread to [^"]+"/;

  # Literalize dollar-signs
  $result =~ s/\$/\\\$/g;
#  $result =~ s/\\$/\\\$/g;
  $result =~ s/\$\d+\./\$\\d+./; # inners

  # Literalize dots
  $result =~ s/\./\\./g; # do this first before adding our own metas

  # Have to do this after the dots, or the pattern doesn't work :-(
  $result =~ s/P2P message reader for .*/P2P message reader for .*/;
  $result =~ s/"Cache Client Updater Thread  on .*/"Cache Client Updater Thread  on .*/;

  # Get noise out of first line
  $result =~ s/prio=\d+/prio=\\d+/;
  $result =~ s/tid=0x[^ ]+/tid=0x[^ ]+/;
  $result =~ s/nid=0x[^ ]+ .*/nid=0x[^ ]+ .*/; # includes remainder of line

  # Hide the lock addresses
  $result =~ s/<0x[^>]+>/<0x[^>]+>/g;

  # Hide the line numbers
  $result =~ s/\.java:\d+/.java:\\d+/g;

  # Hide the parens
  $result =~ s/\(/\\(/g;
  $result =~ s/\)/\\)/g;

  # Debugging
  # print "$result\n\n";
  # print "----------\n";

  return $result;
  }

# ------------------
# These are the known call stacks to ignore.
# TODO: add JRockit stacks
# Tricks used herein:
#   [^"]+ match one or more occurrences of not-"
#   [^ ]+ match one or more occurrences of not-space
#   [^>]+ match one or more occurrences of not->
#   .*  match anything
#   \.  match a period
#   \d+ match one or more digits (esp. for line numbers
#   \( match left paren
#   \) match right paren

sub init_patterns {
@KNOWN_PATTERNS = ( # com.gemstone patterns

# ---------------------------------------

'"State Logger Consumer Thread" daemon prio=10 tid=0x08b1dc00 nid=0x42d3 waiting on condition [0xcefe0000]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xf3908310> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.internal.sequencelog.SequenceLoggerImpl$ConsumerThread.run(SequenceLoggerImpl.java:94)',

'"Pooled Serial Message Processor 12" daemon prio=10 tid=0x0853a000 nid=0x3ae8 waiting on condition [0xbf94f000]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xc595d178> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:95)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$SerialQueuedExecutorPool$1$1.run(DistributionManager.java:3991)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor 22" daemon prio=10 tid=0x09f66c00 nid=0x4e1d waiting on condition [0xd08c9000]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:283)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:449)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:350)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.take(SynchronousQueueNoSpin.java:884)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4$1.run(DistributionManager.java:833)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor 5" daemon prio=10 tid=0x082b6000 nid=0x684c waiting on condition [0xb049f000..0xb04a0150]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:283)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:449)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:350)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.take(SynchronousQueueNoSpin.java:884)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$5$1.run(DistributionManager.java:869)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor2" daemon prio=10 tid=0x0869c800 nid=0x6a05 waiting on condition [0xb052f000..0xb0530150]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:283)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:449)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:350)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.take(SynchronousQueueNoSpin.java:884)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$7$1.run(DistributionManager.java:940)
	at java.lang.Thread.run(Thread.java:619)',

'"CacheClientProxy Message Dispatcher for client odin(29133:loner):43168:6160d1a6:edgegemfire1_odin_29133" daemon prio=10 tid=0x0870f000 nid=0x2bb1 runnable [0xdd95c000..0xdd95ced0]
   java.lang.Thread.State: RUNNABLE
	at java.net.SocketOutputStream.socketWrite0(Native Method)
	at java.net.SocketOutputStream.socketWrite(SocketOutputStream.java:92)
	at java.net.SocketOutputStream.write(SocketOutputStream.java:136)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.flushBuffer(Message.java:440)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.sendBytes(Message.java:416)
	- locked <0xe5706030> (a java.nio.HeapByteBuffer)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.send(Message.java:880)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.send(Message.java:871)
	at com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy$MessageDispatcher.dispatchMessage(CacheClientProxy.java:2282)
	at com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy$MessageDispatcher.run(CacheClientProxy.java:2024)',

'"CacheClientProxy Message Dispatcher for client odin(29151:loner):50496:7360d1a6:edgegemfire2_odin_29151" daemon prio=10 tid=0x0870c400 nid=0x2baf waiting on condition [0xdd8f8000..0xdd8f9150]
   java.lang.Thread.State: TIMED_WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe57270a8> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:198)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:2054)
	at com.gemstone.gemfire.internal.concurrent.ReentrantLock5$C5.await(ReentrantLock5.java:44)
	at com.gemstone.gemfire.internal.util.concurrent.StoppableCondition.await(StoppableCondition.java:90)
	at com.gemstone.gemfire.internal.util.concurrent.StoppableCondition.await(StoppableCondition.java:80)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue$BlockingHARegionQueue.waitForData(HARegionQueue.java:2101)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue.getAndRemoveNextAvailableID(HARegionQueue.java:907)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue$DurableHARegionQueue.getNextAvailableIDFromList(HARegionQueue.java:2176)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue.peek(HARegionQueue.java:1157)
	at com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy$MessageDispatcher.run(CacheClientProxy.java:2012)',

'"Pooled High Priority Message Processor 3" daemon prio=10 tid=0x08121800 nid=0x74ef waiting on condition [0xaee32000..0xaee32f50]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:283)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:449)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:350)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.take(SynchronousQueueNoSpin.java:884)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$5$1.run(DistributionManager.java:869)
	at java.lang.Thread.run(Thread.java:619)',

'"View Message Processor" daemon prio=10 tid=0x08094800 nid=0x5c24 waiting on condition [0xaf10b000..0xaf10be50]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xb4cf4fe8> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$3$1.run(DistributionManager.java:800)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor 1" daemon prio=10 tid=0x08498400 nid=0x5ca8 waiting on condition [0xdf6b3000..0xdf6b4150]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:283)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:449)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:350)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.take(SynchronousQueueNoSpin.java:884)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4$1.run(DistributionManager.java:833)
	at java.lang.Thread.run(Thread.java:619)',

'"DM-MemberEventInvoker" daemon prio=10 tid=0xe0c17400 nid=0x5c26 waiting on condition [0xdfd8c000..0xdfd8cf50]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe538ba00> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$MemberEventInvoker.run(DistributionManager.java:2242)
	at java.lang.Thread.run(Thread.java:619)',

'"UDP Incoming Message Handler" daemon prio=10 tid=0x08420800 nid=0x5c0b in Object.wait() [0xdfe7f000..0xdfe7fe50]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe5396740> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.org.jgroups.util.Queue.remove(Queue.java:278)
	- locked <0xe5396740> (a java.lang.Object)
	at com.gemstone.org.jgroups.protocols.TP$IncomingMessageHandler.run(TP.java:1502)
	at java.lang.Thread.run(Thread.java:619)',

'"Function Execution Processor0" daemon prio=10 tid=0xe0c0f800 nid=0x5bf7 waiting on condition [0xe001c000..0xe001d0d0]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe53afb90> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:95)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:90)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$8$1.run(DistributionManager.java:983)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor0" daemon prio=10 tid=0xe0c11000 nid=0x5bf4 waiting on condition [0xe006d000..0xe006e150]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe53b0618> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:95)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:90)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$7$1.run(DistributionManager.java:940)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor 0" daemon prio=10 tid=0xe0c11c00 nid=0x5bf3 waiting on condition [0xe00be000..0xe00bf1d0]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe5390248> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:95)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:90)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$5$1.run(DistributionManager.java:869)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor 0" daemon prio=10 tid=0xe0c03c00 nid=0x5bf1 waiting on condition [0xe010f000..0xe010fe50]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe53b1040> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:95)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:90)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:566)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4$1.run(DistributionManager.java:833)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 29384 Thread 14" daemon prio=10 tid=0x085d3800 nid=0x60e7 runnable [0xe0439000..0xe043a150]
   java.lang.Thread.State: RUNNABLE
	at java.net.SocketInputStream.socketRead0(Native Method)
	at java.net.SocketInputStream.read(SocketInputStream.java:129)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.fetchHeader(Message.java:484)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.readHeaderAndPayload(Message.java:507)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.read(Message.java:451)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.recv(Message.java:896)
	- locked <0xf333fe58> (a java.nio.HeapByteBuffer)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.recv(Message.java:910)
	at com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand.readRequest(BaseCommand.java:792)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.doNormalMsg(ServerConnection.java:575)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.doOneMessage(ServerConnection.java:729)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.run(ServerConnection.java:787)
	at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$1$1.run(AcceptorImpl.java:493)
	at java.lang.Thread.run(Thread.java:619)',

'"ThresholdEventProcessor" daemon prio=10 tid=0xe0336000 nid=0x5ce1 waiting on condition [0xdff83000..0xdff83fd0]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe530adf0> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:95)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 29384 Thread 4" daemon prio=10 tid=0x0814e800 nid=0x5e06 waiting on condition [0xdf7f7000..0xdf7f7ed0]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe5403a18> (a java.util.concurrent.SynchronousQueue$TransferStack)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.SynchronousQueue$TransferStack.awaitFulfill(SynchronousQueue.java:422)
	at java.util.concurrent.SynchronousQueue$TransferStack.transfer(SynchronousQueue.java:323)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:857)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$1$1.run(AcceptorImpl.java:493)
	at java.lang.Thread.run(Thread.java:619)',

'"Asynchronous disk writer" daemon prio=10 tid=0x083a3c00 nid=0x14fd in Object.wait() [0xe2dfc000..0xe2dfd130]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe87a05f8> (a com.gemstone.gemfire.internal.concurrent.ConcurrentHashMap5)
	at com.gemstone.gemfire.internal.cache.Oplog$WriterThread.run(Oplog.java:4705)
	- locked <0xe87a05f8> (a com.gemstone.gemfire.internal.concurrent.ConcurrentHashMap5)
	at java.lang.Thread.run(Thread.java:619)',

'"Asynchronous disk writer for region disk" daemon prio=10 tid=0xde4cb000 nid=0x483c in Object.wait() [0xdea0b000]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe591e5c8> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:292)
	at com.gemstone.gemfire.internal.cache.DiskStoreImpl$FlusherThread.waitUntilFlushIsReady(DiskStoreImpl.java:1533)
	- locked <0xe591e5c8> (a java.lang.Object)
	at com.gemstone.gemfire.internal.cache.DiskStoreImpl$FlusherThread.run(DiskStoreImpl.java:1571)
	at java.lang.Thread.run(Thread.java:619)',


'"BridgeMembership Event Invoker" daemon prio=10 tid=0x0814a400 nid=0x4d91 in Object.wait() [0x58bad000..0x58bade20]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0x473fe568> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0x473fe568> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"BridgeMembership Event Invoker" daemon prio=10 tid=0xe072d400 nid=0xf3f in Object.wait() [0xe0257000..0xe0257f30]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xe54b6cb8> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"BridgeServer-LoadPollingThread" daemon prio=10 tid=0xd9186800 nid=0x7537 in Object.wait() [0xd8610000..0xd8610fb0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at com.gemstone.gemfire.cache.server.internal.LoadMonitor$PollingThread.run(LoadMonitor.java:157)
	- locked <0xdd829e78> (a java.lang.Object)',

'"BridgeServer-LoadPollingThread" daemon prio=10 tid=0xe0260400 nid=0x35c8 in Object.wait() [0xdf166000..0xdf166f50]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe55f6c98> (a java.lang.Object)
	at com.gemstone.gemfire.cache.server.internal.LoadMonitor$PollingThread.run(LoadMonitor.java:157)
	- locked <0xe55f6c98> (a java.lang.Object)',

'"Cache Client Updater Thread  on stut.gemstone.com:20760" daemon prio=10 tid=0x080da000 nid=0x54d7 runnable [0xdfeb1000..0xdfeb1eb0]
   java.lang.Thread.State: RUNNABLE
	at java.net.SocketInputStream.socketRead0(Native Method)
	at java.net.SocketInputStream.read(SocketInputStream.java:129)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.fetchHeader(Message.java:474)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.readHeaderAndPayload(Message.java:497)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.read(Message.java:441)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.recv(Message.java:887)
	- locked <0xe55e56d8> (a java.nio.HeapByteBuffer)
	at com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientUpdater.processMessages(CacheClientUpdater.java:1214)
	at com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientUpdater.run(CacheClientUpdater.java:327)',

'"Cache Server Acceptor 0.0.0.0/0.0.0.0:25310 local port: 25310" daemon prio=10 tid=0x081b1000 nid=0x67fe runnable [0xbf26f000..0xbf26ffb0]
   java.lang.Thread.State: RUNNABLE
	at java.net.PlainSocketImpl.socketAccept(Native Method)
	at java.net.PlainSocketImpl.accept(PlainSocketImpl.java:384)
	- locked <0xc4fa0248> (a java.net.SocksSocketImpl)
	at java.net.ServerSocket.implAccept(ServerSocket.java:453)
	at java.net.ServerSocket.accept(ServerSocket.java:421)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl.accept(AcceptorImpl.java:1132)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl.run(AcceptorImpl.java:1067)
	at java.lang.Thread.run(Thread.java:619)',

'"Cache Server Selector /0:0:0:0:0:0:0:0:27192 local port: 27192" daemon prio=10 tid=0xd9185800 nid=0x7536 runnable [0xd8661000..0xd8661f30]
   java.lang.Thread.State: RUNNABLE
	at sun.nio.ch.EPollArrayWrapper.epollWait(Native Method)
	at sun.nio.ch.EPollArrayWrapper.poll(EPollArrayWrapper.java:184)
	at sun.nio.ch.EPollSelectorImpl.doSelect(EPollSelectorImpl.java:65)
	at sun.nio.ch.SelectorImpl.lockAndDoSelect(SelectorImpl.java:69)
	- locked <0xdd827f78> (a sun.nio.ch.Util$1)
	- locked <0xdd827f48> (a java.util.Collections$UnmodifiableSet)
	- locked <0xdd7c59b0> (a sun.nio.ch.EPollSelectorImpl)
	at sun.nio.ch.SelectorImpl.select(SelectorImpl.java:80)
	at sun.nio.ch.SelectorImpl.select(SelectorImpl.java:84)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl.runSelectorLoop(AcceptorImpl.java:898)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$5.run(AcceptorImpl.java:571)
	at java.lang.Thread.run(Thread.java:619)',

'"CacheClientProxy Message Dispatcher for client host=10.80.250.40 port=60579" daemon prio=3 tid=0x00850400 nid=0x117 waiting on condition [0xdcc7f000..0xdcc7f970]
   java.lang.Thread.State: TIMED_WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xec283750> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:198)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:2054)
	at com.gemstone.gemfire.internal.concurrent.ReentrantLock5$C5.await(ReentrantLock5.java:43)
	at com.gemstone.gemfire.internal.util.concurrent.StoppableCondition.await(StoppableCondition.java:89)
	at com.gemstone.gemfire.internal.util.concurrent.StoppableCondition.await(StoppableCondition.java:79)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue$BlockingHARegionQueue.waitForData(HARegionQueue.java:2029)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue.getNextAvailableID(HARegionQueue.java:903)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue.getNextAvailableIDFromList(HARegionQueue.java:1100)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue.peek(HARegionQueue.java:1115)
	at com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy$MessageDispatcher.run(CacheClientProxy.java:2073)',

'"ClientHealthMonitor Thread" daemon prio=10 tid=0x081c9400 nid=0x67fc waiting on condition [0xbf2c0000..0xbf2c1030]
   java.lang.Thread.State: TIMED_WAITING (sleeping)
	at java.lang.Thread.sleep(Native Method)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ClientHealthMonitor$ClientHealthMonitorThread.run(ClientHealthMonitor.java:835)',

'"Distribution Locator on fscott/10.80.250.88[20470]" daemon prio=10 tid=0x08343800 nid=0x2b96 runnable [0xafd07000..0xafd07f30]
   java.lang.Thread.State: RUNNABLE
	at java.net.PlainSocketImpl.socketAccept(Native Method)
	at java.net.PlainSocketImpl.accept(PlainSocketImpl.java:384)
	- locked <0xb4ed63c8> (a java.net.SocksSocketImpl)
	at java.net.ServerSocket.implAccept(ServerSocket.java:453)
	at java.net.ServerSocket.accept(ServerSocket.java:421)
	at com.gemstone.org.jgroups.stack.tcpserver.TcpServer.run(TcpServer.java:182)
	at com.gemstone.org.jgroups.stack.tcpserver.TcpServer.access$000(TcpServer.java:48)
	at com.gemstone.org.jgroups.stack.tcpserver.TcpServer$2.run(TcpServer.java:138)',

'"Distribution Locator on hs20a[24832]" daemon prio=10 tid=0x576b9000 nid=0x7162 runnable [0x57d43000..0x57d435f0]
   java.lang.Thread.State: RUNNABLE
	at java.net.PlainSocketImpl.socketAccept(Native Method)
	at java.net.PlainSocketImpl.accept(PlainSocketImpl.java:384)
	- locked <0x473dc7c8> (a java.net.SocksSocketImpl)
	at java.net.ServerSocket.implAccept(ServerSocket.java:453)
	at java.net.ServerSocket.accept(ServerSocket.java:421)
	at com.gemstone.org.jgroups.stack.tcpserver.TcpServer.run(TcpServer.java:182)
	at com.gemstone.org.jgroups.stack.tcpserver.TcpServer$2.run(TcpServer.java:138)',

'"DM-MemberEventInvoker" daemon prio=10 tid=0x08328000 nid=0x2bbe in Object.wait() [0xaf88e000..0xaf88ef30]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb4ed7e50> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xb4ed7e50> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$MemberEventInvoker.run(DistributionManager.java:2130)
	at java.lang.Thread.run(Thread.java:619)',

'"DM-MemberEventInvoker" daemon prio=10 tid=0xe8248800 nid=0x2f7b in Object.wait() [0xe7b92000..0xe7b921b0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xeceaf390> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$MemberEventInvoker.run(DistributionManager.java:2130)
	at java.lang.Thread.run(Thread.java:619)',

'"Evictor Thread for /TestRegion" daemon prio=10 tid=0x08464000 nid=0x14ff sleeping[0xe2d5a000..0xe2d5ae30]
   java.lang.Thread.State: TIMED_WAITING (sleeping)
	at java.lang.Thread.sleep(Native Method)
	at com.gemstone.gemfire.internal.cache.lru.HeapLRUCapacityController$EvictorThread.run(HeapLRUCapacityController.java:522)',

'"FD_SOCK ClientConnectionHandler" daemon prio=10 tid=0xaede1c00 nid=0x2bda runnable [0xafdaf000..0xafdaff30]
   java.lang.Thread.State: RUNNABLE
	at java.net.SocketInputStream.socketRead0(Native Method)
	at java.net.SocketInputStream.read(SocketInputStream.java:129)
	at java.net.SocketInputStream.read(SocketInputStream.java:182)
	at com.gemstone.org.jgroups.protocols.FD_SOCK$ClientConnectionHandler.run(FD_SOCK.java:1665)',

'"FD_SOCK Ping thread" daemon prio=10 tid=0x0840c400 nid=0x2eb5 runnable [0xaf5fe000..0xaf5fee30]
   java.lang.Thread.State: RUNNABLE
	at java.net.SocketInputStream.socketRead0(Native Method)
	at java.net.SocketInputStream.read(SocketInputStream.java:129)
	at java.net.SocketInputStream.read(SocketInputStream.java:182)
	at com.gemstone.org.jgroups.protocols.FD_SOCK.run(FD_SOCK.java:654)
	at java.lang.Thread.run(Thread.java:619)',

'"FD_SOCK listener thread" daemon prio=10 tid=0x08327800 nid=0x2ba6 runnable [0xaf930000..0xaf931130]
   java.lang.Thread.State: RUNNABLE
	at java.net.PlainSocketImpl.socketAccept(Native Method)
	at java.net.PlainSocketImpl.accept(PlainSocketImpl.java:384)
	- locked <0xb4ee10d0> (a java.net.SocksSocketImpl)
	at java.net.ServerSocket.implAccept(ServerSocket.java:453)
	at java.net.ServerSocket.accept(ServerSocket.java:421)
	at com.gemstone.org.jgroups.protocols.FD_SOCK$ServerSocketHandler.run(FD_SOCK.java:1531)
	at java.lang.Thread.run(Thread.java:619)',

'"Function Execution Processor0" daemon prio=10 tid=0xb0186000 nid=0x1414 in Object.wait() [0xaf9d9000..0xaf9da0b0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee885078> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:317)
	- locked <0xee885078> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:94)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$7$1.run(DistributionManager.java:970)
	at java.lang.Thread.run(Thread.java:619)',

'"Function Execution Processor1" daemon prio=10 tid=0xde95f400 nid=0x46cb waiting on condition [0xdefee000]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:283)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:449)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:350)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.take(SynchronousQueueNoSpin.java:884)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:947)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:576)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$8$1.run(DistributionManager.java:996)
	at java.lang.Thread.run(Thread.java:619)',

'"Gateway Event Processor from ds_2 to ds_1" daemon prio=10 tid=0x08533800 nid=0x6838 in Object.wait() [0xbf2ad000..0xbf2ae130]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xc4f2a998> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.internal.cache.GatewayImpl.waitToBecomePrimary(GatewayImpl.java:891)
	- locked <0xc4f2a998> (a java.lang.Object)
	at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.run(GatewayImpl.java:1392)',

'"Gateway Event Processor from ds_2 to ds_1" daemon prio=10 tid=0xbf792c00 nid=0x680d waiting for monitor entry [0xbf0da000..0xbf0daf30]
   java.lang.Thread.State: BLOCKED (on object monitor)
	at com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue.remove(SingleWriteSingleReadRegionQueue.java:281)
	- waiting to lock <0xc5016180> (a com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue)
	at com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue.remove(SingleWriteSingleReadRegionQueue.java:313)
	at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.eventQueueRemove(GatewayImpl.java:1278)
	at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.run(GatewayImpl.java:1493)',

'"Gateway Event Processor from ds_1 to ds_2" daemon prio=10 tid=0x08349800 nid=0x736f waiting on condition [0x590f5000..0x590f54f0]
   java.lang.Thread.State: TIMED_WAITING (sleeping)
	at java.lang.Thread.sleep(Native Method)
	at com.gemstone.gemfire.internal.cache.SingleWriteSingleReadRegionQueue.peek(SingleWriteSingleReadRegionQueue.java:382)
	at com.gemstone.gemfire.internal.cache.GatewayImpl$GatewayEventProcessor.run(GatewayImpl.java:1504)',

'"P2P Listener Thread tcp:///10.80.250.201:36242" daemon prio=10 tid=0x08222400 nid=0x340e runnable [0xe0131000..0xe01311d0]
   java.lang.Thread.State: RUNNABLE
	at sun.nio.ch.ServerSocketChannelImpl.accept0(Native Method)
	at sun.nio.ch.ServerSocketChannelImpl.accept(ServerSocketChannelImpl.java:145)
	- locked <0xe55773b8> (a java.lang.Object)
	at com.gemstone.gemfire.internal.tcp.TCPConduit.run(TCPConduit.java:565)
	at java.lang.Thread.run(Thread.java:619)',

'"GlobalTXTimeoutMonitor" daemon prio=10 tid=0xe8261000 nid=0x2fee in Object.wait() [0xe758e000..0xe758e1b0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xecf6b8a8> (a java.util.Collections$SynchronizedSortedSet)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.internal.jta.TransactionManagerImpl$TransactionTimeOutThread.run(TransactionManagerImpl.java:691)
	- locked <0xecf6b8a8> (a java.util.Collections$SynchronizedSortedSet)
	at java.lang.Thread.run(Thread.java:619)',

'"GlobalTXTimeoutMonitor" daemon prio=10 tid=0xdd6e6400 nid=0x5963 in Object.wait() [0xdf19d000..0xdf19dfd0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.internal.jta.TransactionManagerImpl$TransactionTimeOutThread.run(TransactionManagerImpl.java:695)
	- locked <0xe5759368> (a java.util.Collections$SynchronizedSortedSet)
	at java.lang.Thread.run(Thread.java:619)',

'"Handshaker 0.0.0.0/0.0.0.0:21083 Thread 0" daemon prio=10 tid=0x081e0000 nid=0x737e in Object.wait() [0x591e8000..0x591e8670]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0x478776e0> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"Handshaker 0.0.0.0/0.0.0.0:29959 Thread 0" daemon prio=3 tid=0x001bb000 nid=0x86 in Object.wait() [0xe3d7f000..0xe3d7faf0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xec3f1610> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xec3f1610> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"IDLE p2pDestreamer" daemon prio=10 tid=0xc0004c00 nid=0x624 in Object.wait() [0xbedbc000..0xbedbce30]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xc5f9c8d8> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.internal.tcp.MsgDestreamer$DestreamerIS.waitForData(MsgDestreamer.java:338)
	- locked <0xc5f9c8d8> (a java.lang.Object)
	at com.gemstone.gemfire.internal.tcp.MsgDestreamer$DestreamerIS.waitForAvailableData(MsgDestreamer.java:414)
	at com.gemstone.gemfire.internal.tcp.MsgDestreamer$DestreamerIS.read(MsgDestreamer.java:456)
	at java.io.DataInputStream.readByte(DataInputStream.java:248)
	at com.gemstone.gemfire.internal.InternalDataSerializer.readDSFID(InternalDataSerializer.java:1383)
	at com.gemstone.gemfire.internal.tcp.MsgDestreamer$DestreamerThread.run(MsgDestreamer.java:218)',

'"IDLE p2pDestreamer" daemon prio=10 tid=0x081d7400 nid=0x2592 in Object.wait() [0xda6fe000..0xda6ff130]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.internal.tcp.MsgDestreamer$DestreamerIS.waitForData(MsgDestreamer.java:338)
	- locked <0xe61986e8> (a java.lang.Object)
	at com.gemstone.gemfire.internal.tcp.MsgDestreamer$DestreamerIS.waitForAvailableData(MsgDestreamer.java:414)
	at com.gemstone.gemfire.internal.tcp.MsgDestreamer$DestreamerIS.read(MsgDestreamer.java:456)
	at java.io.DataInputStream.readByte(DataInputStream.java:248)
	at com.gemstone.gemfire.internal.InternalDataSerializer.readDSFID(InternalDataSerializer.java:1385)
	at com.gemstone.gemfire.internal.tcp.MsgDestreamer$DestreamerThread.run(MsgDestreamer.java:218)',

'"JoinProcessor" daemon prio=10 tid=0x0819e800 nid=0x311b in Object.wait() [0xa4c6d000..0xa4c6deb0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xa9f8b2d0> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.internal.admin.remote.RemoteGfManagerAgent$JoinProcessor.run(RemoteGfManagerAgent.java:1228)
	- locked <0xa9f8b2d0> (a java.lang.Object)',

'"Lock Grantor for __PRLS" daemon prio=10 tid=0xe829e400 nid=0x2946 in Object.wait() [0xe6f39000..0xe6f39eb0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.distributed.internal.locks.DLockGrantor$DLockGrantorThread.run(DLockGrantor.java:3600)
	- locked <0xecfb3b98> (a java.lang.Object)',

'"Lock Grantor for com.gemstone.gemfire.internal.cache.GatewayHubImpl-ds2" daemon prio=10 tid=0x08299400 nid=0x5987 in Object.wait() [0x9fd69000..0x9fd69ea0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xa5587fb8> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.distributed.internal.locks.DLockGrantor$DLockGrantorThread.run(DLockGrantor.java:3600)
	- locked <0xa5587fb8> (a java.lang.Object)',

'"OplogRoller" daemon prio=10 tid=0x080dfc00 nid=0x4d8f in Object.wait() [0x58b0b000..0x58b0bf20]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0x47427df0> (a java.util.LinkedHashMap)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.gemfire.internal.cache.ComplexDiskRegion$OplogRoller.run(ComplexDiskRegion.java:771)
	- locked <0x47427df0> (a java.util.LinkedHashMap)
	at java.lang.Thread.run(Thread.java:619)',

'"P2P-Handshaker /10.80.250.88:0 Thread 0" daemon prio=10 tid=0x08081c00 nid=0x2ebe in Object.wait() [0xaf37a000..0xaf37afb0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb503b628> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xb503b628> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"P2P-Handshaker fscott/10.80.250.88:0 Thread 13" daemon prio=10 tid=0xe82c9000 nid=0x31d2 in Object.wait() [0xe6aad000..0xe6aadeb0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xed046140> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"P2P-Handshaker clark.gemstone.com/10.80.250.40:0 Thread 4" daemon prio=3 tid=0x006e3000 nid=0x915 in Object.wait() [0xde67f000..0xde67fa70]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xfacdf338> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"P2P message reader for fscott(10661):35629/33314 SHARED=true ORDERED=false UID=1" daemon prio=10 tid=0x0854e400 nid=0x2bdf runnable [0xaef5d000..0xaef5dfb0]
   java.lang.Thread.State: RUNNABLE
	at sun.nio.ch.FileDispatcher.read0(Native Method)
	at sun.nio.ch.SocketDispatcher.read(SocketDispatcher.java:21)
	at sun.nio.ch.IOUtil.readIntoNativeBuffer(IOUtil.java:233)
	at sun.nio.ch.IOUtil.read(IOUtil.java:200)
	at sun.nio.ch.SocketChannelImpl.read(SocketChannelImpl.java:236)
	- locked <0xb50360d0> (a java.lang.Object)
	at com.gemstone.gemfire.internal.tcp.Connection.runNioReader(Connection.java:1555)
	at com.gemstone.gemfire.internal.tcp.Connection.run(Connection.java:1463)
	at java.lang.Thread.run(Thread.java:619)',

'"PRHAFailureAndRecoveryThread" daemon prio=10 tid=0xe828a000 nid=0x303d waiting on condition [0xe7449000..0xe7449e30]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xecf6b4d8> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.internal.cache.PRHAFailureAndRecoveryProvider$1.run(PRHAFailureAndRecoveryProvider.java:330)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor0" daemon prio=10 tid=0x08292000 nid=0x2ba0 in Object.wait() [0xafb1e000..0xafb1ee30]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb4f1dcc0> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xb4f1dcc0> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:89)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$12.run(DistributionManager.java:915)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor1" daemon prio=10 tid=0x085a1000 nid=0x35f5 in Object.wait() [0xe8100000..0xe8100f30]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xecf8eec8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xecf8eec8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$12.run(DistributionManager.java:915)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor18" daemon prio=10 tid=0x08f7e400 nid=0x3f2d in Object.wait() [0xa26ad000..0xa26ae030]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xb4f0c6a8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$12.run(DistributionManager.java:915)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor18" daemon prio=3 tid=0x01ba9400 nid=0xdf7 in Object.wait() [0xd757f000..0xd757f970]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xfba33338> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$12.run(DistributionManager.java:918)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor0" daemon prio=10 tid=0xdf746800 nid=0x50a4 in Object.wait() [0xdd75a000..0xdd75ae50]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xe541f760> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:89)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$12.run(DistributionManager.java:918)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor0" daemon prio=10 tid=0x08343c00 nid=0x1413 in Object.wait() [0xafa2a000..0xafa2b030]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee884e80> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:317)
	- locked <0xee884e80> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:94)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$6$1.run(DistributionManager.java:927)
	at java.lang.Thread.run(Thread.java:619)',

'"PartitionedRegion Message Processor1" daemon prio=10 tid=0x085b4c00 nid=0x359a in Object.wait() [0xdf74c000..0xdf74ce50]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe57045e0> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:366)
	- locked <0xe57045e0> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:529)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$6$1.run(DistributionManager.java:926)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor0" daemon prio=10 tid=0xaff0b800 nid=0x2b9f in Object.wait() [0xafb6f000..0xafb701b0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb4f1e690> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xb4f1e690> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:89)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$8.run(DistributionManager.java:844)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor1" daemon prio=10 tid=0x083afc00 nid=0x2ece in Object.wait() [0xaf287000..0xaf288130]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb503bbc0> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xb503bbc0> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$8.run(DistributionManager.java:844)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor15" daemon prio=10 tid=0x087dc000 nid=0x2f2e in Object.wait() [0xe711f000..0xe7120030]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xed037a28> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$8.run(DistributionManager.java:844)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor37" daemon prio=3 tid=0x00763400 nid=0xe53 in Object.wait() [0xd957f000..0xd957fb70]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xf681ca38> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$8.run(DistributionManager.java:847)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor1" daemon prio=10 tid=0x083e0400 nid=0x14cb in Object.wait() [0xaf606000..0xaf6070b0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee561800> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:366)
	- locked <0xee561800> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:529)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4$1.run(DistributionManager.java:856)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor0" daemon prio=10 tid=0xb0185800 nid=0x1412 in Object.wait() [0xafac0000..0xafac0fb0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee884ae8> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:317)
	- locked <0xee884ae8> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:94)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4$1.run(DistributionManager.java:856)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor41" daemon prio=10 tid=0xe34f2800 nid=0x1627 in Object.wait() [0xe177b000..0xe177c1b0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee377718> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:443)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:427)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:382)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:410)
	- locked <0xee377718> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:574)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4$1.run(DistributionManager.java:856)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor0" daemon prio=10 tid=0xaff65400 nid=0x2b9e in Object.wait() [0xafbc0000..0xafbc1130]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb4f1eeb8> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xb4f1eeb8> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:89)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$6.run(DistributionManager.java:808)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor26" daemon prio=10 tid=0x087be800 nid=0x2950 in Object.wait() [0xe6e97000..0xe6e980b0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xed03bfa8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$6.run(DistributionManager.java:808)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor119" daemon prio=10 tid=0x08222000 nid=0x3a90 in Object.wait() [0xa4167000..0xa4168030]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xed552350> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xed552350> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$6.run(DistributionManager.java:808)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor47" daemon prio=10 tid=0xbf5f3800 nid=0x32c8 in Object.wait() [0xbf21e000..0xbf21f0b0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xefd93648> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$6.run(DistributionManager.java:813)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor2" daemon prio=10 tid=0xa517e400 nid=0x2bd9 in Object.wait() [0xa443b000..0xa443be30]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xa9fb03f8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xa9fb03f8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$6.run(DistributionManager.java:808)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor0" daemon prio=10 tid=0xb014a800 nid=0x1411 in Object.wait() [0xafb11000..0xafb11f30]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee870288> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:317)
	- locked <0xee870288> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.take(OverflowQueueWithDMStats.java:94)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$3$1.run(DistributionManager.java:820)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor6" daemon prio=10 tid=0xe3cc0400 nid=0x163b in Object.wait() [0xe1e83000..0xe1e83e30]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xf3963be8> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:443)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:427)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:382)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:410)
	- locked <0xf3963be8> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:574)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$3$1.run(DistributionManager.java:820)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor1" daemon prio=10 tid=0xe2fe5400 nid=0x14cd in Object.wait() [0xe2669000..0xe266a1b0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe905ef70> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:366)
	- locked <0xe905ef70> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:529)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$3$1.run(DistributionManager.java:820)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor20" daemon prio=10 tid=0x08385400 nid=0x4998 in Object.wait() [0xe055c000..0xe055ce50]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:427)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:382)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:410)
	- locked <0xf3816478> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:574)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$3$1.run(DistributionManager.java:819)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor11" daemon prio=10 tid=0x08928400 nid=0x3aca in Object.wait() [0xdf0d1000..0xdf0d20d0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:366)
	- locked <0xe58cd858> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:529)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$3$1.run(DistributionManager.java:819)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Serial Message Processor 1" daemon prio=10 tid=0xb0159800 nid=0x1502 in Object.wait() [0xaf5b5000..0xaf5b5eb0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee4df700> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:443)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:427)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue.poll(LinkedBlockingQueue.java:350)
	- locked <0xee4df700> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats.poll(OverflowQueueWithDMStats.java:103)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$SerialQueuedExecutorPool$Slot$1$1.run(DistributionManager.java:3769)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Waiting Message Processor88" daemon prio=3 tid=0x00720400 nid=0xe4b in Object.wait() [0xda57f000..0xda57fbf0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xfba33f48> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$10.run(DistributionManager.java:875)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Waiting Message Processor 3" daemon prio=10 tid=0x08577800 nid=0x4823 waiting on condition [0xdec9d000]
   java.lang.Thread.State: TIMED_WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe466bfa8> (a java.util.concurrent.SynchronousQueue$TransferStack)
	at java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:198)
	at java.util.concurrent.SynchronousQueue$TransferStack.awaitFulfill(SynchronousQueue.java:424)
	at java.util.concurrent.SynchronousQueue$TransferStack.transfer(SynchronousQueue.java:323)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:874)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:945)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:576)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$6$1.run(DistributionManager.java:910)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Waiting Message Processor0" daemon prio=10 tid=0x0843b800 nid=0x14f1 in Object.wait() [0xe30d4000..0xe30d4eb0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe87fd3a8> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:443)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:427)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:382)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:410)
	- locked <0xe87fd3a8> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:574)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$5$1.run(DistributionManager.java:884)
	at java.lang.Thread.run(Thread.java:619)',

'"Queue Removal Thread" daemon prio=10 tid=0x08348c00 nid=0xbf3 in Object.wait() [0xdf57b000..0xdf57c030]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue$QueueRemovalThread.run(HARegionQueue.java:2384)
	- locked <0xe580a748> (a com.gemstone.gemfire.internal.cache.ha.HARegionQueue$QueueRemovalThread)',

'"Queue Removal Thread" daemon prio=10 tid=0x0839d000 nid=0x368d in Object.wait() [0xdf28d000..0xdf28e0d0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe555f4c8> (a com.gemstone.gemfire.internal.cache.ha.HARegionQueue$QueueRemovalThread)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue$QueueRemovalThread.run(HARegionQueue.java:2408)
	- locked <0xe555f4c8> (a com.gemstone.gemfire.internal.cache.ha.HARegionQueue$QueueRemovalThread)',

'"Queued Gateway Listener Thread" daemon prio=10 tid=0xbee04000 nid=0x324f in Object.wait() [0xbefad000..0xbefae130]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xc4ee99e8> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 23296 Thread 0" daemon prio=10 tid=0x083af800 nid=0x4b05 runnable [0xdf8c4000..0xdf8c4fd0]
   java.lang.Thread.State: RUNNABLE
	at java.net.SocketInputStream.socketRead0(Native Method)
	at java.net.SocketInputStream.read(SocketInputStream.java:129)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.fetchHeader(Message.java:474)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.readHeaderAndPayload(Message.java:497)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.read(Message.java:441)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.recv(Message.java:887)
	- locked <0xe53d5fa8> (a java.nio.HeapByteBuffer)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.recv(Message.java:901)
	at com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand.readRequest(BaseCommand.java:582)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.doNormalMsg(ServerConnection.java:551)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.doOneMessage(ServerConnection.java:704)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.run(ServerConnection.java:760)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:989)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$2.run(AcceptorImpl.java:443)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 27192 Thread 22" daemon prio=10 tid=0xd958c000 nid=0x248d in Object.wait() [0xd7fce000..0xd7fcf130]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xdbe4f9f8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$2.run(AcceptorImpl.java:443)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 25310 Thread 1" daemon prio=10 tid=0x082ac800 nid=0x6832 in Object.wait() [0xbfda2000..0xbfda2eb0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xc6f311a0> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$2.run(AcceptorImpl.java:443)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 27192 Thread 0" daemon prio=10 tid=0xd915e800 nid=0x7534 in Object.wait() [0xd8703000..0xd8703e30]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:316)
	- locked <0xdd829f58> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats$1.run(PooledExecutorWithDMStats.java:88)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$2.run(AcceptorImpl.java:443)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 25334 Thread 1" daemon prio=10 tid=0x08081800 nid=0x75bb in Object.wait() [0xdf251000..0xdf251fb0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe56887c8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xe56887c8> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$2.run(AcceptorImpl.java:443)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 28758 Thread 15" daemon prio=10 tid=0x084fc400 nid=0x3ed1 runnable [0xdeaa4000..0xdeaa4ed0]
   java.lang.Thread.State: RUNNABLE
	at java.net.SocketInputStream.socketRead0(Native Method)
	at java.net.SocketInputStream.read(SocketInputStream.java:129)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.fetchHeader(Message.java:475)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.readHeaderAndPayload(Message.java:498)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.read(Message.java:442)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.recv(Message.java:888)
	- locked <0xf3527ae8> (a java.nio.HeapByteBuffer)
	at com.gemstone.gemfire.internal.cache.tier.sockets.Message.recv(Message.java:902)
	at com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand.readRequest(BaseCommand.java:611)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.doNormalMsg(ServerConnection.java:553)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.doOneMessage(ServerConnection.java:706)
	at com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection.run(ServerConnection.java:762)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:989)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$1$1.run(AcceptorImpl.java:443)
	at java.lang.Thread.run(Thread.java:619)',

'"ServerConnection on port 28758 Thread 13" daemon prio=10 tid=0x082d9000 nid=0x3d1c in Object.wait() [0xdea53000..0xdea540d0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe5751e08> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:366)
	- locked <0xe5751e08> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue$Node)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:529)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl$1$1.run(AcceptorImpl.java:443)
	at java.lang.Thread.run(Thread.java:619)',

'"SnapshotResultDispatcher" daemon prio=10 tid=0x08301000 nid=0x315d waiting on condition [0xa3fcb000..0xa3fcbf30]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xa9f8ade8> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.internal.admin.remote.RemoteGfManagerAgent$SnapshotResultDispatcher.run(RemoteGfManagerAgent.java:987)',

'"StatDispatcher" daemon prio=10 tid=0x[0-f]+ nid=0x[0-f]+ waiting on condition \[0x[0-f]+..0x[0-f]+\]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0x[0-f]+> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.internal.admin.remote.RemoteGemFireVM$StatDispatcher.run(RemoteGemFireVM.java:802)',


'"SystemFailure Proctor" daemon prio=10 tid=0x082f9400 nid=0x2b98 waiting on condition [0xafc65000..0xafc66030]
   java.lang.Thread.State: TIMED_WAITING (sleeping)
	at java.lang.Thread.sleep(Native Method)
	at com.gemstone.gemfire.SystemFailure.runProctor(SystemFailure.java:573)
	at com.gemstone.gemfire.SystemFailure$4.run(SystemFailure.java:524)
	at java.lang.Thread.run(Thread.java:619)',

'"SystemFailure WatchDog" daemon prio=10 tid=0x082fc000 nid=0x2b97 in Object.wait() [0xafcb6000..0xafcb6fb0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb4f5cf40> (a java.lang.Object)
	at com.gemstone.gemfire.SystemFailure.runWatchDog(SystemFailure.java:370)
	- locked <0xb4f5cf40> (a java.lang.Object)
	at com.gemstone.gemfire.SystemFailure$3.run(SystemFailure.java:320)
	at java.lang.Thread.run(Thread.java:619)',

'"SystemFailure WatchDog" daemon prio=10 tid=0x0829b400 nid=0x2f37 in Object.wait() [0xe7f14000..0xe7f150b0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at com.gemstone.gemfire.SystemFailure.runWatchDog(SystemFailure.java:370)
	- locked <0xecef7af8> (a java.lang.Object)
	at com.gemstone.gemfire.SystemFailure$3.run(SystemFailure.java:320)
	at java.lang.Thread.run(Thread.java:619)',

'"Thread-7 StatSampler" daemon prio=10 tid=0xaff73000 nid=0x2bc0 in Object.wait() [0xaf7ec000..0xaf7ed030]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb4f21740> (a com.gemstone.gemfire.internal.GemFireStatSampler)
	at com.gemstone.gemfire.internal.HostStatSampler.delay(HostStatSampler.java:239)
	- locked <0xb4f21740> (a com.gemstone.gemfire.internal.GemFireStatSampler)
	at com.gemstone.gemfire.internal.HostStatSampler.run(HostStatSampler.java:296)
	at java.lang.Thread.run(Thread.java:619)',

'"Thread-6 StatSampler" daemon prio=10 tid=0xe828b000 nid=0x2fe5 in Object.wait() [0xe7680000..0xe7680e30]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at com.gemstone.gemfire.internal.HostStatSampler.delay(HostStatSampler.java:239)
	- locked <0xecf6baf0> (a com.gemstone.gemfire.internal.GemFireStatSampler)
	at com.gemstone.gemfire.internal.HostStatSampler.run(HostStatSampler.java:296)
	at java.lang.Thread.run(Thread.java:619)',

'"Thread-8" daemon prio=3 tid=0x00638400 nid=0xd8 in Object.wait() [0xe097f000..0xe097f9f0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at com.gemstone.gemfire.internal.cache.ha.HARegionQueue$QueueRemovalThread.run(HARegionQueue.java:2372)
	- locked <0xec1cae98> (a com.gemstone.gemfire.internal.cache.ha.HARegionQueue$QueueRemovalThread)',

'"TimeScheduler.Thread" daemon prio=10 tid=0x08328800 nid=0x2ba3 in Object.wait() [0xafa23000..0xafa23fb0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb4ee2f58> (a com.gemstone.org.jgroups.util.TimeScheduler$TaskQueue)
	at com.gemstone.org.jgroups.util.TimeScheduler._run(TimeScheduler.java:462)
	- locked <0xb4ee2f58> (a com.gemstone.org.jgroups.util.TimeScheduler$TaskQueue)
	at com.gemstone.org.jgroups.util.TimeScheduler$Loop.run(TimeScheduler.java:146)
	at java.lang.Thread.run(Thread.java:619)',

'"TimeScheduler.Thread" daemon prio=10 tid=0x082e3c00 nid=0x2f60 in Object.wait() [0xe7cd5000..0xe7cd6030]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at com.gemstone.org.jgroups.util.TimeScheduler._run(TimeScheduler.java:462)
	- locked <0xeceb6d58> (a com.gemstone.org.jgroups.util.TimeScheduler$TaskQueue)
	at com.gemstone.org.jgroups.util.TimeScheduler$Loop.run(TimeScheduler.java:146)
	at java.lang.Thread.run(Thread.java:619)',

'"UDP mcast receiver" daemon prio=10 tid=0xb0147c00 nid=0x1418 runnable [0xaf895000..0xaf895eb0]
   java.lang.Thread.State: RUNNABLE
	at java.net.PlainDatagramSocketImpl.receive0(Native Method)
	- locked <0xee0aa130> (a java.net.PlainDatagramSocketImpl)
	at java.net.PlainDatagramSocketImpl.receive(PlainDatagramSocketImpl.java:136)
	- locked <0xee0aa130> (a java.net.PlainDatagramSocketImpl)
	at java.net.DatagramSocket.receive(DatagramSocket.java:712)
	- locked <0xee0e7808> (a java.net.DatagramPacket)
	- locked <0xee0aa0e0> (a java.net.MulticastSocket)
	at com.gemstone.org.jgroups.protocols.UDP.run(UDP.java:278)
	at java.lang.Thread.run(Thread.java:619)',

'"UDP ucast receiver" daemon prio=10 tid=0x08329000 nid=0x2ba5 runnable [0xaf981000..0xaf9820b0]
   java.lang.Thread.State: RUNNABLE
	at java.net.PlainDatagramSocketImpl.receive0(Native Method)
	- locked <0xb4ef7038> (a java.net.PlainDatagramSocketImpl)
	at java.net.PlainDatagramSocketImpl.receive(PlainDatagramSocketImpl.java:136)
	- locked <0xb4ef7038> (a java.net.PlainDatagramSocketImpl)
	at java.net.DatagramSocket.receive(DatagramSocket.java:712)
	- locked <0xb4f7d298> (a java.net.DatagramPacket)
	- locked <0xb4ee47c0> (a java.net.DatagramSocket)
	at com.gemstone.org.jgroups.protocols.UDP$UcastReceiver.run(UDP.java:1197)
	at java.lang.Thread.run(Thread.java:619)',

'"VERIFY_SUSPECT.TimerThread" daemon prio=10 tid=0x08298000 nid=0x7ac2 waiting on condition [0xa0299000..0xa029a020]
   java.lang.Thread.State: TIMED_WAITING (sleeping)
	at java.lang.Thread.sleep(Native Method)
	at com.gemstone.org.jgroups.util.Util.sleep(Util.java:535)
	at com.gemstone.org.jgroups.protocols.VERIFY_SUSPECT.run(VERIFY_SUSPECT.java:265)
	at java.lang.Thread.run(Thread.java:619)',

'"ViewHandler" daemon prio=10 tid=0x082ed400 nid=0xe20 in Object.wait() [0xa085c000..0xa085d020]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb3f43720> (a java.lang.Object)
	at com.gemstone.org.jgroups.util.Queue.remove(Queue.java:321)
	- locked <0xb3f43720> (a java.lang.Object)
	at com.gemstone.org.jgroups.protocols.pbcast.GMS$ViewHandler.run(GMS.java:1655)
	at java.lang.Thread.run(Thread.java:619)',

'"ViewHandler" daemon prio=10 tid=0x084d3c00 nid=0x32b2 in Object.wait() [0xbebfe000..0xbebff1b0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at com.gemstone.org.jgroups.util.Queue.peek(Queue.java:460)
	- locked <0xc4fcc7a0> (a java.lang.Object)
	at com.gemstone.org.jgroups.protocols.pbcast.GMS$ViewHandler.process(GMS.java:1750)
	at com.gemstone.org.jgroups.protocols.pbcast.GMS$ViewHandler.run(GMS.java:1656)
	at java.lang.Thread.run(Thread.java:619)',

'"View Message Processor" daemon prio=10 tid=0x082f2000 nid=0x32c0 in Object.wait() [0xe065c000..0xe065d1b0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe5502ed8> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.LinkedBlockingQueue.poll(LinkedBlockingQueue.java:349)
	- locked <0xe5502ed8> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4.run(DistributionManager.java:780)
	at java.lang.Thread.run(Thread.java:619)',

'"View Message Processor" daemon prio=10 tid=0xbe55c000 nid=0x32c1 in Object.wait() [0xbeb5c000..0xbeb5d130]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.LinkedBlockingQueue.poll(LinkedBlockingQueue.java:349)
	- locked <0xc5145308> (a java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:547)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4.run(DistributionManager.java:780)
	at java.lang.Thread.run(Thread.java:619)',

'"View Message Processor" daemon prio=10 tid=0x083d9000 nid=0x14c7 in Object.wait() [0xaf657000..0xaf658030]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee8852d0> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at java.lang.Object.wait(Object.java:443)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:427)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue.poll(LinkedBlockingQueue.java:350)
	- locked <0xee8852d0> (a com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue$SerializableLock)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at com.gemstone.bp.edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:553)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$2$1.run(DistributionManager.java:787)
	at java.lang.Thread.run(Thread.java:619)',

'"loadEstimateTimeoutProcessor" daemon prio=10 tid=0x080dd800 nid=0x59d7 in Object.wait() [0x9fec6000..0x9fec6e20]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xa551e3b0> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.DelayQueue.take(DelayQueue.java:152)
	- locked <0xa551e3b0> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:680)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"locator request thread[4]" daemon prio=10 tid=0xaf4cdc00 nid=0x6a8b in Object.wait() [0xaec5c000..0xaec5ceb0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xede5aa98> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:358)
	- locked <0xede5aa98> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.take(SynchronousQueue.java:521)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"locator request thread[13]" daemon prio=10 tid=0x080dc400 nid=0x5dbf in Object.wait() [0xa005b000..0xa005bda0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb3348178> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xb3348178> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"locatoragent_bridgeds_pippin_12353" prio=10 tid=0x08058400 nid=0x3044 in Object.wait() [0xf7e02000..0xf7e03298]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xee529390> (a com.gemstone.org.jgroups.stack.tcpserver.TcpServer$2)
	at java.lang.Thread.join(Thread.java:1143)
	- locked <0xee529390> (a com.gemstone.org.jgroups.stack.tcpserver.TcpServer$2)
	at java.lang.Thread.join(Thread.java:1196)
	at com.gemstone.org.jgroups.stack.tcpserver.TcpServer.join(TcpServer.java:154)
	at com.gemstone.gemfire.distributed.internal.InternalLocator.waitToStop(InternalLocator.java:632)
	at hydra.GemFireLocatorAgent.main(GemFireLocatorAgent.java:217)',

'"locatoragent_ds_fscott_11131" prio=10 tid=0x08059000 nid=0x2b7c in Object.wait() [0xf7fcd000..0xf7fce288]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xb4f2e1b8> (a com.gemstone.org.jgroups.stack.tcpserver.TcpServer$2)
	at java.lang.Thread.join(Thread.java:1143)
	- locked <0xb4f2e1b8> (a com.gemstone.org.jgroups.stack.tcpserver.TcpServer$2)
	at java.lang.Thread.join(Thread.java:1196)
	at com.gemstone.org.jgroups.stack.tcpserver.TcpServer.join(TcpServer.java:153)
	at com.gemstone.gemfire.distributed.internal.InternalLocator.waitToStop(InternalLocator.java:621)
	at hydra.GemFireLocatorAgent.main(GemFireLocatorAgent.java:221)',

'"osprocess reaper" daemon prio=10 tid=0x08323c00 nid=0x2b9d waiting on condition [0xafc14000..0xafc150b0]
   java.lang.Thread.State: TIMED_WAITING (sleeping)
	at java.lang.Thread.sleep(Native Method)
	at com.gemstone.gemfire.internal.OSProcess$1.run(OSProcess.java:471)
	at java.lang.Thread.run(Thread.java:619)',

'"poolLoadConditioningMonitor-brclient" daemon prio=10 tid=0x080d7400 nid=0x54d3 in Object.wait() [0xdffa4000..0xdffa4f30]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe55078e0> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.DelayQueue.take(DelayQueue.java:156)
	- locked <0xe55078e0> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:680)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"poolLoadConditioningMonitor-brclient" daemon prio=10 tid=0x081a2400 nid=0xf5d in Object.wait() [0xe0164000..0xe01650b0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.DelayQueue.take(DelayQueue.java:156)
	- locked <0xe552b078> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:680)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"poolLoadConditioningMonitor-edgeDescript" daemon prio=10 tid=0xe0540400 nid=0x9eb in Object.wait() [0xdfe9a000..0xdfe9afb0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe5638198> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.DelayQueue.take(DelayQueue.java:156)
	- locked <0xe5638198> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:689)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"poolTimer-brclient-20" daemon prio=10 tid=0x080c3c00 nid=0x556d in Object.wait() [0xdf950000..0xdf951030]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.DelayQueue.poll(DelayQueue.java:200)
	- locked <0xe55a58c8> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.poll(ScheduledThreadPoolExecutor.java:682)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"poolTimer-edgeDescript-21" daemon prio=10 tid=0x0837cc00 nid=0x629d in Object.wait() [0xe04fe000..0xe04ff050]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xf4633958> (a java.util.concurrent.SynchronousQueue$Node)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.SynchronousQueue$Node.attempt(SynchronousQueue.java:374)
	at java.util.concurrent.SynchronousQueue$Node.waitForPut(SynchronousQueue.java:402)
	- locked <0xf4633958> (a java.util.concurrent.SynchronousQueue$Node)
	at java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:566)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"poolTimer-edgeDescript-1" daemon prio=10 tid=0xe0521000 nid=0x4af6 in Object.wait() [0xdffee000..0xdffeefd0]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe53ac9a0> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.DelayQueue.take(DelayQueue.java:156)
	- locked <0xe53ac9a0> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:689)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"poolTimer-brclient-23" daemon prio=10 tid=0x081ef800 nid=0x71e6 in Object.wait() [0xdf25c000..0xdf25cf30]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe54d9158> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.DelayQueue.poll(DelayQueue.java:200)
	- locked <0xe54d9158> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.poll(ScheduledThreadPoolExecutor.java:682)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"poolTimer-GatewayPool-2-35" daemon prio=10 tid=0x58385c00 nid=0x7b65 in Object.wait() [0x59bf2000..0x59bf2470]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.DelayQueue.poll(DelayQueue.java:200)
	- locked <0x476d7898> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.poll(ScheduledThreadPoolExecutor.java:691)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"queueTimer-brclient" daemon prio=10 tid=0x0826fc00 nid=0x54de in Object.wait() [0xdfdbe000..0xdfdbf030]
   java.lang.Thread.State: TIMED_WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	at java.lang.Object.wait(Object.java:443)
	at java.util.concurrent.TimeUnit.timedWait(TimeUnit.java:364)
	at java.util.concurrent.DelayQueue.take(DelayQueue.java:156)
	- locked <0xe55b53b0> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:680)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled High Priority Message Processor 116" daemon prio=10 tid=0xdf67a800 nid=0x11d1 waiting on condition [0xd9e69000]
   java.lang.Thread.State: TIMED_WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	at java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:317)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:451)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:350)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.poll(SynchronousQueueNoSpin.java:901)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:945)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:576)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$5$1.run(DistributionManager.java:882)
	at java.lang.Thread.run(Thread.java:619)',

'"Pooled Message Processor 233" daemon prio=10 tid=0xdd75bc00 nid=0x9c8 waiting on condition [0xdc20b000]
   java.lang.Thread.State: TIMED_WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	at java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:317)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:451)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:350)
	at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.poll(SynchronousQueueNoSpin.java:901)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:945)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:907)
	at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:576)
	at com.gemstone.gemfire.distributed.internal.DistributionManager$4$1.run(DistributionManager.java:846)
	at java.lang.Thread.run(Thread.java:619)',

'"queueTimer-edgeDescript" daemon prio=10 tid=0x08487400 nid=0x4f74 in Object.wait() [0xdff4c000..0xdff4d1d0]
   java.lang.Thread.State: WAITING (on object monitor)
	at java.lang.Object.wait(Native Method)
	- waiting on <0xe53adf08> (a java.lang.Object)
	at java.lang.Object.wait(Object.java:485)
	at java.util.concurrent.DelayQueue.take(DelayQueue.java:152)
	- locked <0xe53adf08> (a java.lang.Object)
	at java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:689)
	at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:922)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:981)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:529)
	at java.lang.Thread.run(Thread.java:619)',

'"State Logger Consumer Thread" daemon prio=10 tid=0x0848ac00 nid=0x2c0c waiting on condition [0xdf301000]
   java.lang.Thread.State: WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
	- parking to wait for  <0xe4938140> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
	at java.util.concurrent.locks.LockSupport.park(LockSupport.java:158)
	at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1925)
	at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:358)
	at com.gemstone.gemfire.internal.sequencelog.SequenceLoggerImpl$ConsumerThread.run(SequenceLoggerImpl.java:94)',
  ); # com.gemstone patterns

  my ($i);

  if ($PRINT_PATTERNS == 2) {
    # ...to clean up (canonicalize) stacks...
    @KNOWN_PATTERNS = sort(@KNOWN_PATTERNS);
    for ($i = 0; $i <= $#KNOWN_PATTERNS; $i ++) {
      print "'$KNOWN_PATTERNS[$i]',\n\n";
      }
    print "--------------------\n";
  }

  for ($i = 0; $i <= $#KNOWN_PATTERNS; $i ++) {
    $KNOWN_PATTERNS[$i] = &prepPattern($KNOWN_PATTERNS[$i]);
    }

  # Canonicalization could--theoretically--change sort order
  if ($PRINT_PATTERNS == 1) {
    @KNOWN_PATTERNS = sort(@KNOWN_PATTERNS);
    # Check for duplicates... note that we stop with length-1
    for ($i = 0; $i < $#KNOWN_PATTERNS; $i ++) {
# To dump the patterns being used...
      print "'$KNOWN_PATTERNS[$i]',\n\n";

# To test for duplicates...
#      next if $KNOWN_PATTERNS[$i] ne $KNOWN_PATTERNS[$i + 1];
#      print "\nDuplicate pattern:\n$KNOWN_PATTERNS[$i]\n\n";
      }
    print "--------------------\n";
    }

  return if $omit_foreign_stacks;

  # -------------------------------------------------------------
  # Patterns that don't have com.gemstone in them, in case you want to
  # see non-GemStone stacks but still want to elide uninteresting ones...
  my (@more_patterns) =
      ( # non-com.gemstone patterns
  );  # non-com.gemstone patterns
  for ($i = 0; $i <= $#more_patterns; $i ++) {
    push @KNOWN_PATTERNS, &prepPattern($more_patterns[$i]);
    }

} # init_patterns
