#! /bin/perl -w
# Compare the output of running hydra.TestConfigComparison
# on exactly two directories.
#
# Arguments:
# 0 - the output of said java run, contains exactly five lines.
#
# Output:
# A concise analysis of the differences.
#
# TODO:
# I guess we could try to recognize non-significant test
# differences and omit them.

open(IN, "<$ARGV[0]") || die "Cannot read input";

my(@ignore) = (
  "hydra.Prms-roundRobin",
  "hydra.Prms-useNFS",
  "hydra.Prms-doStartTasksInSeparateVMs",
  "hydra.log.LogPrms-gemfire_dumpOnError",
  # hydra.log.LogPrms-gemfire_logLevel could actually affect performance <grin>
  "hydra.log.LogPrms-gemfire_logging",
  "hydra.log.LogPrms-gemfire_maxKBPerLog",
  "hydra.Prms-testDescription",
  );

my($name1);
$name1 = <IN>;
chomp($name1);

my($vals1);
$vals1 = <IN>;
chomp($vals1);

<IN>;

my($name2);
$name2 = <IN>;
chomp($name2);
my($vals2);
$vals2 = <IN>;
chomp($vals1);

close IN;


$vals1 =~ s/^\[(.*);\]$/$1/;
$vals2 =~ s/^\[(.*);\]$/$1/;

my(@props1) = split(/;, /, $vals1);
my(@props2) = split(/;, /, $vals2);

@props1 = sort @props1;
@props2 = sort @props2;

my($each, %hash1, %hash2);
foreach $each (@props1) {
  if ($each !~ /^([^=]+)=(.+)$/) {
    print STDERR "Funky property line $each\n";
    next;
    }
  $hash1{$1} = $2;
  }

print "\n";
foreach $each (@props2) {
  if ($each !~ /^([^=]+)=(.+)$/) {
    print "Funky property line $each\n";
    next;
    }
  $hash2{$1} = $2;
  }

foreach $each (@ignore) {
  delete $hash1{$each};
  delete $hash2{$each};
  }

foreach $each (sort(keys(%hash1))) {
  if (!defined($hash2{$each})) {
    print "Only in $name1 $each = $hash1{$each}\n";
    delete $hash1{$each};
    }
  }
foreach $each (sort(keys(%hash2))) {
  if (!defined($hash1{$each})) {
    print "Only in $name2 $each = $hash2{$each}\n";
    delete $hash2{$each};
    }
  }

foreach $each (sort(keys(%hash1))) {
  my($v1, $v2);
  $v1 = $hash1{$each};
  $v2 = $hash2{$each};
  next if $v1 eq $v2;
  print "$each:\n  $name1 -> $v1\n  $name2 -> $v2\n";
  }


exit 0;
