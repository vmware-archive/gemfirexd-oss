#! /bin/perl -w
# run this in your dunit test directory (where dunit-progress.txt sits)
# to see which tests are taking the longest
#
# parses dunit-progress.txt and sorts it to
# show which tests are taking the longest
# ---------------------------------------------------------------------

&process_file("dunit-progress.txt");

sub process_file {
  my($in) = @_;
  my(@timeslist);

  open(IN, "<$in") || die "$in: $!";
  for (;;) {
    &get_line();
    last unless defined($current_line);
    next unless &is_end_line();
    my(@parts) = split "\ ",$current_line;
     next unless $#parts == 9;
     # pull out the millisecond time-to-run
     my(@time) = split "ms", $parts[9];
     # pick apart the test name and class so we can rearrange them
     my(@testparts) = split "[()]",$parts[7];
     # xxx  package.classname.testname
     my($line) = "$time[0]\t$testparts[1]\.$testparts[0]";
     push @timeslist, $line;
  }
  @timeslist = sort {
    my(@partsA) = split "\t",$a;
    my(@partsB) = split "\t",$b;
    my($timeA) = $partsA[0];
    my($timeB) = $partsB[0];
    if ($timeA > $timeB) { # reverse sort - highest first
      return -1;
    }
    if ($timeA == $timeB) {
      return 0;
    }
    return 1;
    } @timeslist;
  my($line);
  foreach $line (@timeslist) {
    print "$line\n";
  }
}

sub get_line() {
  $current_line = <IN>;
  return $current_line;
}

sub is_end_line() {
  return $current_line =~ ".*\] END test.*";
}

