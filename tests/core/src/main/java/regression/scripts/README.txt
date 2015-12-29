A directory for QA to use to share scripts used during regression.

This group of scripts (checkhosts, checkdisk, regrps, checktask) can be used to monitor an active UNIX regression.  

getsummary is used to create the Summary spreadsheet data in a space delimited format for import into Excel.   

Scripts require a regrhosts file (a list of hosts that the regression is running on) and a regrdirs file (a list of the base directories where the output is lcocated).  You will need to create your own ~/bin/regrhosts and ~/bin/regrdirs files to list your regression hosts and home directories.

You may want to copy these scripts to your home directory (so you can fine tune them for your own use).

Sample regrhosts and regrdirs files:
*** regrhosts ***
odin 
thor
stut
bobo
monster
merry
hs20d
hs20e
hs20g
pippin
biscuit

*** regrdirs ***
/export/odin1/users/gfqa
/export/thor1/users/gfqa
/export/stut1/users/gfqa
/export/monster2/users/gfqa
/export/merry2/users/gfqa
/export/pippin2/users/gfqa
/export/hs20d2/users/lhughes
/export/hs20e2/users/lhughes
/export/hs20g2/users/lhughes

Scripts:
- checkdisk - for each directory in regrdirs, du -s .
- checkhosts - displays uptime for each host listed in regrhosts
- checktask - tails batterytest.log for the latest directory in regrdirs
- regrps - ps -Hu `whoami` for each host in regrhosts
- getSummary <svn revision number> - creates a r<svn revision number>Summary.txt file in the local directory from output in regrdirs.  For example 'getsummary 29279' creates a r29279Summaryhdr.txt and r29279Summary.txt file in the local directory.

This summary file can be imported into Excel (for the summary spreadsheet).
In Excel, open your Summary spreadsheet to a clean worksheet and put the cursor into cell A$4.  Then select Data > Import External Data and follow the prompts to import this "space delimited" file into your spreadsheet.  The SummaryHdr file provides additional information on build, branch, revision number, O/S and JRE.

