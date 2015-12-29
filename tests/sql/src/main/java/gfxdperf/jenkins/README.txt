================================================================================
https://perf-jenkins.gemstone.com/
--------------------------------------------------------------------------------
Log in using your vmware username and password.

STATUS:
The status sphere is solid when nothing is running and blinking when a build/run
is underway. The status bar shows progress on the active run.

FAILURES:
Solid red means the last build/run failed. Hover over # under the last failure,
and pick Console from the dropdown to see the cause of the failure
For a build failure, look at the buildLinux.log in /var/lib/jenkins/checkouts.
For test failures, look at the run in /export/perf-jenkins/user/jenkins/runs.
Jenkins is configured to rebuild every 30 minutes after a failure until a run
gets underway. TBD add the path to the failed run in the text.

CANCELLING RUNS:
Use the little x by the status bar to stop the current run. Then go kill the
actual hydra run and clean up with nuke*.sh and move*.sh. Jenkins will start
another run at the scheduled time.

SCHEDULING RUNS:
Runs are currently scheduled every 4 hours. The little clock can be used to add
another run to the queue. This will cause a run to proceed as soon as the current
one completes. After that, it goes back to running every 4 hours

PAUSING:
To pause, click on GemFireXD (not the dropdown). Hit Disable project to stop
the cron entry. The current run will complete first.

CONFIGURATION:
Pick Configure from the GemFireXD dropdown. This shows the location of the
script used by Jenkins to build/run, when to do it, and who to notify.
TBD Change this to notify rtds-dev when things are stable.

RESTART:
If the Jenkins Java process is killed (so the web site is down and no runs are
scheduled), restart it as user jenkins or lises:

        sudo /etc/init.d/jenkins restart

REPORTING:
The csv reports are generated at the end of each run, using the default baseline
and all runs in /export/perf-jenkins/user/jenkins/runs. The web page report is
rendered on the fly. If you see old values right after a run, wait for the web
cache to clear (this takes at most 2 minutes).
TBD: Add a page for the last 10 runs for raw numbers, just like the ratios.

ADMINISTRATION:
To modify, move, or delete files related to Jenkins runs, log in as jenkins.

        ssh -l jenkins perf-jenkins
        user: jenkins
        password: querty-obvious (numbers first)

================================================================================
/var/lib/jenkins
--------------------------------------------------------------------------------
This is the home directory of user jenkins on host perf-jenkins.

Key subdirectories and files:

    checkouts/
        This directory must exist.

    scripts/gfxd_12_hour.py
        Manages the svn update, build, run, failure detection, email
        notification, and generates performance report text and csv.

Directory contents (top level only):

    checkouts/                                       jenkins.model.JenkinsLocationConfiguration.xml
    config.xml                                       jenkins.mvn.GlobalMavenConfig.xml
    Connection Activity monitoring to slaves.log     jenkins.security.QueueItemAuthenticatorConfiguration.xml
    Fingerprint cleanup.log                          jobs/
    gfxd_12_hour.py                                  nodeMonitors.xml
    hudson.maven.MavenModuleSet.xml                  Out of order build detection.log
    hudson.model.UpdateCenter.xml                    plugins/
    hudson.scm.CVSSCM.xml                            queue.xml.bak
    hudson.scm.SubversionSCM.xml                     scripts/
    hudson.tasks.Ant.xml                             secret.key
    hudson.tasks.Mailer.xml                          secret.key.not-so-secret
    hudson.tasks.Maven.xml                           secrets/
    hudson.tasks.Shell.xml                           updates/
    hudson.triggers.SCMTrigger.xml                   userContent
    identity.key                                     users/
    jenkins.diagnostics.ooom.OutOfOrderBuildMonitor  Workspace clean-up.log
    jenkins.model.ArtifactManagerConfiguration.xml

================================================================================
/export/perf-jenkins/users/jenkins
--------------------------------------------------------------------------------

Key subdirectories and files:

    bts/
        The batterytest and local.conf file used for the runs.

    build/gfxd/
       The latest build. See buildLinux.log to diagnose build failures.

    reports/
        The ratio and raw csv files and the full report. The "current" ones
        are links to the most recent reports.

    runs/
        These hold all runs not archived yet. Use df periodically to check for
        the disk filling up. Archive results as needed.

            /dev/mapper/vg_perfjenkins-LogVol02
                 76029016   4598396  67568504   7% /srv

    runs/default_baseline@
        This is the baseline to use in the reports. To change it:
            ln -s <desired_run> default_baseline

    www/perf-jenkins/Utilities.py
        This is where the severity percentages are set for the web report.

    www/perf-jenkins/assets/css/custom.css
        This is where the severity colors are set. Values are in hex. See
        online for help converting colors to hex.

Directory contents (top level only plus all of www):

    bts:
        continuous.bt@  local.conf@
    builds:
        gfxd/
    reports:
        ...
        04-28-2014-09-06-49_ratio.csv   current_ratio.csv@
        04-28-2014-09-06-49_raw.csv     current_raw.csv@
        04-28-2014-09-06-49.report      current.report@
    runs:
        ...
        04-22-2014-05-05-59/  04-25-2014-01-07-00/  04-28-2014-13-06-42/
        04-22-2014-09-05-52/  04-25-2014-05-06-48/  default-baseline@
        04-22-2014-11-50-37/  04-25-2014-09-06-50/
    www:
        cache/  perf-jenkins/
    www/cache:
    www/perf-jenkins:
        assets/  ReportFile.py  ReportFile.pyc  templates/  Utilities.py  Utilities.pyc  web.py*
    www/perf-jenkins/assets:
        css/
    www/perf-jenkins/assets/css:
        bootstrap.css  custom.css
    www/perf-jenkins/templates:
        base.html

================================================================================
/export/perf/users/jenkins
--------------------------------------------------------------------------------
This is where old runs are archived. These are not included in the web reports.
These tests should be cleaned up when space becomes an issue.
