#!/usr/bin/env python
# Sc

import datetime
import os
import sys
import time
import commands
import smtplib
import shutil

should_build = True

class BuildProduct:
    """
    build the prodct.
    """
    def __init__(self):
        self.checkout_dir = '/var/lib/jenkins/checkouts/gemfirexd_rebrand_Dec13'
        self.build_full_command  = './build.sh build-product compile-tests gfxd-build-product gfxd-compile-all-tests'
        self.build_clean_command = './build.sh gfxd-clean clean'

    def _get_svn_info(self):
        os.chdir(self.checkout_dir)
        return commands.getstatusoutput("svn info %s" % checkout_dir)[1]

    def clean(self):
        os.chdir(self.checkout_dir)
        return commands.getstatusoutput(self.build_clean_command)

    def svn_update(self):
        os.chdir(self.checkout_dir)
        return commands.getstatusoutput("svn up")

    
    def build_product(self):
        os.chdir(self.checkout_dir)
        return commands.getstatusoutput(self.build_full_command)

    def do_all_steps(self):
        """
        Returns a tuple with the exit status and the results of the build command.
        """
        self.clean()
        self.svn_update()
        return self.build_product()

    def send_email(self,failure_message):
        """
        Sends email in the event of an error. 
        """
        SERVER = "mail.gemstone.com"
        FROM = "perf-jenkins@gemstone.com"
        TO = ["ablakeman@pivotal.io", "lstorc@pivotal.io"] 
        SUBJECT = "[PERF JENKINS] Build failed"
        TEXT  = "Build on perf-jenkins failed! \n"
        TEXT += "Build info:\n %s\n\n" % self._get_svn_info()
        TEXT += failure_message
        message = """\
From: %s
To: %s
Subject: %s

%s
        """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
        # Send the mail
        server = smtplib.SMTP(SERVER)
        server.sendmail(FROM, TO, message)
        server.quit()

class PerfTests:
    """
    Runn the bt for the 12 hour regression.
    """
    def __init__(self, name):
        self.bt_name      = name
        self.timestamp    = datetime.datetime.fromtimestamp(time.time()).strftime('%m-%d-%Y-%H-%M-%S')
        self.base_dir     = "/export/perf-jenkins/users/jenkins/"
        self.bt_dir       = "%s/bts/" % self.base_dir
        self.base_run_dir = "%s/runs" % self.base_dir
        self.run_dir      = "%s/%s" % (self.base_run_dir, self.timestamp)
        self.reports_dir  = "%s/reports/" % self.base_dir
        self.default_baseline = "%s/default-baseline" % self.base_run_dir
        self.java_home    = '/export/gcm/where/jdk/1.7.0_05/x86_64.linux'
        self._setenv()
        self._make_run_dir()
        self._copy_local_conf()

    def _copy_local_conf(self):
        shutil.copyfile("%s/local.conf" % self.bt_dir, "%s/local.conf" % self.run_dir)

    def _make_run_dir(self):
        try:
            os.mkdir(self.run_dir)
        except:
            print "Unable to create run directory %s" % self.run_dir
            system.exit(1)

    def _setenv(self):
        os.environ["GFXD"]    = "/export/perf-jenkins/users/jenkins/builds/gfxd/product-gfxd"        
        os.environ["GEMFIRE"] = os.environ["GFXD"] + "/../product/"
        os.environ["JTESTS"]  = os.environ["GFXD"] + "/../tests/classes/"
        os.environ["EXTRA_JTESTS"] = os.environ["HOME"] # not sure...

    def run(self):
        os.chdir(self.run_dir)
        command  = "%s/bin/java -server -classpath " % (self.java_home)
        command += "%s/../tests/classes:%s/../product-gfxd/lib/gemfirexd.jar " % (os.environ["GFXD"], os.environ["GFXD"])
        command += "-DGEMFIRE=%s " % os.environ["GEMFIRE"]
        command += "-DEXTRA_JTESTS=%s " % os.environ["EXTRA_JTESTS"]
        command += "-DJTESTS=%s " % os.environ["JTESTS"]
        command += "-DprovidePropertiesForJenkins=true "
        command += "-Dlocal.conf=%s/local.conf " % self.bt_dir
        command += "-DtestFileName=%s/%s " % (self.bt_dir, self.bt_name)
        command += "-DnumTimesToRun=1 -DprovideRegressionSummary=false -DnukeHungTest=true -DmoveRemoteDirs=true batterytest.BatteryTest"
        return commands.getstatusoutput(command)

    def did_all_tests_pass(self):
        bt_log = "%s/batterytest.log" % self.run_dir 
        self.battery_test_log = commands.getstatusoutput(' grep "STATUS REPORT" %s' % bt_log)[1]
        results = self.battery_test_log.split(" ")
        test_status = []

        next_item = 0
        for entry in results:
            next_item += 1
            if entry == "Failed:":
                test_status.append(results[next_item])
            elif entry == "Hung:":
                test_status.append(results[next_item])
            elif entry == "Remaining:":
                test_status.append(results[next_item])

        # If any tests failed, tell Jenkins that the build failed.
        for item in test_status:
            if int(item) > 0:
                return False

        return True

    def get_bt_log(self):
        return self.battery_test_log

    def compare_to_baseline(self, mode="ratio"):
        command  = 'java -cp $GEMFIRE/lib/gemfire.jar:$JTESTS -Xmx1024m '
        command += '-Dmode=%s -DcompareByKey=true ' % mode
        command += '-DlogLevel=none '
        command += '-DmarkFailedTests=true '
        command += '-DJTESTS=$JTESTS -Dgemfire.home=$GEMFIRE -DaddTestKey=true '
        command += '-DcompReportFile=%s/%s.report ' % (self.reports_dir,self.timestamp)
        command += '-DgenerateCSVFile=true -DcsvFile=%s/%s_%s.csv ' % (self.reports_dir, self.timestamp, mode)
        command += 'perffmwk.PerfComparer %s %s/??-??-* ' % (self.default_baseline, self.base_run_dir)
        return commands.getstatusoutput(command)

    def update_current_report(self, mode):
        csv_name = '%s/%s_%s.csv ' % (self.reports_dir, self.timestamp, mode)
        command  = 'ln -sf %s %s/current_%s.csv' % (csv_name, self.reports_dir, mode)
        return commands.getstatusoutput(command)

    def update_key(self):
        # 04-25-2014-13-06-45.report
        report_name = '%s/%s.report ' % (self.reports_dir, self.timestamp)
        command  = 'ln -sf %s %s/current.report' % (report_name, self.reports_dir)
        return commands.getstatusoutput(command)
        
if __name__ == '__main__':

    return_code = 0
    if should_build:
        bp = BuildProduct()
        build_results = bp.do_all_steps()

        # If the build fails, send an email and try again in 30 minutes.
        while build_results[0] != 0:
            bp.send_email(build_results[1]) 
            time.sleep(30 * 60)
            build_results = bp.do_all_steps()

    tests = PerfTests("continuous.bt")
    tests.run()


    # Create the .csv reports for both raw and ratio reports.
    # Set unclean exit status if this fails as these reports are pretty important
    for i in "raw", "ratio":
        comp_status, comp_results = tests.compare_to_baseline(mode=i)
        if comp_status > 0:
            return_code = 1
            print "Error creating %s performance report" % i
        else:
            tests.update_current_report(i)

    # Update the key for the latest run
    tests.update_key()

    # print status and exit uncleanly if *any* tests failed.
    if not tests.did_all_tests_pass():
        print tests.get_bt_log()
        return_code = 1

    sys.exit(return_code)
