#!/usr/bin/env python


class ReportFile:
    def __init__(self, report_file):
        self.report_file = report_file

    def get_keys(self):
        fh = open(self.report_file, "r")
        output = ""
        should_print = False
        first_line = True

        for line in fh:
            if "Test Key" in line:
                should_print = True
            elif should_print == False:
                continue

            if first_line:
                output += "<h2>%s</h2>" % line
                first_line = False
            else:
                output += "%s<br>" % line
            
        return output
        
