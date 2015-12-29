#!/usr/bin/env python
# This is REALLY bad :/

class Utilities:
    def __init__(self, num_results="all"):
        self.num_results = num_results
        self.rows_to_exclude = []
        

    def csv_to_html(self, csvfile, mode):
        data = open(csvfile, "r")
        output = '<table class="table-condensed" style="width:300px"> '

        rownum = 0
        for rows in data:
            rows = rows.strip()
            row = rows.split(",")
            output += "<tr>"
            if self.num_results != "all":
                self._remove_rows(row)
            if rownum == 0:
                output += self._create_rownum(row)
                rownum = 1
            else:
                output += self._process_row(row, mode)
        output += '</table>'
        return output

    @staticmethod
    def _create_rownum(row):
        result  = "<thead>"
        for i in row:
            result += "\t<th>%s</th>" % i
        result += "</thead>\n"
        return result

    def _process_row(self, row, mode):
        output = "\t<tr>"
        convert_to_millis = False
        col_num = 0

        for i in row:
            if col_num == 0 and "0" in i:
                output += '\t<td title="test description"> %s </td>' % i
            elif col_num == 1:
                if "ResponseTime" in i:
                    convert_to_millis = True
                output += "\t <td> %s </td>" % i
            elif col_num < 3:
                output += "\t<td> %s </td>\n" % i
            elif mode == "raw":
                if convert_to_millis:
                    # Try to convert the value to a float.  If not, ignore.
                    try:
                        output += "\t<td>%.2f</td>" % (float(i) / 1000000.0)
                    except:
                        pass
                else:
                    output += "\t<td>%s</td>" % (i)
            else:
                output += "\t<td %s>%s</td>" % (self._get_severity(i), i)
            col_num += 1
        output += "\t</tr>\n"
        return output

    @staticmethod
    def _get_severity(data):
        if "---" in data:
            return ""
        elif "xxx" in data:
            return 'class="row-failed"'
        try:
            # Don't color ratios +/- 7%
            if 1.07 >= float(data) >= -1.07:
                return 'class="row-info"'
            elif 1.15 >= float(data) >= 1.0:
                return 'class="row-mild-gain"'
            elif float(data) >= 1.16:
                return 'class="row-severe-gain"'
            elif -1.08 >= float(data) >= -1.15:
                return 'class="row-mild-degradation"'
            elif -1.05 > float(data) > -1.30: # 15- 30
                return 'class="row-degradation"'
            elif float(data) <= -1.30: # > 30
                return 'class="row-severe-degradation"'
        except:
            pass

    def _remove_rows(self,row):
        '''
        Save the first 4 rows as they contain the test, statspec, ops, and baseline.
        Delete everything between this and the last N runs, where N is retrieved by the user
        '''
        
        index = int(self.num_results) * -1
        row[4:index] = []
        return row


