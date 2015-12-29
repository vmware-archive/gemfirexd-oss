/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package sql.ciIndex;

import hydra.BasePrms;
import hydra.HydraVector;

public class CaseInsensitiveIndexPrms extends BasePrms {

    static {
        setValues(CaseInsensitiveIndexPrms.class);
    }

    public static Long ddlFilePath;

    public static String getCiIndexDDLFilePath() {
        if (CaseInsensitiveIndexPrms.tasktab().get(
                CaseInsensitiveIndexPrms.ddlFilePath) != null) {
            return CaseInsensitiveIndexPrms.tasktab().stringAt(
                    CaseInsensitiveIndexPrms.ddlFilePath);
        } else if (CaseInsensitiveIndexPrms.tab().stringAt(
                CaseInsensitiveIndexPrms.ddlFilePath) != null) {
            return CaseInsensitiveIndexPrms.tab().stringAt(
                    CaseInsensitiveIndexPrms.ddlFilePath);
        } else {
            return "sql/ciIndex/ciIndexDDL.sql";
        }
    }

    public static Long ciIndexDataFiles;

    public static HydraVector getCiIndexDataFiles() {
        if (CaseInsensitiveIndexPrms.tasktab().get(
                CaseInsensitiveIndexPrms.ciIndexDataFiles) != null) {
            return CaseInsensitiveIndexPrms.tasktab().vecAt(
                    CaseInsensitiveIndexPrms.ciIndexDataFiles);
        } else {
            if (CaseInsensitiveIndexPrms.tab().get(
                    CaseInsensitiveIndexPrms.ciIndexDataFiles) != null) {
                return CaseInsensitiveIndexPrms.tab().vecAt(
                        CaseInsensitiveIndexPrms.ciIndexDataFiles);
            } else {
                return new HydraVector();
            }
        }
    }

    public static Long queryStatements;
    public static HydraVector getQueryStatements() {
        if (CaseInsensitiveIndexPrms.tasktab().get(
                CaseInsensitiveIndexPrms.queryStatements) != null) {
            return CaseInsensitiveIndexPrms.tasktab().vecAt(
                    CaseInsensitiveIndexPrms.queryStatements);
        } else {
            if (CaseInsensitiveIndexPrms.tab().get(
                    CaseInsensitiveIndexPrms.queryStatements) != null) {
                return CaseInsensitiveIndexPrms.tab().vecAt(
                        CaseInsensitiveIndexPrms.queryStatements);
            } else {
                return new HydraVector();
            }
        }
    }

    public static Long ciQueryStatements;
    public static HydraVector getCiQueryStatements() {
        if (CaseInsensitiveIndexPrms.tasktab().get(
                CaseInsensitiveIndexPrms.ciQueryStatements) != null) {
            return CaseInsensitiveIndexPrms.tasktab().vecAt(
                    CaseInsensitiveIndexPrms.ciQueryStatements);
        } else {
            if (CaseInsensitiveIndexPrms.tab().get(
                    CaseInsensitiveIndexPrms.ciQueryStatements) != null) {
                return CaseInsensitiveIndexPrms.tab().vecAt(
                        CaseInsensitiveIndexPrms.ciQueryStatements);
            } else {
                return new HydraVector();
            }
        }
    }

    public static Long resultComparators;
    public static HydraVector getResultComparators() {
        if (CaseInsensitiveIndexPrms.tasktab().get(
                CaseInsensitiveIndexPrms.resultComparators) != null) {
            return CaseInsensitiveIndexPrms.tasktab().vecAt(
                    CaseInsensitiveIndexPrms.resultComparators);
        } else {
            if (CaseInsensitiveIndexPrms.tab().get(
                    CaseInsensitiveIndexPrms.resultComparators) != null) {
                return CaseInsensitiveIndexPrms.tab().vecAt(
                        CaseInsensitiveIndexPrms.resultComparators);
            } else {
                return new HydraVector();
            }
        }
    }

    public static Long queryHints;
    public static HydraVector getQueryHints() {
        if (CaseInsensitiveIndexPrms.tasktab().get(
                CaseInsensitiveIndexPrms.queryHints) != null) {
            return CaseInsensitiveIndexPrms.tasktab().vecAt(
                    CaseInsensitiveIndexPrms.queryHints);
        } else {
            if (CaseInsensitiveIndexPrms.tab().get(
                    CaseInsensitiveIndexPrms.queryHints) != null) {
                return CaseInsensitiveIndexPrms.tab().vecAt(
                        CaseInsensitiveIndexPrms.queryHints);
            } else {
                return new HydraVector();
            }
        }
    }
}
