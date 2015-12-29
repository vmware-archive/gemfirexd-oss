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
package sql.view;

import hydra.BasePrms;
import hydra.HydraVector;

public class ViewPrms extends BasePrms {

 static {
   setValues( ViewPrms.class );
 }

 public static Long viewDDLFilePath;

 public static String getViewDDLFilePath() {
   if (ViewPrms.tasktab().get(ViewPrms.viewDDLFilePath) != null) {
     return ViewPrms.tasktab().stringAt(ViewPrms.viewDDLFilePath);
   } else if (ViewPrms.tab().stringAt(ViewPrms.viewDDLFilePath) != null) {
     return ViewPrms.tab().stringAt(ViewPrms.viewDDLFilePath);
   } else {
     return "sql/view/viewDDL.sql";
   }
 }
 
 public static Long viewDataFilePath;

 public static String getViewDataFilePath() {
   if (ViewPrms.tasktab().get(ViewPrms.viewDataFilePath) != null) {
     return ViewPrms.tasktab().stringAt(ViewPrms.viewDataFilePath);
   } else if (ViewPrms.tab().stringAt(ViewPrms.viewDataFilePath) != null) {
     return ViewPrms.tab().stringAt(ViewPrms.viewDataFilePath);
   } else {
     return "sql/view/viewData.sql";
   }
 }
 
 public static Long queryViewsStatements;
 public static HydraVector getQueryViewsStatements() {
   if (ViewPrms.tasktab().get(ViewPrms.queryViewsStatements) != null) {
     return ViewPrms.tasktab().vecAt(ViewPrms.queryViewsStatements);
   } else {
     if (ViewPrms.tab().get(ViewPrms.queryViewsStatements) != null) {
       return ViewPrms.tab().vecAt(ViewPrms.queryViewsStatements);
     } else {
       return new HydraVector();
     }
   }
 }
}
