package org.apache.ddlutils.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Contains some utility functions for working with the model classes.
 * 
 * @version $Revision: $
 */
public class ModelHelper
{
    /**
     * Determines whether one of the tables in the list has a foreign key to a table outside of the list,
     * or a table outside of the list has a foreign key to one of the tables in the list.
     * 
     * @param model  The database model
     * @param tables The tables
     * @throws ModelException If such a foreign key exists
     */
    public void checkForForeignKeysToAndFromTables(Database model, Table[] tables) throws ModelException
    {
        List tableList = Arrays.asList(tables);

        for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++)
        {
            Table   curTable         = model.getTable(tableIdx);
            boolean curTableIsInList = tableList.contains(curTable);

            for (int fkIdx = 0; fkIdx < curTable.getForeignKeyCount(); fkIdx++)
            {
                ForeignKey curFk = curTable.getForeignKey(fkIdx);

                if (curTableIsInList != tableList.contains(curFk.getForeignTable()))
                {
                    throw new ModelException("The table " + curTable.getQualifiedName() + " has a foreign key to table " + curFk.getForeignTable().getQualifiedName());
                }
            }
        }
    }

    /**
     * Removes all foreign keys from the tables in the list to tables outside of the list,
     * or from tables outside of the list to tables in the list.
     * 
     * @param model  The database model
     * @param tables The tables
     */
    public void removeForeignKeysToAndFromTables(Database model, Table[] tables)
    {
        List tableList = Arrays.asList(tables);

        for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++)
        {
            Table     curTable         = model.getTable(tableIdx);
            boolean   curTableIsInList = tableList.contains(curTable);
            ArrayList fksToRemove      = new ArrayList();

            for (int fkIdx = 0; fkIdx < curTable.getForeignKeyCount(); fkIdx++)
            {
                ForeignKey curFk = curTable.getForeignKey(fkIdx);

                if (curTableIsInList != tableList.contains(curFk.getForeignTable()))
                {
                    fksToRemove.add(curFk);
                }
                for (Iterator fkIt = fksToRemove.iterator(); fkIt.hasNext();)
                {
                    curTable.removeForeignKey((ForeignKey)fkIt.next());
                }
            }
        }
    }
// GemStone changes BEGIN

    public static void getDelimitedName(String name, StringBuilder query,
        String delimiterToken) {
      int dotIndex = name.indexOf('.');
      if (dotIndex == -1) {
        query.append(delimiterToken).append(name).append(delimiterToken);
      }
      else {
        int startIndex;
        query.append(delimiterToken).append(name.substring(0, dotIndex));
        for (;;) {
          startIndex = dotIndex + 1;
          dotIndex = name.indexOf('.', startIndex);
          query.append(delimiterToken).append('.').append(delimiterToken);
          if (dotIndex != -1) {
            query.append(name.substring(startIndex, dotIndex));
          }
          else {
            query.append(name.substring(startIndex)).append(delimiterToken);
            break;
          }
        }
      }
    }
// GemStone changes END
}
