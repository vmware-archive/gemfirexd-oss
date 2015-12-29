package org.apache.ddlutils.io;

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

import java.util.Iterator;
import java.util.List;
import org.apache.ddlutils.model.Table;

/**
 * Base interface for different strategies to write the XML for a data bean for a specific table.
 */
public class TableXmlWriter extends ModelXmlWriter
{
    private static final int AS_TAG_NAME  = 0;
    private static final int AS_ATTRIBUTE = 1;
    private static final int AS_SUB_TAG   = 2;

    private final String tableName;
    private final int formattingMethod;
    private final boolean base64Encoded;

    public TableXmlWriter(Table table)
    {
// GemStone changes BEGIN
        // use fully qualified name
        // changes below replace table.getName() with fullTableName
        final String fullTableName = table.getQualifiedName();
// GemStone changes END
        if (XMLUtils.hasIllegalXMLCharacters(fullTableName))
        {
            tableName        = XMLUtils.base64Encode(fullTableName);
            formattingMethod = AS_SUB_TAG;
            base64Encoded    = true;
        }
        else
        {
            tableName     = fullTableName;
            base64Encoded = false;
            if (tableName.length() > XMLUtils.MAX_NAME_LENGTH)
            {
                formattingMethod = AS_SUB_TAG;
            }
            else if ("table".equals(tableName) || !XMLUtils.isWellFormedXMLName(tableName))
            {
                formattingMethod = AS_ATTRIBUTE;
            }
            else
            {
                formattingMethod = AS_TAG_NAME;
            }
        }
    }

    /**
     * Write the table data to XML to the given writer.
     * 
     * @param columnXmlWriters A list of column xml writers for writing out the bean's values to XML
     * @param writer           The writer to write to
     */
    public void write(List columnXmlWriters, DataWriter writer)
    {
        writer.indentIfPrettyPrinting(1);
        if (formattingMethod == AS_TAG_NAME)
        {
            writer.writeElementStart(null, tableName);
        }
        else
        {
            writer.writeElementStart(null, "table");
        }
        if (formattingMethod == AS_ATTRIBUTE)
        {
            writer.writeAttribute(null, "table-name", tableName);
        }
        for (Iterator it = columnXmlWriters.iterator(); it.hasNext();)
        {
            ((ColumnXmlWriter)it.next()).writeAttribute(writer);
        }

        boolean hasSubTags = false;

        if (formattingMethod == AS_SUB_TAG)
        {
            writer.printlnIfPrettyPrinting();
            writer.indentIfPrettyPrinting(2);
            writer.writeElementStart(null, "table-name");
            writeText(writer, tableName, base64Encoded);
            writer.writeElementEnd();
            hasSubTags = true;
        }
        for (Iterator it = columnXmlWriters.iterator(); it.hasNext();)
        {
            hasSubTags = ((ColumnXmlWriter)it.next()).writeSubElement(writer) || hasSubTags;
        }
        if (hasSubTags)
        {
            writer.printlnIfPrettyPrinting();
            writer.indentIfPrettyPrinting(1);
        }
        writer.writeElementEnd();
        writer.printlnIfPrettyPrinting();
    }
}
