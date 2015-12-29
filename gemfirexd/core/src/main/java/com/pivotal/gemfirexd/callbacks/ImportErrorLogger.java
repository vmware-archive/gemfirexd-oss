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
/**
 * 
 */
package com.pivotal.gemfirexd.callbacks;

import javax.xml.stream.XMLStreamException;

import static com.pivotal.gemfirexd.internal.impl.load.ImportBase.MINUS_ONE;

/**
 * @author vivekb
 *
 */
public class ImportErrorLogger extends EventErrorLogger {
  /*
   * For use of no relation to this class
   */
  public boolean isRetry = false;
  public char[][] currentRows = null;
  public int currentRowIndex = MINUS_ONE;

  /*
   * For use of ImportErrorLogger
   */
  final String tableName;
  final String importFile;
  
  // Error file XML tag/attribute names
  protected static final String ERR_XML_IFILE = "importfile";
  protected static final String ERR_XML_LINENUM = "linenumber";
  protected static final String ERR_XML_LINE = "linewitherror";
  protected static final String ERR_XML_TABLE = "tablename";
  
  public ImportErrorLogger(String errorFileName, String importFileName,
      String tableName) {
    super(errorFileName);
    this.tableName = tableName;
    this.importFile = importFileName;
  }

  private void writeProp(String startElement, String value, int indent)
      throws XMLStreamException {
    indentInErrorXML(indent);
    if (value != null) {
      this.errorWriter.writeStartElement(startElement);
      this.errorWriter.writeCharacters(value);
      this.errorWriter.writeEndElement();
    }
    else {
      this.errorWriter.writeStartElement(ERR_XML_SQL);
      this.errorWriter.writeAttribute(ERR_XML_ATTR_NULL, "true");
      this.errorWriter.writeEndElement();
    }
  }
  
  public void logError(String line, Integer lineNumber, String dmlString,
      Exception e) throws Exception {
    // initialize the error file
    initErrorFile();

    final int indentStep = 2;
    int indent = indentStep;
    this.errorWriter.writeStartElement(ERR_XML_FAILURE);    
    writeProp (ERR_XML_SQL, dmlString, indent);
    writeProp (ERR_XML_TABLE, tableName, indent);
    writeProp (ERR_XML_IFILE, importFile, indent);
    writeProp (ERR_XML_LINENUM, Integer.toString(lineNumber), indent);
    indent += indentStep;
    writeProp (ERR_XML_LINE, line, indent);
    indent -= indentStep;
    this.errorWriter.writeCharacters("\n");

    // write exception details at the end
    writeExceptionInErrorXML(e, indent, indentStep, true);

    this.errorWriter.writeCharacters("\n");
    this.errorWriter.writeEndElement();
    this.errorWriter.writeCharacters("\n");
    this.errorWriter.flush();
    this.errorStream.flush();
  }

}
