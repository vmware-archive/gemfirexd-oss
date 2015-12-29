<!--
//=========================================================================
// Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License. You
// may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See accompanying
// LICENSE file.
//=========================================================================
-->
<!DOCTYPE stylesheet [
<!ENTITY space "<xsl:text> </xsl:text>">
<!ENTITY cr "<xsl:text>
</xsl:text>">
]>

<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  version="1.0">

  <xsl:output method="xml" indent="yes"
    doctype-public="-//W3C//DTD HTML 4.01//EN"
    doctype-system="http://www.w3.org/TR/html4/strict.dtd"/>

  <xsl:output method="text" indent="yes"/>
  
  <xsl:template name="spaces">
    <xsl:param name="howMany">1</xsl:param>
    <xsl:param name="pText">1</xsl:param>
    <xsl:if test="$howMany &gt; 0">

      <xsl:choose>
	      <xsl:when test="starts-with($pText,'hyphen')" >
	          <xsl:text>-</xsl:text>
	      </xsl:when>
        <xsl:otherwise>
            &space;
        </xsl:otherwise>
      </xsl:choose>

      <xsl:call-template name="spaces">
        <xsl:with-param name="howMany" select="$howMany - 1" />
        <xsl:with-param name="pText" select="$pText" />
      </xsl:call-template>
    </xsl:if>
  </xsl:template>

  <xsl:template name="lineageToSpace">
    <xsl:param name="pText">1</xsl:param>
    <xsl:param name="rank">1</xsl:param>
    
    <xsl:if test="string-length($pText) > 0">
    
      <xsl:variable name="indent">
      <xsl:choose>
	      <xsl:when test="string-length(substring-before($pText, '/')) > 3">
	        <xsl:value-of select="3"/>
	      </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="string-length(substring-before($pText, '/'))"/>
        </xsl:otherwise>
      </xsl:choose>
      </xsl:variable>
      
      <xsl:choose>
      
      <xsl:when test="string-length(substring-after($pText, '/')) > 0">
        <xsl:text>|</xsl:text>
	      <xsl:call-template  name="spaces">
	        <xsl:with-param name="howMany" select="$indent"/>
	        <xsl:with-param name="pText" select='"space"' />
	      </xsl:call-template>
	    </xsl:when>

      <xsl:otherwise>
        <xsl:text>|</xsl:text>
        
	      <xsl:variable name="rankV">
	        <xsl:if test="number($rank) &lt;= 5">
	          <xsl:value-of select="concat('(',number($rank),')')"/>
	        </xsl:if>
        </xsl:variable>
	        
        <xsl:value-of select="$rankV"/>
        
        <xsl:call-template  name="spaces">
          <xsl:with-param name="howMany" select="$indent - string-length($rankV)"/>
          <xsl:with-param name="pText" select='"hyphen"'/>
        </xsl:call-template>
        
      </xsl:otherwise>
      
      </xsl:choose>
      
      <xsl:call-template name="lineageToSpace">
        <xsl:with-param name="pText" select="substring-after($pText, '/')"/>
        <xsl:with-param name="rank" select="$rank"/>
      </xsl:call-template>
    </xsl:if>

  </xsl:template>

  
  <xsl:template match="/">
        <xsl:apply-templates select="root"/>
        <xsl:apply-templates select="plan"/>
        <xsl:apply-templates select="local"/>
  </xsl:template>

  <xsl:template match="root">
        <xsl:apply-templates select="plan"/>
        <xsl:apply-templates select="local"/>
  </xsl:template>

  <xsl:template match="plan">
        <xsl:if test="member[.!='']"> 
          <xsl:text>member </xsl:text>  <xsl:value-of select="member"/>
        </xsl:if>
        <xsl:if test="string-length(stmt_id) > 0 "> 
          <xsl:text>stmt_id </xsl:text> <xsl:value-of select="stmt_id"/>
          &cr; 
          <xsl:text>SQL_stmt </xsl:text><xsl:value-of select="normalize-space(statement)"/> 
          &cr;
        </xsl:if>
        <xsl:text> begin_execution </xsl:text> <xsl:value-of select="begin_exe_time"/> 
        <xsl:text> end_execution </xsl:text> <xsl:value-of select="end_exe_time"/> 
        <xsl:if test="count(elapsed_time)!=0">
           	<xsl:choose>
           	<xsl:when test="elapsed_time &gt; 999 ">
           		<xsl:value-of select="concat(' (', format-number(elapsed_time div 1000,'###,###,##0.00'), ' seconds elapsed) ')"/> 
         	</xsl:when>
         	<xsl:otherwise>
           		<xsl:value-of select="concat(' (', elapsed_time, ' milli-seconds elapsed) ')"/> 
         	</xsl:otherwise>
       		</xsl:choose>
        </xsl:if>
        &cr;
        <xsl:apply-templates select="details/node"/>
        <xsl:apply-templates select="local"/>
  </xsl:template>

  <xsl:template match="local">
        <xsl:text>Local plan:</xsl:text>
            &cr;
        <xsl:if test="member[.!='']"> 
          <xsl:text>member </xsl:text> <xsl:value-of select="member"/> 
        </xsl:if>
        <xsl:if test="string-length(stmt_id) > 0 "> 
          <xsl:text>stmt_id </xsl:text> <xsl:value-of select="stmt_id"/>
        </xsl:if>
        <xsl:text> begin_execution </xsl:text> <xsl:value-of select="begin_exe_time"/>
        <xsl:text> end_execution </xsl:text> <xsl:value-of select="end_exe_time"/> 
        <xsl:if test="count(elapsed_time)!=0">
           	<xsl:choose>
           	<xsl:when test="elapsed_time &gt; 999 ">
           		<xsl:value-of select="concat(' (', format-number(elapsed_time div 1000,'###,###,##0.00'), ' seconds elapsed) ')"/> 
         	</xsl:when>
         	<xsl:otherwise>
           		<xsl:value-of select="concat(' (', elapsed_time, ' milli-seconds elapsed) ')"/> 
         	</xsl:otherwise>
       		</xsl:choose>
        </xsl:if>
        &cr;
        <xsl:apply-templates select="details/node"/>
  </xsl:template>
  
  <xsl:template match="node">
        <xsl:param name="depth"><xsl:value-of select="@depth"/></xsl:param>
        <!--  The list of node types for which we display the DETAIL string 
        -->    
        <xsl:variable name="detail_print" select="'ROWIDSCAN AGGREGATION PROJECTION FILTER PROJECT-FILTER INDEXSCAN CONSTRAINTSCAN REGION-GET'"/>
        
        <!--xsl:call-template name="spaces">
          <xsl:with-param name="howMany" select="$depth * 2" />
        </xsl:call-template-->
        <xsl:call-template name="lineageToSpace">
          <xsl:with-param name="pText" select="@lineage" />
          <xsl:with-param name="rank" select="@rank" />
        </xsl:call-template>
        
        <!-- xsl:text>(</xsl:text>
        <xsl:value-of select="@rank"/>
        &space;
        <xsl:text>)</xsl:text-->
        
        <xsl:value-of select="@name"/>

			<!-- Print out percentage of total execution time of the plan
			       as a formatted decimal with % character trailing
			       and parentheses   i.e   (85.33%)
			  -->
	        &space;
	        <!-- xsl:if test="count(@percent_exec_time)!=0 and number(@percent_exec_time)!=0"-->
	        	<xsl:value-of select="concat('(',format-number(@percent_exec_time div 100,'0.00%'),')')"/>
	        <!--/xsl:if-->
	        
	        <xsl:if test="count(@execute_time)!=0">
	            <xsl:apply-templates select="@execute_time"/>
	        </xsl:if>

	        <xsl:if test="count(@construct_time)!=0">
	            <xsl:value-of select="concat(' (construct=',@construct_time,' + open=',@open_time,' + next=',@next_time,' + close=',@close_time,')')"/>
	        </xsl:if>
	        
	        <xsl:if test="count(@process_time)!=0">
	            <xsl:value-of select="concat(' (serialize/deserialize=',@ser_deser_time,' + process=',@process_time,' + throttle=',@throttle_time,')')"/>
	        </xsl:if>

        	<xsl:if test="count(@input_rows)!=0">
            	<xsl:apply-templates select="@input_rows"/>
	        </xsl:if>

            <xsl:apply-templates select="@returned_rows"/>

            <xsl:apply-templates select="@no_opens"/>

            <xsl:apply-templates select="@visited_pages"/>
            
            <xsl:apply-templates select="@visited_rows"/>

            <xsl:apply-templates select="@scan_qualifiers"/>

            <xsl:apply-templates select="@next_qualifiers"/>

            <xsl:apply-templates select="@scanned_object"/>

            <xsl:apply-templates select="@scan_type"/>

            <xsl:apply-templates select="@sort_type"/>

            <xsl:apply-templates select="@sorter_input"/>

            <xsl:apply-templates select="@sorter_output"/>
            
            <xsl:apply-templates select="@member_node"/>

			<!-- Print out node details for the following node types
			       ROWIDSCAN (shows columns selected from index)
			       AGGREGATION (shows agg types evaluated)
			       PROJECTION/FILTER/PROJECT-FILTER (shows non-agg columns/filter preds)
			       INDEXSCAN/CONSTRAINTSCAN (shows non-qualifier predicates)
			       REGION-GET (shows table and column names)
			-->
            <xsl:if test="contains(concat(' ', $detail_print, ' '), concat(' ',@name, ' ') )">
              <xsl:apply-templates select="@node_details"/>
            </xsl:if>
            
            &cr;
        
        <xsl:apply-templates select="node"/>

  </xsl:template>

  <xsl:template match="node/@*">

        &space;
        <xsl:value-of select="normalize-space(name())"/>

        &space;
        <xsl:value-of select="."/> 

  </xsl:template>
</xsl:stylesheet>
