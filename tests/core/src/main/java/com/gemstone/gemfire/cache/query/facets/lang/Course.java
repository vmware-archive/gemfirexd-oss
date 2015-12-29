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


package com.gemstone.gemfire.cache.query.facets.lang;


//import java.util.*;


public class Course
{
    private String _title;
    private String _courseNum;
    private Department _dept;


    public Course()
    {
    }

    public Course(String title, String courseNum, Department department)
    {
        _title = title;
        _courseNum = courseNum;
        _dept = department;
        
    }

    public String toString()
    {
        return getCourseNumber() + ':'+ getTitle();
    }
    

    public String getTitle()
    {
        return _title;
    }

    public String getCourseNumber()
    {
        return _courseNum;
    }

    public Department getDepartment()
    {
        return _dept;
    }

    public String getDeptId()
    {
        System.out.println(this);
        return getCourseNumber().substring(0,3);
    }
    


    public void setTitle(String title)
    {
        _title = title;
    }


    public void setCourseNumber(String courseNum)
    {
        _courseNum = courseNum;
    }

    public void setDepartment(Department dept)
    {
        _dept = dept;
    }

    
}
