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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import java.util.*;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


public class Bootstrap extends TestCase{
   public Bootstrap(String testName) {
    super(testName);
  }
  
  public static Test suite(){
    TestSuite suite = new TestSuite(Bootstrap.class);
    return suite;
  }
  
  protected void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }
  
  protected void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
//  private static final String APP_NAME = "TestApp";
  
  
  private static Region _courses;
  private static Region _depts;
  private static Region _faculty;
  private static Region _persons;
  private static Region _students;
  
  private static int _sizeHint = 0;
  private static int _nullChance = 1; // % chance for a null value
  
  public void test1() throws Exception{
    init();
    //initApp();
    initPersons();
    initDepartments();
    initCourses();
    initFaculty();
    initStudents();
    initG_Students();
    initUG_Students();
    fillInDepartments();
    fillInFaculty();
    
  }
  
  
  private static void init()
  throws Exception {
  }
  
  
  //     private static void initApp()
  //         throws Exception
  //     {
  //         try
  //         {
  //             _gsSession.setCurrentApplication(APP_NAME);
  //         }
  //         catch(NameNotFoundException e)
  //         {
  //             CacheUtils.log("Creating new Application named " + APP_NAME);
  //             _gsSession.createApplication(APP_NAME);
  //             Context app = (Context)_gsSession.resolveFully(GsSession.APPLICATIONS + "/" + APP_NAME);
  //             app.unbind(GsSession.HIDDEN);
  //             _gsSession.setCurrentApplication(APP_NAME);
  //         }
  //     }
  
  private static void initPersons()
  throws Exception {
        /*IndexableSet qSet = Utils.getQueryService().newIndexableSet(Person.class);
        _gsSession.getInitialContext().rebind("Persons", qSet);
        _persons = qSet;
        Utils.checkpoint(_gsSession);
         */
    _persons = CacheUtils.createRegion("Persons", Person.class);
  }
  
  
  
  private static void initDepartments()
  throws Exception {
        /*IndexableSet qSet = Utils.getQueryService().newIndexableSet(Department.class);
        _gsSession.getInitialContext().rebind("Departments", qSet);
        _depts = qSet;*/
    _depts = CacheUtils.createRegion("Departments", Department.class);
    // fixed data
    for (int i = 0; i < DEPTS.length; i++) {
      Department dept = new Department(DEPTS[i][0],
              DEPTS[i][1],
              DEPTS[i][2],
              null /* fill in later after faculty inited */);
      //qSet.add(dept);
      CacheUtils.log("dept="+dept);
      _depts.put("dept"+i,dept);
    }
    
    // random data
    for (int i = 0; i < _sizeHint; i++) {
      Department dept = new Department(getRandomName(),
              getRandomDeptId(),
              getRandomOffice(), null);
      //qSet.add(dept);
      CacheUtils.log("dept="+dept);
      _depts.put("dept"+i,dept);
    }
    
    
    //Utils.checkpoint(_gsSession);
    
  }
  
  
  
  
  private static void initCourses()
  throws Exception {
        /*IndexableSet qSet = Utils.getQueryService().newIndexableSet(Course.class);
        _gsSession.getInitialContext().rebind("Courses", qSet);
        _courses = qSet;
         */
    _courses = CacheUtils.createRegion("Courses", Course.class);
    // fixed data
    for (int i = 0; i < COURSES.length; i++) {
      Course crs = new Course(COURSES[i][1],
              COURSES[i][0],
              null);
      crs.setDepartment((Department)_depts.selectValue("id = '" + crs.getDeptId() + "'"));
      //qSet.add(crs);
      CacheUtils.log(crs);
      _courses.put("course"+i,crs);
    }
    
    // random data
    for (int i = 0; i < _sizeHint; i++) {
      Course crs = new Course(getRandomName(),
              getRandomCourseNumber(), null);
      crs.setDepartment((Department)_depts.selectValue("id = '" + crs.getDeptId() + "'"));
      //qSet.add(crs);
      CacheUtils.log(crs);
      _courses.put("course"+i,crs);
    }
    
    
    //Utils.checkpoint(_gsSession);
  }
  
  
  
  private static void initFaculty()
  throws Exception {
        /*IndexableSet qSet = Utils.getQueryService().newIndexableSet(Faculty.class);
        _gsSession.getInitialContext().rebind("Faculty", qSet);
        _faculty = qSet;
         */
    _faculty = CacheUtils.createRegion("Faculty", Faculty.class);
    // fixed data
    for (int i = 0; i < FACULTY.length; i++) {
      Faculty fac = new Faculty(FACULTY[i][0],
              FACULTY[i][1],
              java.sql.Date.valueOf(FACULTY[i][2]),
              FACULTY[i][3],
              (Department)_depts.selectValue("id = '" + FACULTY[i][4] + "'"),
              Integer.parseInt(FACULTY[i][5]),
              Utils.toTokens(FACULTY[i][6]),
              null /* fill in later after Students inited */);
      //qSet.add(fac);
      CacheUtils.log("fac"+fac);
      _faculty.put("fac"+i,fac);
      _persons.put("fac"+i,fac);
    }
    
    // random data
    for (int i = 0; i < _sizeHint; i++) {
      Faculty fac = new Faculty(getRandomSSN(), getRandomName(), getRandomBD(),
              getRandomRank(), getRandomDepartment(), getRandomSalary(),
              getRandomHobbies(), null);
      //qSet.add(fac);
      CacheUtils.log("fac"+fac);
      _faculty.put("fac"+i,fac);
      _persons.put("fac"+i,fac);
    }
    
    
    //Utils.checkpoint(_gsSession);
  }
  
  
  private static void initStudents()
  throws Exception {
        /*IndexableSet qSet = Utils.getQueryService().newIndexableSet(Student.class);
        _gsSession.getInitialContext().rebind("Students", qSet);
        _students = qSet;
         */
    _students = CacheUtils.createRegion("Students", Student.class);
    for (int i = 0; i < STUDENTS.length; i++) {
      List crsNames = Utils.toTokens(STUDENTS[i][3]);
      List courses = new ArrayList();
      Iterator j = crsNames.iterator();
      while(j.hasNext()) {
        String cname = (String)j.next();
        Course c = (Course)_courses.selectValue("courseNumber = '" + cname + "'");
        CacheUtils.log("course=" + c);
        courses.add(c);
      }
      
      
      Student stud = new Student(STUDENTS[i][0],
              STUDENTS[i][1],
              java.sql.Date.valueOf(STUDENTS[i][2]),
              courses,
              Float.parseFloat(STUDENTS[i][4]),
              (Department)_depts
              .selectValue("id = '" + STUDENTS[i][5] + "'"));
      //qSet.add(stud);
      //_persons.add(stud);
      CacheUtils.log("stud"+stud);
      _students.put("stud"+i,stud);
      _persons.put("stud"+i,stud);
    }
    
    // Random
    for (int i = 0; i < _sizeHint; i++) {
      Student stud = new Student(getRandomSSN(), getRandomName(), getRandomBD(),
              getRandomCourses(), getRandomGPA(), getRandomDepartment());
      //qSet.add(stud);
      //CacheUtils.log(stud);
      //_persons.add(stud);
      CacheUtils.log(stud);
      _students.put("stud"+i,stud);
      _persons.put("stud"+i,stud);
      
    }
    
    //Utils.checkpoint(_gsSession);
    
    
  }
  
  
  private static void initG_Students()
  throws Exception {
        /*IndexableSet qSet = Utils.getQueryService().newIndexableSet(G_Student.class);
        _gsSession.getInitialContext().rebind("G_Students", qSet);
         **/
    Region g_students = CacheUtils.createRegion("G_Students",G_Student.class);
    for (int i = 0; i < G_STUDENTS.length; i++) {
      List crsNames = Utils.toTokens(G_STUDENTS[i][3]);
      List courses = new ArrayList();
      Iterator j = crsNames.iterator();
      while(j.hasNext()) {
        String cname = (String)j.next();
        Course c = (Course)_courses.selectValue("courseNumber = '" + cname + "'");
        CacheUtils.log("course=" + c);
        courses.add(c);
      }
      
      
      G_Student stud = new G_Student(G_STUDENTS[i][0],
              G_STUDENTS[i][1],
              java.sql.Date.valueOf(G_STUDENTS[i][2]),
              courses,
              Float.parseFloat(G_STUDENTS[i][4]),
              (Department)_depts
              .selectValue("id = '" + G_STUDENTS[i][5] + "'"),
              G_STUDENTS[i][6],
              (Faculty)_faculty.selectValue("name = '" + G_STUDENTS[i][7] + "'"));
      //qSet.add(stud);
      //_persons.add(stud);
      //_students.add(stud);
      CacheUtils.log("stud"+stud);
      g_students.put("stud"+i,stud);
      _persons.put("stud"+i,stud);
      _students.put("stud"+i,stud);
    }
    
    // Random
    for (int i = 0; i < _sizeHint; i++) {
      G_Student stud = new G_Student(getRandomSSN(), getRandomName(), getRandomBD(),
              getRandomCourses(), getRandomGPA(), getRandomDepartment(),
              getRandomName(), getRandomFaculty());
      //qSet.add(stud);
      //CacheUtils.log(stud);
      //_persons.add(stud);
      //_students.add(stud);
      CacheUtils.log("stud"+stud);
      g_students.put("stud"+i,stud);
      _persons.put("stud"+i,stud);
      _students.put("stud"+i,stud);
      
    }
    
    //Utils.checkpoint(_gsSession);
    
  }
  
  
  private static void initUG_Students()
  throws Exception {
    /*IndexableSet qSet = Utils.getQueryService().newIndexableSet(UG_Student.class);
    _gsSession.getInitialContext().rebind("UG_Students", qSet);
     **/
    Region ug_students = CacheUtils.createRegion("UG_Students", UG_Student.class);
    for (int i = 0; i < UG_STUDENTS.length; i++) {
      List crsNames = Utils.toTokens(UG_STUDENTS[i][3]);
      List courses = new ArrayList();
      Iterator j = crsNames.iterator();
      while(j.hasNext()) {
        String cname = (String)j.next();
        Course c = (Course)_courses.selectValue("courseNumber = '" + cname + "'");
        CacheUtils.log("course=" + c);
        courses.add(c);
      }
      
      
      UG_Student stud = new UG_Student(UG_STUDENTS[i][0],
              UG_STUDENTS[i][1],
              java.sql.Date.valueOf(UG_STUDENTS[i][2]),
              courses,
              Float.parseFloat(UG_STUDENTS[i][4]),
              (Department)_depts
              .selectValue("id = '" + UG_STUDENTS[i][5] + "'"),
              Integer.parseInt(UG_STUDENTS[i][6]),
              Integer.parseInt(UG_STUDENTS[i][7]));
      //qSet.add(stud);
      //_persons.add(stud);
      //_students.add(stud);
      
      CacheUtils.log(stud);
      ug_students.put("stud"+i, stud);
      _persons.put("stud"+i, stud);
      _students.put("stud"+i, stud);
    }
    
    // Random
    for (int i = 0; i < _sizeHint; i++) {
      UG_Student stud = new UG_Student(getRandomSSN(), getRandomName(), getRandomBD(),
              getRandomCourses(), getRandomGPA(), getRandomDepartment(),
              Utils.randomInt(1, 4), Utils.randomInt(500, 2000));
      //qSet.add(stud);
      //CacheUtils.log(stud);
      //_persons.add(stud);
      //_students.add(stud);
      CacheUtils.log(stud);
      ug_students.put("stud"+i, stud);
      _persons.put("stud"+i, stud);
      _students.put("stud"+i, stud);
      
    }
    
    //Utils.checkpoint(_gsSession);
  }
  
  
  
  private static void fillInDepartments()
  throws Exception {
    Iterator di = _depts.values().iterator();
    while (di.hasNext()) {
      Department d = (Department)di.next();
      String chairName = null;
      for (int i = 0; i < DEPTS.length; i++)
        if (DEPTS[i][1].equals(d.getId())) {
        chairName = DEPTS[i][3];
        break;
        }
      if (chairName == null)
        d.setChairperson(getRandomFaculty());
      else {
        Faculty f = (Faculty)_faculty.selectValue("name = '" + chairName + "'");
        d.setChairperson(f);
      }
      
    }
    
    //Utils.checkpoint(_gsSession);
  }
  
  
  private static void fillInFaculty()
  throws Exception {
    Iterator fi = _faculty.values().iterator();
    while (fi.hasNext()) {
      Faculty f = (Faculty)fi.next();
//      String deptId = null;
      String advisees = null;
      for (int i = 0; i < FACULTY.length; i++)
        if (FACULTY[i][0].equals(f.getSSN())) {
        advisees = FACULTY[i][7];
        break;
        }
      if (advisees == null) {
        f.setAdvisees(getRandomStudents());
      } else {
        List adviseeSsnList = Utils.toTokens(advisees);
//        List adviseeList = new ArrayList();
        Iterator i = adviseeSsnList.iterator();
        while (i.hasNext()) {
          String ssn = (String)i.next();
          G_Student stud = (G_Student)_students.selectValue("SSN = '" + ssn + "'");
          CacheUtils.log("advisee = "  + stud);
          f.addAdvisee(stud);
        }
      }
    }
    
    //Utils.checkpoint(_gsSession);
  }
  
  // RANDOM DATA GENERATION
  private static String getRandomName() {
    // see if this should be null
    if (Utils.randomInt(1, 100) <= _nullChance)
      return null;
    
    int length = Utils.randomInt(3, 12);
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < length; i++) {
      buf.append(Utils.randomAlphabetic());
    }
    return buf.toString();
  }
  
  private static String getRandomDeptId() {
    // see if this should be null
    if (Utils.randomInt(1, 100) <= _nullChance)
      return null;
    
    StringBuffer buf = new StringBuffer(3);
    for (int i = 0; i < 3; i++)
      buf.append(Utils.randomAlphabetic());
    return buf.toString();
  }
  
  
  private static String getRandomOffice() {
    // see if this should be null
    if (Utils.randomInt(1, 100) <= _nullChance)
      return null;
    
    StringBuffer buf = new StringBuffer(5);
    buf.append(Utils.randomAlphabetic());
    buf.append('-');
    buf.append(String.valueOf(Utils.randomInt(0, 999)));
    return buf.toString();
  }
  
  private static String getRandomCourseNumber() {
    // see if this should be null
    if (Utils.randomInt(1, 100) <= _nullChance)
      return null;
    
    // get a dept id
    Department dept = getRandomDepartment();
    String deptID = dept.getId();
    int num = Utils.randomInt(0, 999);
    String cnum = ((num < 10) ? "0" :"") + ((num < 100) ? "0" : "") + String.valueOf(num);
    return deptID + cnum;
  }
  
  private static String getRandomSSN() {
    // see if this should be null
    if (Utils.randomInt(1, 100) <= _nullChance)
      return null;
    
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < 9;i++)
      buf.append(String.valueOf(Utils.randomInt(0,9)));
    return buf.toString();
  }
  
  private static java.sql.Date getRandomBD() {
    // see if this should be null
    if (Utils.randomInt(1, 100) <= _nullChance)
      return null;
    
    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(Calendar.YEAR, Utils.randomInt(1940, 1982));
    cal.set(Calendar.MONTH, Utils.randomInt(0, 11));
    cal.set(Calendar.DATE, Utils.randomInt(1, 30));
    return new java.sql.Date(cal.getTime().getTime());
  }
  
  
  private static final String[] RANKS = { "Professor", "Associate Professor" };
  
  
  private static String getRandomRank() {
    // see if this should be null
    if (Utils.randomInt(1, 100) <= _nullChance)
      return null;
    
    return RANKS[Utils.randomInt(0, RANKS.length - 1)];
  }
  
  private static Department getRandomDepartment() {
    int dn = Utils.randomInt(1, _depts.values().size());
    Iterator i = _depts.values().iterator();
    Department dept = null;
    for (int j = 0; j < dn; j++)
      dept = (Department)i.next();
    return dept;
  }
  
  
  private static int getRandomSalary() {
    return Utils.randomInt(10000, 150000);
  }
  
  
  private static Set getRandomHobbies() {
    int num = Utils.randomInt(0, 12);
    HashSet set = new HashSet();//Utils.getQueryService().newIndexableSet(String.class);
    for (int i = 0; i < num; i++)
      set.add(HOBBIES[Utils.randomInt(0, HOBBIES.length-1)]);
    return set;
  }
  
  private static Faculty getRandomFaculty() {
    return null;
  }
  
  private static Set getRandomStudents() {
    return null;
  }
  
  
  private static Set getRandomCourses() {
    return null;
  }
  
  private static float getRandomGPA() {
    return 0.0f;
  }
  
  private static final String[][] COURSES = {
    {"CSE500","Introduction to Software Engineering"} ,
    {"CSE502","Functional Programming"} ,
    {"CSE503","Software Engineering Processes"},
    {"CSE504","Object-Oriented Analysis and Design"},
    {"CSE507","Logic Programming"},
    {"CSE509","Object-Oriented Programming"},
    {"CSE510","Software Tools"},
    {"CSE511","Principles of Compiler Design"},
    {"CSE512","Compiling Functional Languages"},
    {"CSE513","Introduction to Operating Systems"},
    {"CSE514","Introduction to Database Systems"},
    {"CSE515","Distributed Computing Systems"},
    {"CSE516","Computer Graphics: Theory and Application"},
    {"CSE518","Software Design and Development"},
    {"BMB527","Biochemistry I: Proteins and Enzymes"},
    {"BMB528","Biochemistry II: Introduction to Molecular Biology"},
    {"BMB529","Biochemistry III: Metabolism and Bioenergetics"},
    {"BMB530","Enzyme Structure and Function"},
    {"BMB531","Enzyme Mechanisms"},
    {"BMB532","Bioenergetics and Membrane Transport"},
    {"BMB534","Instrumental Methods in Biophysics I"},
    {"BMB535","Instrumental Methods in Biophysics II"},
    {"BMB537","Metals in Biochemistry"},
    {"BMB538","Coordination Chemistry"},
    {"BMB539","Chemical Group Theory"},
    {"BMB540","Advanced Molecular Biology"},
    {"BMB541","Molecular Genetics of Development"},
    {"BMB542","Molecular Cell Biology"},
    {"BMB543","Current Topics in Proteomics"},
    {"BMB544","Introduction to Computational Biology"},
    {"ESE500","Numerical Methods"},
    {"ESE504","Uncertainty Analysis"},
    {"ESE506","Environmental Systems Analysis"},
    {"ESE508","Advanced Topics in Numerical Methods"},
    {"ESE510","Aquatic Chemistry"},
    {"ESE511","Advanced Aquatic Chemistry"},
    {"ESE514","Distribution and Fate of Organic Pollutants"},
    {"ESE516","Chemical Degradation and Remediation"},
    {"ESE530","Transport Processes"},
    {"MSE400","Mathematics for Engineers"},
    {"MSE490","Statistics for Engineers"},
    {"MSE495","Heat Treatment of Steel"},
    {"MSE500","Introduction to Crystallography"},
    {"MSE501","Theory of Engineering Materials"},
    {"MSE503","Mechanics of Materials"},
    {"MSE505","Metallurgical Thermodynamics in Thin Films"},
    {"MSE507","Materials Selection in Mechanical Design"}
  };
  
  
  
  
  private static final String[][] DEPTS = {
    {"Biochemistry and Molecular Biology","BMB","A-100","BLACKBURN"},
    {"Computer Science and Engineering", "CSE","B-100","BLACK"},
    {"Electrical and Computer Engineering","ECE","C-100","HAMMERSTROM"},
    {"Environmental Science and Engineering","ESE","D-100","PANKOW"},
    {"Management in Science and Technology","MST","E-100","PHILLIPS"},
    {"Materials Science and Engineering","MSE","F-100","ATTERIDGE"}
  };
  
  
  
  
  private static final String[][] FACULTY = {
    // ssn name bd rank dept salary hobbies advisees
    {"000000000","BLACKBURN","1950-01-05","Professor and Department Head","BMB","50000", "a b c", "678912345"},
    {"010101010","BLACK","1951-02-06","Professor and Department Head","CSE","60000","cycling skiing hiking","456789123"},
    {"101010101","HAMMERSTROM","1955-03-17","Professor and Department Head","ECE","70000","b d e",""},
    {"020202020","PANKOW","1960-04-18","Professor and Department Head","ESE","80000","b a f",""},
    {"202020202","ATTERIDGE","1965-05-23","Professor and Department Head","MSE","90000","c f g",""},
    {"121221212", "PHILLIPS", "1970-06-22","Professor and Department Head","MST","100000","g h a", ""},
    {"912345678", "KENT", "1955-07-30","Professor","ECE", "63000", "h c x a b", "" },
    {"112345678","MARIA","1959-08-31","Associate Professor","MST", "45000", "z",""},
    {"123456781","LISA","1953-09-03","Professor","ESE", "65000","p d q","567891234"}
  };
  
  private static final String[][] STUDENTS = {
    // ssn name bd courses gpa dept
    {"123456789", "JOHN", "1966-10-31","CSE502 CSE510 CSE502","3.2","CSE"},
    {"234567891","KETTY","1973-11-14","BMB529 BMB531 CSE500","2.8","BMB"},
    {"345678912","WANG","1981-12-19","ESE500 MSE400","3.5","ESE"}
  };
  
  
  private static final String[][] G_STUDENTS = {
    // ssn name bd courses gpa dept thesis advisor
    {"456789123", "YOUNG", "1976-01-09","CSE518 CSE512 ESE530 MSE400","3.5","CSE", "Title1" , "BLACK"},
    {"567891234","WANG","1968-02-01","ESE508","3.8","ESE","Title2","LISA"},
    {"678912345","MARY","1973-03-31","BMB532 BMB528","3.5","BMB", "Title3", "BLACKBURN"}
  };
  
  
  private static final String[][] UG_STUDENTS = {
    // ssn name bd courses gpa dept year satScore
    {"789123456", "JULIE", "1981-04-01","MSE400 MSE490","3.5","MSE", "2" , "1268"},
    {"891234567","BOB","1980-05-19","MSE490 MSE495","3.2","MSE","3","1198"},
  };
  
  private static final String[] HOBBIES = {
    "Amateur and Ham Radio",
            "Amateur Telescope Making",
            "Aquariums",
            "Astronomy",
            "Beachcombing",
            "Bell Ringing",
            "Birding" ,
            "Books" ,
            "Bubbles",
            "Candlemaking",
            "Collecting",
            "Companies",
            "Crafts",
            "Dolls",
            "Dumpster Diving",
            "Electronics",
            "Events",
            "Firewalking",
            "Games",
            "Gardening",
            "Genealogy",
            "Handwriting Analysis",
            "Homebrewing",
            "Juggling",
            "Kites",
            "Knotting",
            "Lock Picking",
            "Magazines",
            "Magic",
            "Models",
            "Photography",
            "Pottery",
            "Puppetry",
            "Pyrotechnics",
            "Rockets",
            "Rocks, Gems, and Minerals",
            "Scrapbooks" ,
            "Smokeless Tobacco",
            "Smoking",
            "Soapmaking",
            "String Figures",
            "Textiles",
            "Treasure Hunting",
            "Urban Exploration",
            "Writing"
  };
  
  
  
}
