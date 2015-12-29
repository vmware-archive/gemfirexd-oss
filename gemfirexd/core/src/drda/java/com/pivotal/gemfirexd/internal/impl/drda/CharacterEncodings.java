/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.drda.CharacterEncodings

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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

package com.pivotal.gemfirexd.internal.impl.drda;

final class CharacterEncodings
{

  // This is a static class, so hide the default constructor.
  private CharacterEncodings () {}

  //private static java.util.Hashtable javaEncodingToCCSIDTable__ = new java.util.Hashtable();
  private static java.util.Hashtable ccsidToJavaEncodingTable__ = new java.util.Hashtable();

  static {
    populate_ccsidToJavaEncodingTable();
    //populate_javaEncodingToCCSIDTable();
  }

  /*
  static void populate_javaEncodingToCCSIDTable ()
  {
    javaEncodingToCCSIDTable__.put ("Cp037", Integer.valueOf(37));
    javaEncodingToCCSIDTable__.put ("Cp273", Integer.valueOf(273));
    javaEncodingToCCSIDTable__.put ("Cp277", Integer.valueOf(277));
    javaEncodingToCCSIDTable__.put ("Cp278", Integer.valueOf(278));
    javaEncodingToCCSIDTable__.put ("Cp280", Integer.valueOf(280));
    javaEncodingToCCSIDTable__.put ("Cp284", Integer.valueOf(284));
    javaEncodingToCCSIDTable__.put ("Cp285", Integer.valueOf(285));
    javaEncodingToCCSIDTable__.put ("Cp297", Integer.valueOf(297));
    javaEncodingToCCSIDTable__.put ("Cp420", Integer.valueOf(420));
    javaEncodingToCCSIDTable__.put ("Cp424", Integer.valueOf(424));
    javaEncodingToCCSIDTable__.put ("Cp747", Integer.valueOf(437));
    javaEncodingToCCSIDTable__.put ("Cp500", Integer.valueOf(500));
    javaEncodingToCCSIDTable__.put ("Cp737", Integer.valueOf(737));
    javaEncodingToCCSIDTable__.put ("Cp775", Integer.valueOf(775));
    javaEncodingToCCSIDTable__.put ("ISO8859_7", Integer.valueOf(813));
    javaEncodingToCCSIDTable__.put ("ISO8859_1", Integer.valueOf(819));
    javaEncodingToCCSIDTable__.put ("Cp838", Integer.valueOf(838));
    javaEncodingToCCSIDTable__.put ("Cp850", Integer.valueOf(850));
    javaEncodingToCCSIDTable__.put ("Cp852", Integer.valueOf(852));
    javaEncodingToCCSIDTable__.put ("Cp855", Integer.valueOf(855));
    javaEncodingToCCSIDTable__.put ("Cp856", Integer.valueOf(856));
    javaEncodingToCCSIDTable__.put ("Cp857", Integer.valueOf(857));
    javaEncodingToCCSIDTable__.put ("Cp858", Integer.valueOf(858));
    javaEncodingToCCSIDTable__.put ("Cp860", Integer.valueOf(860));
    javaEncodingToCCSIDTable__.put ("Cp861", Integer.valueOf(861));
    javaEncodingToCCSIDTable__.put ("Cp862", Integer.valueOf(862));
    javaEncodingToCCSIDTable__.put ("Cp863", Integer.valueOf(863));
    javaEncodingToCCSIDTable__.put ("Cp864", Integer.valueOf(864));
    javaEncodingToCCSIDTable__.put ("Cp865", Integer.valueOf(865));
    javaEncodingToCCSIDTable__.put ("Cp866", Integer.valueOf(866));
    javaEncodingToCCSIDTable__.put ("Cp868", Integer.valueOf(868));
    javaEncodingToCCSIDTable__.put ("Cp869", Integer.valueOf(869));
    javaEncodingToCCSIDTable__.put ("Cp870", Integer.valueOf(870));
    javaEncodingToCCSIDTable__.put ("Cp871", Integer.valueOf(871));
    javaEncodingToCCSIDTable__.put ("Cp874", Integer.valueOf(874));
    javaEncodingToCCSIDTable__.put ("Cp875", Integer.valueOf(875));
    javaEncodingToCCSIDTable__.put ("KOI8_R", Integer.valueOf(878));
    javaEncodingToCCSIDTable__.put ("ISO8859_2", Integer.valueOf(912));
    javaEncodingToCCSIDTable__.put ("ISO8859_3", Integer.valueOf(913));
    javaEncodingToCCSIDTable__.put ("ISO8859_4", Integer.valueOf(914));
    javaEncodingToCCSIDTable__.put ("ISO8859_5", Integer.valueOf(915));
    javaEncodingToCCSIDTable__.put ("ISO8859_8", Integer.valueOf(916));
    javaEncodingToCCSIDTable__.put ("Cp918", Integer.valueOf(918));
    javaEncodingToCCSIDTable__.put ("ISO8859_9", Integer.valueOf(920));
    javaEncodingToCCSIDTable__.put ("ISO8859_15_FDIS", Integer.valueOf(923));
    javaEncodingToCCSIDTable__.put ("Cp921", Integer.valueOf(921));
    javaEncodingToCCSIDTable__.put ("Cp922", Integer.valueOf(922));
    javaEncodingToCCSIDTable__.put ("Cp930", Integer.valueOf(930));
    javaEncodingToCCSIDTable__.put ("Cp933", Integer.valueOf(933));
    javaEncodingToCCSIDTable__.put ("Cp935", Integer.valueOf(935));
    javaEncodingToCCSIDTable__.put ("Cp937", Integer.valueOf(937));
    javaEncodingToCCSIDTable__.put ("Cp939", Integer.valueOf(939));
    javaEncodingToCCSIDTable__.put ("Cp948", Integer.valueOf(948));
    javaEncodingToCCSIDTable__.put ("Cp950", Integer.valueOf(950));
    javaEncodingToCCSIDTable__.put ("Cp964", Integer.valueOf(964));
    javaEncodingToCCSIDTable__.put ("Cp970", Integer.valueOf(970));
    javaEncodingToCCSIDTable__.put ("Cp1006", Integer.valueOf(1006));
    javaEncodingToCCSIDTable__.put ("Cp1025", Integer.valueOf(1025));
    javaEncodingToCCSIDTable__.put ("Cp1026", Integer.valueOf(1026));
    javaEncodingToCCSIDTable__.put ("Cp1046", Integer.valueOf(1046));
    javaEncodingToCCSIDTable__.put ("ISO8859_6", Integer.valueOf(1089));
    javaEncodingToCCSIDTable__.put ("Cp1097", Integer.valueOf(1097));
    javaEncodingToCCSIDTable__.put ("Cp1098", Integer.valueOf(1098));
    javaEncodingToCCSIDTable__.put ("Cp1112", Integer.valueOf(1112));
    javaEncodingToCCSIDTable__.put ("Cp1122", Integer.valueOf(1122));
    javaEncodingToCCSIDTable__.put ("Cp1123", Integer.valueOf(1123));
    javaEncodingToCCSIDTable__.put ("Cp1124", Integer.valueOf(1124));
    javaEncodingToCCSIDTable__.put ("Cp1140", Integer.valueOf(1140));
    javaEncodingToCCSIDTable__.put ("Cp1141", Integer.valueOf(1141));
    javaEncodingToCCSIDTable__.put ("Cp1142", Integer.valueOf(1142));
    javaEncodingToCCSIDTable__.put ("Cp1143", Integer.valueOf(1143));
    javaEncodingToCCSIDTable__.put ("Cp1144", Integer.valueOf(1144));
    javaEncodingToCCSIDTable__.put ("Cp1145", Integer.valueOf(1145));
    javaEncodingToCCSIDTable__.put ("Cp1146", Integer.valueOf(1146));
    javaEncodingToCCSIDTable__.put ("Cp1147", Integer.valueOf(1147));
    javaEncodingToCCSIDTable__.put ("Cp1148", Integer.valueOf(1148));
    javaEncodingToCCSIDTable__.put ("Cp1149", Integer.valueOf(1149));
    javaEncodingToCCSIDTable__.put ("UTF8", Integer.valueOf(1208));
    javaEncodingToCCSIDTable__.put ("Cp1250", Integer.valueOf(1250));
    javaEncodingToCCSIDTable__.put ("Cp1251", Integer.valueOf(1251));
    javaEncodingToCCSIDTable__.put ("Cp1252", Integer.valueOf(1252));
    javaEncodingToCCSIDTable__.put ("Cp1253", Integer.valueOf(1253));
    javaEncodingToCCSIDTable__.put ("Cp1254", Integer.valueOf(1254));
    javaEncodingToCCSIDTable__.put ("Cp1255", Integer.valueOf(1255));
    javaEncodingToCCSIDTable__.put ("Cp1256", Integer.valueOf(1256));
    javaEncodingToCCSIDTable__.put ("Cp1257", Integer.valueOf(1257));
    javaEncodingToCCSIDTable__.put ("Cp1258", Integer.valueOf(1258));
    javaEncodingToCCSIDTable__.put ("MacGreek", Integer.valueOf(1280));
    javaEncodingToCCSIDTable__.put ("MacTurkish", Integer.valueOf(1281));
    javaEncodingToCCSIDTable__.put ("MacCyrillic", Integer.valueOf(1283));
    javaEncodingToCCSIDTable__.put ("MacCroatian", Integer.valueOf(1284));
    javaEncodingToCCSIDTable__.put ("MacRomania", Integer.valueOf(1285));
    javaEncodingToCCSIDTable__.put ("MacIceland", Integer.valueOf(1286));
    javaEncodingToCCSIDTable__.put ("Cp1381", Integer.valueOf(1381));
    javaEncodingToCCSIDTable__.put ("Cp1383", Integer.valueOf(1383));
    javaEncodingToCCSIDTable__.put ("Cp33722", Integer.valueOf(33722));

    javaEncodingToCCSIDTable__.put ("Cp290", Integer.valueOf(8482));
    javaEncodingToCCSIDTable__.put ("Cp300", Integer.valueOf(16684));
    javaEncodingToCCSIDTable__.put ("Cp930", Integer.valueOf(1390));
    javaEncodingToCCSIDTable__.put ("Cp833", Integer.valueOf(13121));
    javaEncodingToCCSIDTable__.put ("Cp834", Integer.valueOf(4930));
    javaEncodingToCCSIDTable__.put ("Cp836", Integer.valueOf(13124));
    javaEncodingToCCSIDTable__.put ("Cp837", Integer.valueOf(4933));
    javaEncodingToCCSIDTable__.put ("Cp943", Integer.valueOf(941));
    javaEncodingToCCSIDTable__.put ("Cp1027", Integer.valueOf(5123));
    javaEncodingToCCSIDTable__.put ("Cp1043", Integer.valueOf(904));
    javaEncodingToCCSIDTable__.put ("Cp1114", Integer.valueOf(5210));
    javaEncodingToCCSIDTable__.put ("ASCII", Integer.valueOf(367));
    javaEncodingToCCSIDTable__.put ("MS932", Integer.valueOf(932));
    javaEncodingToCCSIDTable__.put ("UnicodeBigUnmarked", Integer.valueOf(1200));
    javaEncodingToCCSIDTable__.put ("Cp943", Integer.valueOf(943));
    javaEncodingToCCSIDTable__.put ("Cp1362", Integer.valueOf(1114));
    javaEncodingToCCSIDTable__.put ("Cp301", Integer.valueOf(301));
    javaEncodingToCCSIDTable__.put ("Cp1041", Integer.valueOf(1041));
    javaEncodingToCCSIDTable__.put ("Cp1351", Integer.valueOf(1351));
    javaEncodingToCCSIDTable__.put ("Cp1088", Integer.valueOf(1088));
    javaEncodingToCCSIDTable__.put ("Cp951", Integer.valueOf(951));
    javaEncodingToCCSIDTable__.put ("Cp971", Integer.valueOf(971));
    javaEncodingToCCSIDTable__.put ("Cp1362", Integer.valueOf(1362));
    javaEncodingToCCSIDTable__.put ("Cp1363", Integer.valueOf(1363));
    javaEncodingToCCSIDTable__.put ("Cp1115", Integer.valueOf(1115));
    javaEncodingToCCSIDTable__.put ("Cp1380", Integer.valueOf(1380));
    javaEncodingToCCSIDTable__.put ("Cp1385", Integer.valueOf(1385));
    javaEncodingToCCSIDTable__.put ("Cp947", Integer.valueOf(947));
    javaEncodingToCCSIDTable__.put ("Cp942", Integer.valueOf(942));
    javaEncodingToCCSIDTable__.put ("Cp897", Integer.valueOf(897));
    javaEncodingToCCSIDTable__.put ("Cp949", Integer.valueOf(949));
    javaEncodingToCCSIDTable__.put ("Cp1370", Integer.valueOf(1370));
    javaEncodingToCCSIDTable__.put ("Cp927", Integer.valueOf(927));
    javaEncodingToCCSIDTable__.put ("Cp1382", Integer.valueOf(1382));
    javaEncodingToCCSIDTable__.put ("Cp1386", Integer.valueOf(1386));
    javaEncodingToCCSIDTable__.put ("Cp835", Integer.valueOf(835));
    javaEncodingToCCSIDTable__.put ("Cp1051", Integer.valueOf(1051));
  }
  */

  static void populate_ccsidToJavaEncodingTable ()
  {
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5346), "Cp1250");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5347), "Cp1251");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5348), "Cp1252");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5349), "Cp1253");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5350), "Cp1254");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5351), "Cp1255");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(4909), "Cp813");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(858), "Cp850"); //we can't map 858 to Cp850 because 858 has Euro characters that Cp858 doesn't support
    ccsidToJavaEncodingTable__.put (Integer.valueOf(872), "Cp855");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(867), "Cp862");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(17248), "Cp864");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(808), "Cp866");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1162), "Cp847");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(9044), "Cp852");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(9048), "Cp856");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(9049), "Cp857");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(9061), "Cp869");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(901), "Cp921");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(902), "Cp922");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(21427), "Cp947");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1370), "Cp950"); //we can't map 1370 to Cp1370 becasue 1370 has Euro character that Cp1370 doesn't support
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5104), "Cp1008");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(9238), "Cp1046");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(848), "Cp1125");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1163), "Cp1129");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(849), "Cp1131");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5352), "Cp1256");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5353), "Cp1257");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5354), "Cp1258");

    ccsidToJavaEncodingTable__.put (Integer.valueOf(37), "Cp037");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(273), "Cp273");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(277), "Cp277");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(278), "Cp278");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(280), "Cp280");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(284), "Cp284");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(285), "Cp285");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(297), "Cp297");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(420), "Cp420");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(424), "Cp424");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(437), "Cp437");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(500), "Cp500");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(737), "Cp737");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(775), "Cp775");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(838), "Cp838");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(850), "Cp850");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(852), "Cp852");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(855), "Cp855");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(856), "Cp856");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(857), "Cp857");
    //ccsidToJavaEncodingTable__.put (Integer.valueOf(858), "Cp858");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(860), "Cp860");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(861), "Cp861");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(862), "Cp862");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(863), "Cp863");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(864), "Cp864");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(865), "Cp865");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(866), "Cp866");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(868), "Cp868");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(869), "Cp869");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(870), "Cp870");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(871), "Cp871");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(874), "Cp874");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(875), "Cp875");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(918), "Cp918");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(921), "Cp921");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(922), "Cp922");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(930), "Cp930");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(933), "Cp933");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(935), "Cp935");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(937), "Cp937");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(939), "Cp939");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(948), "Cp948");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(950), "Cp950");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(964), "Cp964");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(970), "Cp970");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1006), "Cp1006");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1025), "Cp1025");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1026), "Cp1026");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1046), "Cp1046");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1097), "Cp1097");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1098), "Cp1098");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1112), "Cp1112");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1122), "Cp1122");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1123), "Cp1123");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1124), "Cp1124");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1140), "Cp1140");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1141), "Cp1141");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1142), "Cp1142");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1143), "Cp1143");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1144), "Cp1144");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1145), "Cp1145");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1146), "Cp1146");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1147), "Cp1147");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1148), "Cp1148");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1149), "Cp1149");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1250), "Cp1250");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1251), "Cp1251");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1252), "Cp1252");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1253), "Cp1253");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1254), "Cp1254");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1255), "Cp1255");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1256), "Cp1256");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1257), "Cp1257");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1258), "Cp1258");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1381), "Cp1381");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1383), "Cp1383");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(33722), "Cp33722");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(943), "Cp943");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1043), "Cp1043");

    ccsidToJavaEncodingTable__.put (Integer.valueOf(813), "ISO8859_7");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(819), "ISO8859_1");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(878), "KOI8_R");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(912), "ISO8859_2");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(913), "ISO8859_3");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(914), "ISO8859_4");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(915), "ISO8859_5");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(916), "ISO8859_8");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(920), "ISO8859_9");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(923), "ISO8859_15_FDIS");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1089), "ISO8859_6");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1208), "UTF8");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1280), "MacGreek");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1281), "MacTurkish");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1283), "MacCyrillic");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1284), "MacCroatian");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1285), "MacRomania");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1286), "MacIceland");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(8482), "Cp290");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(16684), "Cp300");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1390), "Cp930");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(13121), "Cp833");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(4930), "Cp834");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(13124), "Cp836");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(4933), "Cp837");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(941), "Cp943");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5123), "Cp1027");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(904), "Cp1043");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5210), "Cp1114");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(367), "ASCII");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(932), "MS932");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1200), "UnicodeBigUnmarked");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5026), "Cp930");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1399), "Cp939");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(4396), "Cp300");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1388), "Cp935");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1364), "Cp933");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(5035), "Cp939");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(28709), "Cp37");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1114), "Cp1362");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(954), "Cp33722");

    //----the following codepages may  only be supported by IBMSDk 1.3.1
    ccsidToJavaEncodingTable__.put (Integer.valueOf(301), "Cp301");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1041), "Cp1041");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1351), "Cp1351");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1088), "Cp1088");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(951), "Cp951");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(971), "Cp971");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1362), "Cp1362");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1363), "Cp1363");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1115), "Cp1115");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1380), "Cp1380");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1386), "Cp1386");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1385), "Cp1385");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(947), "Cp947");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(942), "Cp942");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(897), "Cp897");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(949), "Cp949");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(927), "Cp927");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1382), "Cp1382");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(290), "Cp290");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(300), "Cp300");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1027), "Cp1027");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(16686), "Cp16686");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(833), "Cp833");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(834), "Cp834");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(836), "Cp836");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(837), "Cp837");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(835), "Cp835");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(895), "Cp33722");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1051), "Cp1051");
    ccsidToJavaEncodingTable__.put (Integer.valueOf(13488), "UnicodeBigUnmarked");
// GemStone changes BEGIN
    ccsidToJavaEncodingTable__.put (Integer.valueOf(1047), "Cp1047");
// GemStone changes END

  }

  /*
  public static int getCCSID (String javaEncoding) throws java.io.UnsupportedEncodingException
  {
    int ccsid = ((Integer) javaEncodingToCCSIDTable__.get (javaEncoding)).intValue();
    if (ccsid == 0)
      throw new java.io.UnsupportedEncodingException ("unsupported java encoding");
    else
      return ccsid;
  }
  */

  public static String getJavaEncoding (int ccsid) throws java.io.UnsupportedEncodingException
  {
// GemStone changes BEGIN
    String javaEncoding = (String)ccsidToJavaEncodingTable__.get(
        Integer.valueOf(ccsid));
    // changed to use Integer.valueOf()
    /* (original code)
    String javaEncoding = (String) ccsidToJavaEncodingTable__.get (Integer.valueOf(ccsid));
    */
// GemStone changes END
    if (javaEncoding == null)
      throw new java.io.UnsupportedEncodingException ("unsupported ccsid");
    else
      return javaEncoding;
  }
}
