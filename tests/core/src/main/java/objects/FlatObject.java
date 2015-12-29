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
package objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;

/**
 * Object modified here only to implement the ConfigurableObject interface.
 */
public class FlatObject implements ConfigurableObject, DataSerializable
{
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(FlatObject.class, (byte) 11) {
      public DataSerializable newInstance() {
        return new FlatObject();
      }
    });
  }
  private String strfield0;
  private String strfield1;
  private String strfield2;
  private String strfield3;
  private String strfield4;
  private String strfield5;
  private String strfield6;
  private String strfield7;
  private String strfield8;
  private String strfield9;
  private String strfield10;
  private String strfield11;
  private String strfield12;
  private String strfield13;
  private String strfield14;
  private String strfield15;
  private String strfield16;
  private String strfield17;
  private String strfield18;
  private String strfield19;
  private String strfield20;
  private String strfield21;
  private String strfield22;
  private String strfield23;
  private String strfield24;
  private String strfield25;
  private String strfield26;
  private String strfield27;
  private String strfield28;
  private String strfield29;
  private String strfield30;
  private String strfield31;
  private String strfield32;
  private String strfield33;
  private String strfield34;
  private String strfield35;
  private String strfield36;
  private String strfield37;
  private String strfield38;
  private String strfield39;
  private String strfield40;
  private String strfield41;
  private String strfield42;
  private String strfield43;
  private String strfield44;
  private String strfield45;
  private String strfield46;
  private String strfield47;
  private String strfield48;
  private String strfield49;
  private String strfield50;
  private String strfield51;
  private String strfield52;
  private String strfield53;
  private String strfield54;
  private String strfield55;
  private String strfield56;
  private String strfield57;
  private String strfield58;
  private String strfield59;
  private String strfield60;
  private String strfield61;
  private String strfield62;
  private String strfield63;
  private String strfield64;
  private String strfield65;
  private String strfield66;
  private String strfield67;
  private String strfield68;
  private String strfield69;
  private String strfield70;
  private String strfield71;
  private String strfield72;
  private String strfield73;
  private String strfield74;
  private String strfield75;
  private String strfield76;
  private String strfield77;
  private String strfield78;
  private String strfield79;
  private String strfield80;
  private String strfield81;
  private String strfield82;
  private String strfield83;
  private String strfield84;
  private String strfield85;
  private String strfield86;
  private String strfield87;
  private String strfield88;
  private String strfield89;
  private String strfield90;
  private String strfield91;
  private String strfield92;
  private String strfield93;
  private String strfield94;
  private String strfield95;
  private String strfield96;
  private String strfield97;
  private String strfield98;
  private String strfield99;
  private String strfield100;
  private String strfield101;
  private String strfield102;
  private String strfield103;
  private String strfield104;
  private String strfield105;
  private String strfield106;
  private String strfield107;
  private String strfield108;
  private String strfield109;
  private String strfield110;
  private String strfield111;
  private String strfield112;
  private String strfield113;
  private String strfield114;
  private String strfield115;
  private String strfield116;
  private String strfield117;
  private String strfield118;
  private String strfield119;
  private String strfield120;
  private String strfield121;
  private String strfield122;
  private String strfield123;
  private String strfield124;
  private String strfield125;
  private String strfield126;
  private String strfield127;
  private String strfield128;
  private String strfield129;
  private String strfield130;
  private String strfield131;
  private String strfield132;
  private String strfield133;
  private String strfield134;
  private String strfield135;
  private String strfield136;
  private String strfield137;
  private String strfield138;
  private String strfield139;
  private String strfield140;
  private String strfield141;
  private String strfield142;
  private String strfield143;
  private String strfield144;
  private String strfield145;
  private String strfield146;
  private String strfield147;
  private String strfield148;
  private String strfield149;
  private String strfield150;
  private String strfield151;
  private String strfield152;
  private String strfield153;
  private String strfield154;
  private String strfield155;
  private String strfield156;
  private String strfield157;
  private String strfield158;
  private String strfield159;
  private String strfield160;
  private String strfield161;
  private String strfield162;
  private String strfield163;
  private String strfield164;
  private String strfield165;
  private String strfield166;
  private String strfield167;
  private String strfield168;
  private String strfield169;
  private String strfield170;
  private String strfield171;
  private String strfield172;
  private String strfield173;
  private String strfield174;
  private String strfield175;
  private String strfield176;
  private String strfield177;
  private String strfield178;
  private String strfield179;
  private int id;
  private int intfield1;
  private int intfield2;
  private int intfield3;
  private int intfield4;
  private int intfield5;
  private int intfield6;
  private int intfield7;
  private int intfield8;
  private int intfield9;
  private int intfield10;
  private int intfield11;
  private int intfield12;
  private int intfield13;
  private int intfield14;
  private int intfield15;
  private int intfield16;
  private int intfield17;
  private int intfield18;
  private int intfield19;
  private int intfield20;
  private int intfield21;
  private int intfield22;
  private int intfield23;
  private int intfield24;
  private int intfield25;
  private int intfield26;
  private int intfield27;
  private int intfield28;
  private int intfield29;
  private int intfield30;
  private int intfield31;
  private int intfield32;
  private int intfield33;
  private int intfield34;
  private int intfield35;
  private int intfield36;
  private int intfield37;
  private int intfield38;
  private int intfield39;
  private int intfield40;
  private int intfield41;
  private int intfield42;
  private int intfield43;
  private int intfield44;
  private int intfield45;
  private int intfield46;
  private int intfield47;
  private int intfield48;
  private int intfield49;
  private int intfield50;
  private int intfield51;
  private int intfield52;
  private int intfield53;
  private int intfield54;
  private int intfield55;
  private int intfield56;
  private int intfield57;
  private int intfield58;
  private int intfield59;
  private int intfield60;
  private int intfield61;
  private int intfield62;
  private int intfield63;
  private int intfield64;
  private int intfield65;
  private int intfield66;
  private int intfield67;
  private int intfield68;
  private int intfield69;
  private int intfield70;
  private int intfield71;
  private int intfield72;
  private int intfield73;
  private int intfield74;
  private int intfield75;
  private int intfield76;
  private int intfield77;
  private int intfield78;
  private int intfield79;
  private int intfield80;
  private int intfield81;
  private int intfield82;
  private int intfield83;
  private int intfield84;
  private int intfield85;
  private int intfield86;
  private int intfield87;
  private int intfield88;
  private int intfield89;
  private int intfield90;
  private int intfield91;
  private int intfield92;
  private int intfield93;
  private int intfield94;
  private int intfield95;
  private int intfield96;
  private int intfield97;
  private int intfield98;
  private int intfield99;
  private int intfield100;
  private int intfield101;
  private int intfield102;
  private int intfield103;
  private int intfield104;
  private int intfield105;
  private int intfield106;
  private int intfield107;
  private int intfield108;
  private int intfield109;
  private int intfield110;
  private int intfield111;
  private int intfield112;
  private int intfield113;
  private int intfield114;
  private int intfield115;
  private int intfield116;
  private int intfield117;
  private int intfield118;
  private int intfield119;
  private int intfield120;
  private int intfield121;
  private int intfield122;
  private int intfield123;
  private int intfield124;
  private int intfield125;
  private int intfield126;
  private int intfield127;
  private int intfield128;
  private int intfield129;
  private int intfield130;
  private int intfield131;
  private int intfield132;
  private int intfield133;
  private int intfield134;
  private int intfield135;
  private int intfield136;
  private int intfield137;
  private int intfield138;
  private int intfield139;
  private int intfield140;
  private int intfield141;
  private int intfield142;
  private int intfield143;
  private int intfield144;
  private int intfield145;
  private int intfield146;
  private int intfield147;
  private int intfield148;
  private int intfield149;
  private int intfield150;
  private int intfield151;
  private int intfield152;
  private int intfield153;
  private int intfield154;
  private int intfield155;
  private int intfield156;
  private int intfield157;
  private int intfield158;
  private int intfield159;
  private int intfield160;
  private int intfield161;
  private int intfield162;
  private int intfield163;
  private int intfield164;
  private int intfield165;
  private int intfield166;
  private int intfield167;
  private int intfield168;
  private int intfield169;
  private int intfield170;
  private int intfield171;
  private int intfield172;
  private int intfield173;
  private int intfield174;
  private int intfield175;
  private int intfield176;
  private double doublefield0;
  private double doublefield1;
  private double doublefield2;
  private double doublefield3;
  private double doublefield4;
  private double doublefield5;
  private double doublefield6;
  private double doublefield7;
  private double doublefield8;
  private double doublefield9;
  private double doublefield10;
  private double doublefield11;
  private double doublefield12;
  private double doublefield13;
  private double doublefield14;
  private double doublefield15;
  private double doublefield16;
  private double doublefield17;
  private double doublefield18;
  private double doublefield19;
  private double doublefield20;
  private double doublefield21;
  private double doublefield22;
  private double doublefield23;
  private double doublefield24;
  private double doublefield25;
  private double doublefield26;
  private double doublefield27;
  private double doublefield28;
  private double doublefield29;
  private double doublefield30;
  private double doublefield31;
  private double doublefield32;
  private double doublefield33;
  private double doublefield34;
  private double doublefield35;
  private double doublefield36;
  private double doublefield37;
  private double doublefield38;
  private double doublefield39;
  private double doublefield40;
  private double doublefield41;
  private double doublefield42;
  private double doublefield43;
  private double doublefield44;
  private double doublefield45;
  private double doublefield46;
  private double doublefield47;
  private double doublefield48;
  private double doublefield49;
  private double doublefield50;
  private double doublefield51;
  private double doublefield52;
  private double doublefield53;
  private double doublefield54;
  private double doublefield55;
  private double doublefield56;
  private double doublefield57;
  private double doublefield58;
  private double doublefield59;
  private double doublefield60;
  private double doublefield61;
  private double doublefield62;
  private double doublefield63;
  private double doublefield64;
  private double doublefield65;
  private double doublefield66;
  private double doublefield67;
  private double doublefield68;
  private double doublefield69;
  private double doublefield70;
  private double doublefield71;
  private double doublefield72;
  private double doublefield73;
  private double doublefield74;
  private double doublefield75;
  private double doublefield76;
  private double doublefield77;
  private double doublefield78;
  private double doublefield79;
  private double doublefield80;
  private double doublefield81;
  private double doublefield82;
  private double doublefield83;
  private double doublefield84;
  private double doublefield85;
  private double doublefield86;
  private double doublefield87;
  private double doublefield88;
  private double doublefield89;
  private double doublefield90;
  private double doublefield91;
  private double doublefield92;
  private double doublefield93;
  private double doublefield94;
  private double doublefield95;
  private double doublefield96;
  private double doublefield97;
  private double doublefield98;
  private double doublefield99;
  private double doublefield100;
  private double doublefield101;
  private double doublefield102;
  private double doublefield103;
  private double doublefield104;
  private double doublefield105;
  private double doublefield106;
  private double doublefield107;
  private double doublefield108;
  private double doublefield109;
  private double doublefield110;
  private double doublefield111;
  private double doublefield112;
  private double doublefield113;
  private double doublefield114;
  private double doublefield115;
  private double doublefield116;
  private double doublefield117;
  private double doublefield118;
  private double doublefield119;
  private double doublefield120;
  private double doublefield121;
  private double doublefield122;
  private double doublefield123;
  private double doublefield124;
  private double doublefield125;
  private double doublefield126;
  private double doublefield127;
  private double doublefield128;
  private double doublefield129;
  private double doublefield130;
  private double doublefield131;
  private double doublefield132;
  private double doublefield133;
  private double doublefield134;
  private double doublefield135;
  private double doublefield136;
  private double doublefield137;
  private double doublefield138;
  private double doublefield139;
  private double doublefield140;
  private double doublefield141;
  private double doublefield142;
  private double doublefield143;
  private double doublefield144;
  private double doublefield145;
  private double doublefield146;
  private double doublefield147;
  private double doublefield148;
  private double doublefield149;
  private double doublefield150;
  private double doublefield151;
  private double doublefield152;
  private double doublefield153;
  private double doublefield154;
  private double doublefield155;
  private double doublefield156;
  private double doublefield157;
  private double doublefield158;
  private double doublefield159;
  private byte bytefield0;
  private byte bytefield1;
  private byte bytefield2;
  private byte bytefield3;
  private byte bytefield4;
  private byte bytefield5;
  private byte bytefield6;
  private byte bytefield7;
  private byte bytefield8;
  private byte bytefield9;
  private byte bytefield10;
  private byte bytefield11;
  private byte bytefield12;
  private byte bytefield13;
  private byte bytefield14;
  private byte bytefield15;
  private byte bytefield16;
  private byte bytefield17;
  private byte bytefield18;
  private byte bytefield19;
  private byte bytefield20;
  private byte bytefield21;
  private byte bytefield22;
  private byte bytefield23;
  private byte bytefield24;
  private byte bytefield25;
  private byte bytefield26;
  private byte bytefield27;
  private byte bytefield28;
  private byte bytefield29;
  private byte bytefield30;
  private byte bytefield31;
  private byte bytefield32;
  private byte bytefield33;
  private byte bytefield34;
  private byte bytefield35;
  private byte bytefield36;
  private byte bytefield37;
  private byte bytefield38;
  private byte bytefield39;
  private byte bytefield40;
  private byte bytefield41;
  private byte bytefield42;
  private byte bytefield43;
  private byte bytefield44;
  private byte bytefield45;
  private byte bytefield46;
  private byte bytefield47;
  private byte bytefield48;
  private byte bytefield49;
  private byte bytefield50;
  private byte bytefield51;
  private byte bytefield52;
  private byte bytefield53;
  private byte bytefield54;
  private byte bytefield55;
  private byte bytefield56;
  private byte bytefield57;
  private byte bytefield58;
  private byte bytefield59;
  private byte bytefield60;
  private byte bytefield61;
  private byte bytefield62;
  private byte bytefield63;
  private byte bytefield64;
  private byte bytefield65;
  private byte bytefield66;
  private byte bytefield67;
  private byte bytefield68;
  private byte bytefield69;
  private byte bytefield70;
  private byte bytefield71;
  private byte bytefield72;
  private byte bytefield73;
  private byte bytefield74;
  private byte bytefield75;
  private byte bytefield76;
  private byte bytefield77;
  private byte bytefield78;
  private byte bytefield79;
  private byte bytefield80;
  private byte bytefield81;
  private byte bytefield82;
  private byte bytefield83;
  private byte bytefield84;
  private byte bytefield85;
  private byte bytefield86;
  private byte bytefield87;
  private byte bytefield88;
  private byte bytefield89;
  private byte bytefield90;
  private byte bytefield91;
  private byte bytefield92;
  private byte bytefield93;
  private byte bytefield94;
  private byte bytefield95;
  private byte bytefield96;
  private byte bytefield97;
  private byte bytefield98;
  private byte bytefield99;
  private byte bytefield100;
  private byte bytefield101;
  private byte bytefield102;
  private byte bytefield103;
  private byte bytefield104;
  private byte bytefield105;
  private byte bytefield106;
  private byte bytefield107;
  private byte bytefield108;
  private byte bytefield109;
  private byte bytefield110;
  private byte bytefield111;
  private byte bytefield112;
  private byte bytefield113;
  private byte bytefield114;
  private byte bytefield115;
  private byte bytefield116;
  private byte bytefield117;
  private byte bytefield118;
  private byte bytefield119;
  private byte bytefield120;
  private byte bytefield121;
  private byte bytefield122;
  private byte bytefield123;
  private byte bytefield124;
  private byte bytefield125;
  private byte bytefield126;
  private byte bytefield127;
  private byte bytefield128;
  private byte bytefield129;
  private byte bytefield130;
  private byte bytefield131;
  private byte bytefield132;
  private byte bytefield133;
  private byte bytefield134;
  private byte bytefield135;
  private byte bytefield136;
  private byte bytefield137;
  private byte bytefield138;
  private byte bytefield139;
  private byte bytefield140;
  private byte bytefield141;
  private byte bytefield142;
  private byte bytefield143;
  private byte bytefield144;
  private byte bytefield145;
  private byte bytefield146;
  private byte bytefield147;
  private byte bytefield148;
  private byte bytefield149;
  private char charfield0;
  private char charfield1;
  private char charfield2;
  private char charfield3;
  private char charfield4;
  private char charfield5;
  private char charfield6;
  private char charfield7;
  private char charfield8;
  private char charfield9;
  private char charfield10;
  private char charfield11;
  private char charfield12;
  private char charfield13;
  private char charfield14;
  private char charfield15;
  private char charfield16;
  private char charfield17;
  private char charfield18;
  private char charfield19;
  private char charfield20;
  private char charfield21;
  private char charfield22;
  private char charfield23;
  private char charfield24;
  private char charfield25;
  private char charfield26;
  private char charfield27;
  private char charfield28;
  private char charfield29;
  private char charfield30;
  private char charfield31;
  private char charfield32;
  private char charfield33;
  private char charfield34;
  private char charfield35;
  private char charfield36;
  private char charfield37;
  private char charfield38;
  private char charfield39;
  private char charfield40;
  private char charfield41;
  private char charfield42;
  private char charfield43;
  private char charfield44;
  private char charfield45;
  private char charfield46;
  private char charfield47;
  private char charfield48;
  private char charfield49;
  private char charfield50;
  private char charfield51;
  private char charfield52;
  private char charfield53;
  private char charfield54;
  private char charfield55;
  private char charfield56;
  private char charfield57;
  private char charfield58;
  private char charfield59;
  private char charfield60;
  private char charfield61;
  private char charfield62;
  private char charfield63;
  private char charfield64;
  private char charfield65;
  private char charfield66;
  private char charfield67;
  private char charfield68;
  private char charfield69;
  private char charfield70;
  private char charfield71;
  private char charfield72;
  private char charfield73;
  private char charfield74;
  private char charfield75;
  private char charfield76;
  private char charfield77;
  private char charfield78;
  private char charfield79;
  private char charfield80;
  private char charfield81;
  private char charfield82;
  private char charfield83;
  private char charfield84;
  private char charfield85;
  private char charfield86;
  private char charfield87;
  private char charfield88;
  private char charfield89;
  private char charfield90;
  private char charfield91;
  private char charfield92;
  private char charfield93;
  private char charfield94;
  private char charfield95;
  private char charfield96;
  private char charfield97;
  private char charfield98;
  private char charfield99;
  private char charfield100;
  private char charfield101;
  private char charfield102;
  private char charfield103;
  private char charfield104;
  private char charfield105;
  private char charfield106;
  private char charfield107;
  private char charfield108;
  private char charfield109;
  private char charfield110;
  private char charfield111;
  private char charfield112;
  private char charfield113;
  private char charfield114;
  private char charfield115;
  private char charfield116;
  private char charfield117;
  private char charfield118;
  private char charfield119;
  private char charfield120;
  private char charfield121;
  private char charfield122;
  private char charfield123;
  private char charfield124;
  private char charfield125;
  private char charfield126;
  private char charfield127;
  private char charfield128;
  private char charfield129;
  private char charfield130;
  private char charfield131;
  private char charfield132;
  private char charfield133;
  private char charfield134;
  private char charfield135;
  private char charfield136;
  private char charfield137;
  private char charfield138;
  private char charfield139;
  private char charfield140;
  private char charfield141;
  private char charfield142;
  private char charfield143;
  private char charfield144;
  private char charfield145;
  private char charfield146;
  private char charfield147;
  private char charfield148;
  private char charfield149;
  private boolean booleanfield0;
  private boolean booleanfield1;
  private boolean booleanfield2;
  private boolean booleanfield3;
  private boolean booleanfield4;
  private boolean booleanfield5;
  private boolean booleanfield6;
  private boolean booleanfield7;
  private boolean booleanfield8;
  private boolean booleanfield9;
  private boolean booleanfield10;
  private boolean booleanfield11;
  private boolean booleanfield12;
  private boolean booleanfield13;
  private boolean booleanfield14;
  private boolean booleanfield15;
  private boolean booleanfield16;
  private boolean booleanfield17;
  private boolean booleanfield18;
  private boolean booleanfield19;
  private boolean booleanfield20;
  private boolean booleanfield21;
  private boolean booleanfield22;
  private boolean booleanfield23;
  private boolean booleanfield24;
  private boolean booleanfield25;
  private boolean booleanfield26;
  private boolean booleanfield27;
  private boolean booleanfield28;
  private boolean booleanfield29;
  private boolean booleanfield30;
  private boolean booleanfield31;
  private boolean booleanfield32;
  private boolean booleanfield33;
  private boolean booleanfield34;
  private boolean booleanfield35;
  private boolean booleanfield36;
  private boolean booleanfield37;
  private boolean booleanfield38;
  private boolean booleanfield39;
  private boolean booleanfield40;
  private boolean booleanfield41;
  private boolean booleanfield42;
  private boolean booleanfield43;
  private boolean booleanfield44;
  private boolean booleanfield45;
  private boolean booleanfield46;
  private boolean booleanfield47;
  private boolean booleanfield48;
  private boolean booleanfield49;
  private boolean booleanfield50;
  private boolean booleanfield51;
  private boolean booleanfield52;
  private boolean booleanfield53;
  private boolean booleanfield54;
  private boolean booleanfield55;
  private boolean booleanfield56;
  private boolean booleanfield57;
  private boolean booleanfield58;
  private boolean booleanfield59;
  private boolean booleanfield60;
  private boolean booleanfield61;
  private boolean booleanfield62;
  private boolean booleanfield63;
  private boolean booleanfield64;
  private boolean booleanfield65;
  private boolean booleanfield66;
  private boolean booleanfield67;
  private boolean booleanfield68;
  private boolean booleanfield69;
  private boolean booleanfield70;
  private boolean booleanfield71;
  private boolean booleanfield72;
  private boolean booleanfield73;
  private boolean booleanfield74;
  private boolean booleanfield75;
  private boolean booleanfield76;
  private boolean booleanfield77;
  private boolean booleanfield78;
  private boolean booleanfield79;
  private boolean booleanfield80;
  private boolean booleanfield81;
  private boolean booleanfield82;
  private boolean booleanfield83;
  private boolean booleanfield84;
  private boolean booleanfield85;
  private boolean booleanfield86;
  private boolean booleanfield87;
  private boolean booleanfield88;
  private boolean booleanfield89;
  private boolean booleanfield90;
  private boolean booleanfield91;
  private boolean booleanfield92;
  private boolean booleanfield93;
  private boolean booleanfield94;
  private boolean booleanfield95;
  private boolean booleanfield96;
  private boolean booleanfield97;
  private boolean booleanfield98;
  private boolean booleanfield99;
  private boolean booleanfield100;
  private boolean booleanfield101;
  private boolean booleanfield102;
  private boolean booleanfield103;
  private boolean booleanfield104;
  private boolean booleanfield105;
  private boolean booleanfield106;
  private boolean booleanfield107;
  private boolean booleanfield108;
  private boolean booleanfield109;
  private boolean booleanfield110;
  private boolean booleanfield111;
  private boolean booleanfield112;
  private boolean booleanfield113;
  private boolean booleanfield114;
  private boolean booleanfield115;
  private boolean booleanfield116;
  private boolean booleanfield117;
  private boolean booleanfield118;
  private boolean booleanfield119;
  private boolean booleanfield120;
  private boolean booleanfield121;
  private boolean booleanfield122;
  private boolean booleanfield123;
  private boolean booleanfield124;
  private boolean booleanfield125;
  private boolean booleanfield126;
  private boolean booleanfield127;
  private boolean booleanfield128;
  private boolean booleanfield129;
  private boolean booleanfield130;
  private boolean booleanfield131;
  private boolean booleanfield132;
  private boolean booleanfield133;
  private boolean booleanfield134;
  private boolean booleanfield135;
  private boolean booleanfield136;
  private boolean booleanfield137;
  private boolean booleanfield138;
  private boolean booleanfield139;
  private boolean booleanfield140;
  private boolean booleanfield141;
  private boolean booleanfield142;
  private boolean booleanfield143;
  private boolean booleanfield144;
  private boolean booleanfield145;
  private boolean booleanfield146;
  private boolean booleanfield147;
  private boolean booleanfield148;
  private boolean booleanfield149;
  private float floatfield0;
  private float floatfield1;
  private float floatfield2;
  private float floatfield3;
  private float floatfield4;
  private float floatfield5;
  private float floatfield6;
  private float floatfield7;
  private float floatfield8;
  private float floatfield9;
  private float floatfield10;
  private float floatfield11;
  private float floatfield12;
  private float floatfield13;
  private float floatfield14;
  private float floatfield15;
  private float floatfield16;
  private float floatfield17;
  private float floatfield18;
  private float floatfield19;
  private float floatfield20;
  private float floatfield21;
  private float floatfield22;
  private float floatfield23;
  private float floatfield24;
  private float floatfield25;
  private float floatfield26;
  private float floatfield27;
  private float floatfield28;
  private float floatfield29;
  private float floatfield30;
  private float floatfield31;
  private float floatfield32;
  private float floatfield33;
  private float floatfield34;
  private float floatfield35;
  private float floatfield36;
  private float floatfield37;
  private float floatfield38;
  private float floatfield39;
  private float floatfield40;
  private float floatfield41;
  private float floatfield42;
  private float floatfield43;
  private float floatfield44;
  private float floatfield45;
  private float floatfield46;
  private float floatfield47;
  private float floatfield48;
  private float floatfield49;
  private float floatfield50;
  private float floatfield51;
  private float floatfield52;
  private float floatfield53;
  private float floatfield54;
  private float floatfield55;
  private float floatfield56;
  private float floatfield57;
  private float floatfield58;
  private float floatfield59;
  private float floatfield60;
  private float floatfield61;
  private float floatfield62;
  private float floatfield63;
  private float floatfield64;
  private float floatfield65;
  private float floatfield66;
  private float floatfield67;
  private float floatfield68;
  private float floatfield69;
  private float floatfield70;
  private float floatfield71;
  private float floatfield72;
  private float floatfield73;
  private float floatfield74;
  private float floatfield75;
  private float floatfield76;
  private float floatfield77;
  private float floatfield78;
  private float floatfield79;
  private float floatfield80;
  private float floatfield81;
  private float floatfield82;
  private float floatfield83;
  private float floatfield84;
  private float floatfield85;
  private float floatfield86;
  private float floatfield87;
  private float floatfield88;
  private float floatfield89;
  private float floatfield90;
  private float floatfield91;
  private float floatfield92;
  private float floatfield93;
  private float floatfield94;
  private float floatfield95;
  private float floatfield96;
  private float floatfield97;
  private float floatfield98;
  private float floatfield99;
  private float floatfield100;
  private float floatfield101;
  private float floatfield102;
  private float floatfield103;
  private float floatfield104;
  private float floatfield105;
  private float floatfield106;
  private float floatfield107;
  private float floatfield108;
  private float floatfield109;
  private float floatfield110;
  private float floatfield111;
  private float floatfield112;
  private float floatfield113;
  private float floatfield114;
  private float floatfield115;
  private float floatfield116;
  private float floatfield117;
  private float floatfield118;
  private float floatfield119;
  private float floatfield120;
  private float floatfield121;
  private float floatfield122;
  private float floatfield123;
  private float floatfield124;
  private float floatfield125;
  private float floatfield126;
  private float floatfield127;
  private float floatfield128;
  private float floatfield129;
  private float floatfield130;
  private float floatfield131;
  private float floatfield132;
  private float floatfield133;
  private float floatfield134;
  private float floatfield135;
  private float floatfield136;
  private float floatfield137;
  private float floatfield138;
  private float floatfield139;
  private float floatfield140;
  private float floatfield141;
  private float floatfield142;
  private float floatfield143;
  private float floatfield144;
  private float floatfield145;
  private float floatfield146;
  private float floatfield147;
  private float floatfield148;
  private float floatfield149;
  private long longfield0;
  private long longfield1;
  private long longfield2;
  private long longfield3;
  private long longfield4;
  private long longfield5;
  private long longfield6;
  private long longfield7;
  private long longfield8;
  private long longfield9;
  private long longfield10;
  private long longfield11;
  private long longfield12;
  private long longfield13;
  private long longfield14;
  private long longfield15;
  private long longfield16;
  private long longfield17;
  private long longfield18;
  private long longfield19;
  private long longfield20;
  private long longfield21;
  private long longfield22;
  private long longfield23;
  private long longfield24;
  private long longfield25;
  private long longfield26;
  private long longfield27;
  private long longfield28;
  private long longfield29;
  private long longfield30;
  private long longfield31;
  private long longfield32;
  private long longfield33;
  private long longfield34;
  private long longfield35;
  private long longfield36;
  private long longfield37;
  private long longfield38;
  private long longfield39;
  private long longfield40;
  private long longfield41;
  private long longfield42;
  private long longfield43;
  private long longfield44;
  private long longfield45;
  private long longfield46;
  private long longfield47;
  private long longfield48;
  private long longfield49;
  private long longfield50;
  private long longfield51;
  private long longfield52;
  private long longfield53;
  private long longfield54;
  private long longfield55;
  private long longfield56;
  private long longfield57;
  private long longfield58;
  private long longfield59;
  private long longfield60;
  private long longfield61;
  private long longfield62;
  private long longfield63;
  private long longfield64;
  private long longfield65;
  private long longfield66;
  private long longfield67;
  private long longfield68;
  private long longfield69;
  private long longfield70;
  private long longfield71;
  private long longfield72;
  private long longfield73;
  private long longfield74;
  private long longfield75;
  private long longfield76;
  private long longfield77;
  private long longfield78;
  private long longfield79;
  private long longfield80;
  private long longfield81;
  private long longfield82;
  private long longfield83;
  private long longfield84;
  private long longfield85;
  private long longfield86;
  private long longfield87;
  private long longfield88;
  private long longfield89;
  private long longfield90;
  private long longfield91;
  private long longfield92;
  private long longfield93;
  private long longfield94;
  private long longfield95;
  private long longfield96;
  private long longfield97;
  private long longfield98;
  private long longfield99;
  private long longfield100;
  private long longfield101;
  private long longfield102;
  private long longfield103;
  private long longfield104;
  private long longfield105;
  private long longfield106;
  private long longfield107;
  private long longfield108;
  private long longfield109;
  private long longfield110;
  private long longfield111;
  private long longfield112;
  private long longfield113;
  private long longfield114;
  private long longfield115;
  private long longfield116;
  private long longfield117;
  private long longfield118;
  private long longfield119;
  private long longfield120;
  private long longfield121;
  private long longfield122;
  private long longfield123;
  private long longfield124;
  private long longfield125;
  private long longfield126;
  private long longfield127;
  private long longfield128;
  private long longfield129;
  private long longfield130;
  private long longfield131;
  private long longfield132;
  private long longfield133;
  private long longfield134;
  private long longfield135;
  private long longfield136;
  private long longfield137;
  private long longfield138;
  private long longfield139;
  private long longfield140;
  private long longfield141;
  private long longfield142;
  private long longfield143;
  private long longfield144;
  private long longfield145;
  private long longfield146;
  private long longfield147;
  private long longfield148;
  private long longfield149;
  private short shortfield0;
  private short shortfield1;
  private short shortfield2;
  private short shortfield3;
  private short shortfield4;
  private short shortfield5;
  private short shortfield6;
  private short shortfield7;
  private short shortfield8;
  private short shortfield9;
  private short shortfield10;
  private short shortfield11;
  private short shortfield12;
  private short shortfield13;
  private short shortfield14;
  private short shortfield15;
  private short shortfield16;
  private short shortfield17;
  private short shortfield18;
  private short shortfield19;
  private short shortfield20;
  private short shortfield21;
  private short shortfield22;
  private short shortfield23;
  private short shortfield24;
  private short shortfield25;
  private short shortfield26;
  private short shortfield27;
  private short shortfield28;
  private short shortfield29;
  private short shortfield30;
  private short shortfield31;
  private short shortfield32;
  private short shortfield33;
  private short shortfield34;
  private short shortfield35;
  private short shortfield36;
  private short shortfield37;
  private short shortfield38;
  private short shortfield39;
  private short shortfield40;
  private short shortfield41;
  private short shortfield42;
  private short shortfield43;
  private short shortfield44;
  private short shortfield45;
  private short shortfield46;
  private short shortfield47;
  private short shortfield48;
  private short shortfield49;
  private short shortfield50;
  private short shortfield51;
  private short shortfield52;
  private short shortfield53;
  private short shortfield54;
  private short shortfield55;
  private short shortfield56;
  private short shortfield57;
  private short shortfield58;
  private short shortfield59;
  private short shortfield60;
  private short shortfield61;
  private short shortfield62;
  private short shortfield63;
  private short shortfield64;
  private short shortfield65;
  private short shortfield66;
  private short shortfield67;
  private short shortfield68;
  private short shortfield69;
  private short shortfield70;
  private short shortfield71;
  private short shortfield72;
  private short shortfield73;
  private short shortfield74;
  private short shortfield75;
  private short shortfield76;
  private short shortfield77;
  private short shortfield78;
  private short shortfield79;
  private short shortfield80;
  private short shortfield81;
  private short shortfield82;
  private short shortfield83;
  private short shortfield84;
  private short shortfield85;
  private short shortfield86;
  private short shortfield87;
  private short shortfield88;
  private short shortfield89;
  private short shortfield90;
  private short shortfield91;
  private short shortfield92;
  private short shortfield93;
  private short shortfield94;
  private short shortfield95;
  private short shortfield96;
  private short shortfield97;
  private short shortfield98;
  private short shortfield99;
  private short shortfield100;
  private short shortfield101;
  private short shortfield102;
  private short shortfield103;
  private short shortfield104;
  private short shortfield105;
  private short shortfield106;
  private short shortfield107;
  private short shortfield108;
  private short shortfield109;
  private short shortfield110;
  private short shortfield111;
  private short shortfield112;
  private short shortfield113;
  private short shortfield114;
  private short shortfield115;
  private short shortfield116;
  private short shortfield117;
  private short shortfield118;
  private short shortfield119;
  private short shortfield120;
  private short shortfield121;
  private short shortfield122;
  private short shortfield123;
  private short shortfield124;
  private short shortfield125;
  private short shortfield126;
  private short shortfield127;
  private short shortfield128;
  private short shortfield129;
  private short shortfield130;
  private short shortfield131;
  private short shortfield132;
  private short shortfield133;
  private short shortfield134;
  private short shortfield135;
  private short shortfield136;
  private short shortfield137;
  private short shortfield138;
  private short shortfield139;
  private short shortfield140;
  private short shortfield141;
  private short shortfield142;
  private short shortfield143;
  private short shortfield144;
  private short shortfield145;
  private short shortfield146;
  private short shortfield147;
  private short shortfield148;
  private short shortfield149;
  public String getStrfield0(){
  return strfield0;
  }
  public void setStrfield0(String value){
  this.strfield0 = value;
  }
  public String getStrfield1(){
  return strfield1;
  }
  public void setStrfield1(String value){
  this.strfield1 = value;
  }
  public String getStrfield2(){
  return strfield2;
  }
  public void setStrfield2(String value){
  this.strfield2 = value;
  }
  public String getStrfield3(){
  return strfield3;
  }
  public void setStrfield3(String value){
  this.strfield3 = value;
  }
  public String getStrfield4(){
  return strfield4;
  }
  public void setStrfield4(String value){
  this.strfield4 = value;
  }
  public String getStrfield5(){
  return strfield5;
  }
  public void setStrfield5(String value){
  this.strfield5 = value;
  }
  public String getStrfield6(){
  return strfield6;
  }
  public void setStrfield6(String value){
  this.strfield6 = value;
  }
  public String getStrfield7(){
  return strfield7;
  }
  public void setStrfield7(String value){
  this.strfield7 = value;
  }
  public String getStrfield8(){
  return strfield8;
  }
  public void setStrfield8(String value){
  this.strfield8 = value;
  }
  public String getStrfield9(){
  return strfield9;
  }
  public void setStrfield9(String value){
  this.strfield9 = value;
  }
  public String getStrfield10(){
  return strfield10;
  }
  public void setStrfield10(String value){
  this.strfield10 = value;
  }
  public String getStrfield11(){
  return strfield11;
  }
  public void setStrfield11(String value){
  this.strfield11 = value;
  }
  public String getStrfield12(){
  return strfield12;
  }
  public void setStrfield12(String value){
  this.strfield12 = value;
  }
  public String getStrfield13(){
  return strfield13;
  }
  public void setStrfield13(String value){
  this.strfield13 = value;
  }
  public String getStrfield14(){
  return strfield14;
  }
  public void setStrfield14(String value){
  this.strfield14 = value;
  }
  public String getStrfield15(){
  return strfield15;
  }
  public void setStrfield15(String value){
  this.strfield15 = value;
  }
  public String getStrfield16(){
  return strfield16;
  }
  public void setStrfield16(String value){
  this.strfield16 = value;
  }
  public String getStrfield17(){
  return strfield17;
  }
  public void setStrfield17(String value){
  this.strfield17 = value;
  }
  public String getStrfield18(){
  return strfield18;
  }
  public void setStrfield18(String value){
  this.strfield18 = value;
  }
  public String getStrfield19(){
  return strfield19;
  }
  public void setStrfield19(String value){
  this.strfield19 = value;
  }
  public String getStrfield20(){
  return strfield20;
  }
  public void setStrfield20(String value){
  this.strfield20 = value;
  }
  public String getStrfield21(){
  return strfield21;
  }
  public void setStrfield21(String value){
  this.strfield21 = value;
  }
  public String getStrfield22(){
  return strfield22;
  }
  public void setStrfield22(String value){
  this.strfield22 = value;
  }
  public String getStrfield23(){
  return strfield23;
  }
  public void setStrfield23(String value){
  this.strfield23 = value;
  }
  public String getStrfield24(){
  return strfield24;
  }
  public void setStrfield24(String value){
  this.strfield24 = value;
  }
  public String getStrfield25(){
  return strfield25;
  }
  public void setStrfield25(String value){
  this.strfield25 = value;
  }
  public String getStrfield26(){
  return strfield26;
  }
  public void setStrfield26(String value){
  this.strfield26 = value;
  }
  public String getStrfield27(){
  return strfield27;
  }
  public void setStrfield27(String value){
  this.strfield27 = value;
  }
  public String getStrfield28(){
  return strfield28;
  }
  public void setStrfield28(String value){
  this.strfield28 = value;
  }
  public String getStrfield29(){
  return strfield29;
  }
  public void setStrfield29(String value){
  this.strfield29 = value;
  }
  public String getStrfield30(){
  return strfield30;
  }
  public void setStrfield30(String value){
  this.strfield30 = value;
  }
  public String getStrfield31(){
  return strfield31;
  }
  public void setStrfield31(String value){
  this.strfield31 = value;
  }
  public String getStrfield32(){
  return strfield32;
  }
  public void setStrfield32(String value){
  this.strfield32 = value;
  }
  public String getStrfield33(){
  return strfield33;
  }
  public void setStrfield33(String value){
  this.strfield33 = value;
  }
  public String getStrfield34(){
  return strfield34;
  }
  public void setStrfield34(String value){
  this.strfield34 = value;
  }
  public String getStrfield35(){
  return strfield35;
  }
  public void setStrfield35(String value){
  this.strfield35 = value;
  }
  public String getStrfield36(){
  return strfield36;
  }
  public void setStrfield36(String value){
  this.strfield36 = value;
  }
  public String getStrfield37(){
  return strfield37;
  }
  public void setStrfield37(String value){
  this.strfield37 = value;
  }
  public String getStrfield38(){
  return strfield38;
  }
  public void setStrfield38(String value){
  this.strfield38 = value;
  }
  public String getStrfield39(){
  return strfield39;
  }
  public void setStrfield39(String value){
  this.strfield39 = value;
  }
  public String getStrfield40(){
  return strfield40;
  }
  public void setStrfield40(String value){
  this.strfield40 = value;
  }
  public String getStrfield41(){
  return strfield41;
  }
  public void setStrfield41(String value){
  this.strfield41 = value;
  }
  public String getStrfield42(){
  return strfield42;
  }
  public void setStrfield42(String value){
  this.strfield42 = value;
  }
  public String getStrfield43(){
  return strfield43;
  }
  public void setStrfield43(String value){
  this.strfield43 = value;
  }
  public String getStrfield44(){
  return strfield44;
  }
  public void setStrfield44(String value){
  this.strfield44 = value;
  }
  public String getStrfield45(){
  return strfield45;
  }
  public void setStrfield45(String value){
  this.strfield45 = value;
  }
  public String getStrfield46(){
  return strfield46;
  }
  public void setStrfield46(String value){
  this.strfield46 = value;
  }
  public String getStrfield47(){
  return strfield47;
  }
  public void setStrfield47(String value){
  this.strfield47 = value;
  }
  public String getStrfield48(){
  return strfield48;
  }
  public void setStrfield48(String value){
  this.strfield48 = value;
  }
  public String getStrfield49(){
  return strfield49;
  }
  public void setStrfield49(String value){
  this.strfield49 = value;
  }
  public String getStrfield50(){
  return strfield50;
  }
  public void setStrfield50(String value){
  this.strfield50 = value;
  }
  public String getStrfield51(){
  return strfield51;
  }
  public void setStrfield51(String value){
  this.strfield51 = value;
  }
  public String getStrfield52(){
  return strfield52;
  }
  public void setStrfield52(String value){
  this.strfield52 = value;
  }
  public String getStrfield53(){
  return strfield53;
  }
  public void setStrfield53(String value){
  this.strfield53 = value;
  }
  public String getStrfield54(){
  return strfield54;
  }
  public void setStrfield54(String value){
  this.strfield54 = value;
  }
  public String getStrfield55(){
  return strfield55;
  }
  public void setStrfield55(String value){
  this.strfield55 = value;
  }
  public String getStrfield56(){
  return strfield56;
  }
  public void setStrfield56(String value){
  this.strfield56 = value;
  }
  public String getStrfield57(){
  return strfield57;
  }
  public void setStrfield57(String value){
  this.strfield57 = value;
  }
  public String getStrfield58(){
  return strfield58;
  }
  public void setStrfield58(String value){
  this.strfield58 = value;
  }
  public String getStrfield59(){
  return strfield59;
  }
  public void setStrfield59(String value){
  this.strfield59 = value;
  }
  public String getStrfield60(){
  return strfield60;
  }
  public void setStrfield60(String value){
  this.strfield60 = value;
  }
  public String getStrfield61(){
  return strfield61;
  }
  public void setStrfield61(String value){
  this.strfield61 = value;
  }
  public String getStrfield62(){
  return strfield62;
  }
  public void setStrfield62(String value){
  this.strfield62 = value;
  }
  public String getStrfield63(){
  return strfield63;
  }
  public void setStrfield63(String value){
  this.strfield63 = value;
  }
  public String getStrfield64(){
  return strfield64;
  }
  public void setStrfield64(String value){
  this.strfield64 = value;
  }
  public String getStrfield65(){
  return strfield65;
  }
  public void setStrfield65(String value){
  this.strfield65 = value;
  }
  public String getStrfield66(){
  return strfield66;
  }
  public void setStrfield66(String value){
  this.strfield66 = value;
  }
  public String getStrfield67(){
  return strfield67;
  }
  public void setStrfield67(String value){
  this.strfield67 = value;
  }
  public String getStrfield68(){
  return strfield68;
  }
  public void setStrfield68(String value){
  this.strfield68 = value;
  }
  public String getStrfield69(){
  return strfield69;
  }
  public void setStrfield69(String value){
  this.strfield69 = value;
  }
  public String getStrfield70(){
  return strfield70;
  }
  public void setStrfield70(String value){
  this.strfield70 = value;
  }
  public String getStrfield71(){
  return strfield71;
  }
  public void setStrfield71(String value){
  this.strfield71 = value;
  }
  public String getStrfield72(){
  return strfield72;
  }
  public void setStrfield72(String value){
  this.strfield72 = value;
  }
  public String getStrfield73(){
  return strfield73;
  }
  public void setStrfield73(String value){
  this.strfield73 = value;
  }
  public String getStrfield74(){
  return strfield74;
  }
  public void setStrfield74(String value){
  this.strfield74 = value;
  }
  public String getStrfield75(){
  return strfield75;
  }
  public void setStrfield75(String value){
  this.strfield75 = value;
  }
  public String getStrfield76(){
  return strfield76;
  }
  public void setStrfield76(String value){
  this.strfield76 = value;
  }
  public String getStrfield77(){
  return strfield77;
  }
  public void setStrfield77(String value){
  this.strfield77 = value;
  }
  public String getStrfield78(){
  return strfield78;
  }
  public void setStrfield78(String value){
  this.strfield78 = value;
  }
  public String getStrfield79(){
  return strfield79;
  }
  public void setStrfield79(String value){
  this.strfield79 = value;
  }
  public String getStrfield80(){
  return strfield80;
  }
  public void setStrfield80(String value){
  this.strfield80 = value;
  }
  public String getStrfield81(){
  return strfield81;
  }
  public void setStrfield81(String value){
  this.strfield81 = value;
  }
  public String getStrfield82(){
  return strfield82;
  }
  public void setStrfield82(String value){
  this.strfield82 = value;
  }
  public String getStrfield83(){
  return strfield83;
  }
  public void setStrfield83(String value){
  this.strfield83 = value;
  }
  public String getStrfield84(){
  return strfield84;
  }
  public void setStrfield84(String value){
  this.strfield84 = value;
  }
  public String getStrfield85(){
  return strfield85;
  }
  public void setStrfield85(String value){
  this.strfield85 = value;
  }
  public String getStrfield86(){
  return strfield86;
  }
  public void setStrfield86(String value){
  this.strfield86 = value;
  }
  public String getStrfield87(){
  return strfield87;
  }
  public void setStrfield87(String value){
  this.strfield87 = value;
  }
  public String getStrfield88(){
  return strfield88;
  }
  public void setStrfield88(String value){
  this.strfield88 = value;
  }
  public String getStrfield89(){
  return strfield89;
  }
  public void setStrfield89(String value){
  this.strfield89 = value;
  }
  public String getStrfield90(){
  return strfield90;
  }
  public void setStrfield90(String value){
  this.strfield90 = value;
  }
  public String getStrfield91(){
  return strfield91;
  }
  public void setStrfield91(String value){
  this.strfield91 = value;
  }
  public String getStrfield92(){
  return strfield92;
  }
  public void setStrfield92(String value){
  this.strfield92 = value;
  }
  public String getStrfield93(){
  return strfield93;
  }
  public void setStrfield93(String value){
  this.strfield93 = value;
  }
  public String getStrfield94(){
  return strfield94;
  }
  public void setStrfield94(String value){
  this.strfield94 = value;
  }
  public String getStrfield95(){
  return strfield95;
  }
  public void setStrfield95(String value){
  this.strfield95 = value;
  }
  public String getStrfield96(){
  return strfield96;
  }
  public void setStrfield96(String value){
  this.strfield96 = value;
  }
  public String getStrfield97(){
  return strfield97;
  }
  public void setStrfield97(String value){
  this.strfield97 = value;
  }
  public String getStrfield98(){
  return strfield98;
  }
  public void setStrfield98(String value){
  this.strfield98 = value;
  }
  public String getStrfield99(){
  return strfield99;
  }
  public void setStrfield99(String value){
  this.strfield99 = value;
  }
  public String getStrfield100(){
  return strfield100;
  }
  public void setStrfield100(String value){
  this.strfield100 = value;
  }
  public String getStrfield101(){
  return strfield101;
  }
  public void setStrfield101(String value){
  this.strfield101 = value;
  }
  public String getStrfield102(){
  return strfield102;
  }
  public void setStrfield102(String value){
  this.strfield102 = value;
  }
  public String getStrfield103(){
  return strfield103;
  }
  public void setStrfield103(String value){
  this.strfield103 = value;
  }
  public String getStrfield104(){
  return strfield104;
  }
  public void setStrfield104(String value){
  this.strfield104 = value;
  }
  public String getStrfield105(){
  return strfield105;
  }
  public void setStrfield105(String value){
  this.strfield105 = value;
  }
  public String getStrfield106(){
  return strfield106;
  }
  public void setStrfield106(String value){
  this.strfield106 = value;
  }
  public String getStrfield107(){
  return strfield107;
  }
  public void setStrfield107(String value){
  this.strfield107 = value;
  }
  public String getStrfield108(){
  return strfield108;
  }
  public void setStrfield108(String value){
  this.strfield108 = value;
  }
  public String getStrfield109(){
  return strfield109;
  }
  public void setStrfield109(String value){
  this.strfield109 = value;
  }
  public String getStrfield110(){
  return strfield110;
  }
  public void setStrfield110(String value){
  this.strfield110 = value;
  }
  public String getStrfield111(){
  return strfield111;
  }
  public void setStrfield111(String value){
  this.strfield111 = value;
  }
  public String getStrfield112(){
  return strfield112;
  }
  public void setStrfield112(String value){
  this.strfield112 = value;
  }
  public String getStrfield113(){
  return strfield113;
  }
  public void setStrfield113(String value){
  this.strfield113 = value;
  }
  public String getStrfield114(){
  return strfield114;
  }
  public void setStrfield114(String value){
  this.strfield114 = value;
  }
  public String getStrfield115(){
  return strfield115;
  }
  public void setStrfield115(String value){
  this.strfield115 = value;
  }
  public String getStrfield116(){
  return strfield116;
  }
  public void setStrfield116(String value){
  this.strfield116 = value;
  }
  public String getStrfield117(){
  return strfield117;
  }
  public void setStrfield117(String value){
  this.strfield117 = value;
  }
  public String getStrfield118(){
  return strfield118;
  }
  public void setStrfield118(String value){
  this.strfield118 = value;
  }
  public String getStrfield119(){
  return strfield119;
  }
  public void setStrfield119(String value){
  this.strfield119 = value;
  }
  public String getStrfield120(){
  return strfield120;
  }
  public void setStrfield120(String value){
  this.strfield120 = value;
  }
  public String getStrfield121(){
  return strfield121;
  }
  public void setStrfield121(String value){
  this.strfield121 = value;
  }
  public String getStrfield122(){
  return strfield122;
  }
  public void setStrfield122(String value){
  this.strfield122 = value;
  }
  public String getStrfield123(){
  return strfield123;
  }
  public void setStrfield123(String value){
  this.strfield123 = value;
  }
  public String getStrfield124(){
  return strfield124;
  }
  public void setStrfield124(String value){
  this.strfield124 = value;
  }
  public String getStrfield125(){
  return strfield125;
  }
  public void setStrfield125(String value){
  this.strfield125 = value;
  }
  public String getStrfield126(){
  return strfield126;
  }
  public void setStrfield126(String value){
  this.strfield126 = value;
  }
  public String getStrfield127(){
  return strfield127;
  }
  public void setStrfield127(String value){
  this.strfield127 = value;
  }
  public String getStrfield128(){
  return strfield128;
  }
  public void setStrfield128(String value){
  this.strfield128 = value;
  }
  public String getStrfield129(){
  return strfield129;
  }
  public void setStrfield129(String value){
  this.strfield129 = value;
  }
  public String getStrfield130(){
  return strfield130;
  }
  public void setStrfield130(String value){
  this.strfield130 = value;
  }
  public String getStrfield131(){
  return strfield131;
  }
  public void setStrfield131(String value){
  this.strfield131 = value;
  }
  public String getStrfield132(){
  return strfield132;
  }
  public void setStrfield132(String value){
  this.strfield132 = value;
  }
  public String getStrfield133(){
  return strfield133;
  }
  public void setStrfield133(String value){
  this.strfield133 = value;
  }
  public String getStrfield134(){
  return strfield134;
  }
  public void setStrfield134(String value){
  this.strfield134 = value;
  }
  public String getStrfield135(){
  return strfield135;
  }
  public void setStrfield135(String value){
  this.strfield135 = value;
  }
  public String getStrfield136(){
  return strfield136;
  }
  public void setStrfield136(String value){
  this.strfield136 = value;
  }
  public String getStrfield137(){
  return strfield137;
  }
  public void setStrfield137(String value){
  this.strfield137 = value;
  }
  public String getStrfield138(){
  return strfield138;
  }
  public void setStrfield138(String value){
  this.strfield138 = value;
  }
  public String getStrfield139(){
  return strfield139;
  }
  public void setStrfield139(String value){
  this.strfield139 = value;
  }
  public String getStrfield140(){
  return strfield140;
  }
  public void setStrfield140(String value){
  this.strfield140 = value;
  }
  public String getStrfield141(){
  return strfield141;
  }
  public void setStrfield141(String value){
  this.strfield141 = value;
  }
  public String getStrfield142(){
  return strfield142;
  }
  public void setStrfield142(String value){
  this.strfield142 = value;
  }
  public String getStrfield143(){
  return strfield143;
  }
  public void setStrfield143(String value){
  this.strfield143 = value;
  }
  public String getStrfield144(){
  return strfield144;
  }
  public void setStrfield144(String value){
  this.strfield144 = value;
  }
  public String getStrfield145(){
  return strfield145;
  }
  public void setStrfield145(String value){
  this.strfield145 = value;
  }
  public String getStrfield146(){
  return strfield146;
  }
  public void setStrfield146(String value){
  this.strfield146 = value;
  }
  public String getStrfield147(){
  return strfield147;
  }
  public void setStrfield147(String value){
  this.strfield147 = value;
  }
  public String getStrfield148(){
  return strfield148;
  }
  public void setStrfield148(String value){
  this.strfield148 = value;
  }
  public String getStrfield149(){
  return strfield149;
  }
  public void setStrfield149(String value){
  this.strfield149 = value;
  }
  public String getStrfield150(){
  return strfield150;
  }
  public void setStrfield150(String value){
  this.strfield150 = value;
  }
  public String getStrfield151(){
  return strfield151;
  }
  public void setStrfield151(String value){
  this.strfield151 = value;
  }
  public String getStrfield152(){
  return strfield152;
  }
  public void setStrfield152(String value){
  this.strfield152 = value;
  }
  public String getStrfield153(){
  return strfield153;
  }
  public void setStrfield153(String value){
  this.strfield153 = value;
  }
  public String getStrfield154(){
  return strfield154;
  }
  public void setStrfield154(String value){
  this.strfield154 = value;
  }
  public String getStrfield155(){
  return strfield155;
  }
  public void setStrfield155(String value){
  this.strfield155 = value;
  }
  public String getStrfield156(){
  return strfield156;
  }
  public void setStrfield156(String value){
  this.strfield156 = value;
  }
  public String getStrfield157(){
  return strfield157;
  }
  public void setStrfield157(String value){
  this.strfield157 = value;
  }
  public String getStrfield158(){
  return strfield158;
  }
  public void setStrfield158(String value){
  this.strfield158 = value;
  }
  public String getStrfield159(){
  return strfield159;
  }
  public void setStrfield159(String value){
  this.strfield159 = value;
  }
  public String getStrfield160(){
  return strfield160;
  }
  public void setStrfield160(String value){
  this.strfield160 = value;
  }
  public String getStrfield161(){
  return strfield161;
  }
  public void setStrfield161(String value){
  this.strfield161 = value;
  }
  public String getStrfield162(){
  return strfield162;
  }
  public void setStrfield162(String value){
  this.strfield162 = value;
  }
  public String getStrfield163(){
  return strfield163;
  }
  public void setStrfield163(String value){
  this.strfield163 = value;
  }
  public String getStrfield164(){
  return strfield164;
  }
  public void setStrfield164(String value){
  this.strfield164 = value;
  }
  public String getStrfield165(){
  return strfield165;
  }
  public void setStrfield165(String value){
  this.strfield165 = value;
  }
  public String getStrfield166(){
  return strfield166;
  }
  public void setStrfield166(String value){
  this.strfield166 = value;
  }
  public String getStrfield167(){
  return strfield167;
  }
  public void setStrfield167(String value){
  this.strfield167 = value;
  }
  public String getStrfield168(){
  return strfield168;
  }
  public void setStrfield168(String value){
  this.strfield168 = value;
  }
  public String getStrfield169(){
  return strfield169;
  }
  public void setStrfield169(String value){
  this.strfield169 = value;
  }
  public String getStrfield170(){
  return strfield170;
  }
  public void setStrfield170(String value){
  this.strfield170 = value;
  }
  public String getStrfield171(){
  return strfield171;
  }
  public void setStrfield171(String value){
  this.strfield171 = value;
  }
  public String getStrfield172(){
  return strfield172;
  }
  public void setStrfield172(String value){
  this.strfield172 = value;
  }
  public String getStrfield173(){
  return strfield173;
  }
  public void setStrfield173(String value){
  this.strfield173 = value;
  }
  public String getStrfield174(){
  return strfield174;
  }
  public void setStrfield174(String value){
  this.strfield174 = value;
  }
  public String getStrfield175(){
  return strfield175;
  }
  public void setStrfield175(String value){
  this.strfield175 = value;
  }
  public String getStrfield176(){
  return strfield176;
  }
  public void setStrfield176(String value){
  this.strfield176 = value;
  }
  public String getStrfield177(){
  return strfield177;
  }
  public void setStrfield177(String value){
  this.strfield177 = value;
  }
  public String getStrfield178(){
  return strfield178;
  }
  public void setStrfield178(String value){
  this.strfield178 = value;
  }
  public String getStrfield179(){
  return strfield179;
  }
  public void setStrfield179(String value){
  this.strfield179 = value;
  }
  public int getIntfield0(){
  return id;
  }
  public void setIntfield0(int value){
  this.id = value;
  }
  public int getIntfield1(){
  return intfield1;
  }
  public void setIntfield1(int value){
  this.intfield1 = value;
  }
  public int getIntfield2(){
  return intfield2;
  }
  public void setIntfield2(int value){
  this.intfield2 = value;
  }
  public int getIntfield3(){
  return intfield3;
  }
  public void setIntfield3(int value){
  this.intfield3 = value;
  }
  public int getIntfield4(){
  return intfield4;
  }
  public void setIntfield4(int value){
  this.intfield4 = value;
  }
  public int getIntfield5(){
  return intfield5;
  }
  public void setIntfield5(int value){
  this.intfield5 = value;
  }
  public int getIntfield6(){
  return intfield6;
  }
  public void setIntfield6(int value){
  this.intfield6 = value;
  }
  public int getIntfield7(){
  return intfield7;
  }
  public void setIntfield7(int value){
  this.intfield7 = value;
  }
  public int getIntfield8(){
  return intfield8;
  }
  public void setIntfield8(int value){
  this.intfield8 = value;
  }
  public int getIntfield9(){
  return intfield9;
  }
  public void setIntfield9(int value){
  this.intfield9 = value;
  }
  public int getIntfield10(){
  return intfield10;
  }
  public void setIntfield10(int value){
  this.intfield10 = value;
  }
  public int getIntfield11(){
  return intfield11;
  }
  public void setIntfield11(int value){
  this.intfield11 = value;
  }
  public int getIntfield12(){
  return intfield12;
  }
  public void setIntfield12(int value){
  this.intfield12 = value;
  }
  public int getIntfield13(){
  return intfield13;
  }
  public void setIntfield13(int value){
  this.intfield13 = value;
  }
  public int getIntfield14(){
  return intfield14;
  }
  public void setIntfield14(int value){
  this.intfield14 = value;
  }
  public int getIntfield15(){
  return intfield15;
  }
  public void setIntfield15(int value){
  this.intfield15 = value;
  }
  public int getIntfield16(){
  return intfield16;
  }
  public void setIntfield16(int value){
  this.intfield16 = value;
  }
  public int getIntfield17(){
  return intfield17;
  }
  public void setIntfield17(int value){
  this.intfield17 = value;
  }
  public int getIntfield18(){
  return intfield18;
  }
  public void setIntfield18(int value){
  this.intfield18 = value;
  }
  public int getIntfield19(){
  return intfield19;
  }
  public void setIntfield19(int value){
  this.intfield19 = value;
  }
  public int getIntfield20(){
  return intfield20;
  }
  public void setIntfield20(int value){
  this.intfield20 = value;
  }
  public int getIntfield21(){
  return intfield21;
  }
  public void setIntfield21(int value){
  this.intfield21 = value;
  }
  public int getIntfield22(){
  return intfield22;
  }
  public void setIntfield22(int value){
  this.intfield22 = value;
  }
  public int getIntfield23(){
  return intfield23;
  }
  public void setIntfield23(int value){
  this.intfield23 = value;
  }
  public int getIntfield24(){
  return intfield24;
  }
  public void setIntfield24(int value){
  this.intfield24 = value;
  }
  public int getIntfield25(){
  return intfield25;
  }
  public void setIntfield25(int value){
  this.intfield25 = value;
  }
  public int getIntfield26(){
  return intfield26;
  }
  public void setIntfield26(int value){
  this.intfield26 = value;
  }
  public int getIntfield27(){
  return intfield27;
  }
  public void setIntfield27(int value){
  this.intfield27 = value;
  }
  public int getIntfield28(){
  return intfield28;
  }
  public void setIntfield28(int value){
  this.intfield28 = value;
  }
  public int getIntfield29(){
  return intfield29;
  }
  public void setIntfield29(int value){
  this.intfield29 = value;
  }
  public int getIntfield30(){
  return intfield30;
  }
  public void setIntfield30(int value){
  this.intfield30 = value;
  }
  public int getIntfield31(){
  return intfield31;
  }
  public void setIntfield31(int value){
  this.intfield31 = value;
  }
  public int getIntfield32(){
  return intfield32;
  }
  public void setIntfield32(int value){
  this.intfield32 = value;
  }
  public int getIntfield33(){
  return intfield33;
  }
  public void setIntfield33(int value){
  this.intfield33 = value;
  }
  public int getIntfield34(){
  return intfield34;
  }
  public void setIntfield34(int value){
  this.intfield34 = value;
  }
  public int getIntfield35(){
  return intfield35;
  }
  public void setIntfield35(int value){
  this.intfield35 = value;
  }
  public int getIntfield36(){
  return intfield36;
  }
  public void setIntfield36(int value){
  this.intfield36 = value;
  }
  public int getIntfield37(){
  return intfield37;
  }
  public void setIntfield37(int value){
  this.intfield37 = value;
  }
  public int getIntfield38(){
  return intfield38;
  }
  public void setIntfield38(int value){
  this.intfield38 = value;
  }
  public int getIntfield39(){
  return intfield39;
  }
  public void setIntfield39(int value){
  this.intfield39 = value;
  }
  public int getIntfield40(){
  return intfield40;
  }
  public void setIntfield40(int value){
  this.intfield40 = value;
  }
  public int getIntfield41(){
  return intfield41;
  }
  public void setIntfield41(int value){
  this.intfield41 = value;
  }
  public int getIntfield42(){
  return intfield42;
  }
  public void setIntfield42(int value){
  this.intfield42 = value;
  }
  public int getIntfield43(){
  return intfield43;
  }
  public void setIntfield43(int value){
  this.intfield43 = value;
  }
  public int getIntfield44(){
  return intfield44;
  }
  public void setIntfield44(int value){
  this.intfield44 = value;
  }
  public int getIntfield45(){
  return intfield45;
  }
  public void setIntfield45(int value){
  this.intfield45 = value;
  }
  public int getIntfield46(){
  return intfield46;
  }
  public void setIntfield46(int value){
  this.intfield46 = value;
  }
  public int getIntfield47(){
  return intfield47;
  }
  public void setIntfield47(int value){
  this.intfield47 = value;
  }
  public int getIntfield48(){
  return intfield48;
  }
  public void setIntfield48(int value){
  this.intfield48 = value;
  }
  public int getIntfield49(){
  return intfield49;
  }
  public void setIntfield49(int value){
  this.intfield49 = value;
  }
  public int getIntfield50(){
  return intfield50;
  }
  public void setIntfield50(int value){
  this.intfield50 = value;
  }
  public int getIntfield51(){
  return intfield51;
  }
  public void setIntfield51(int value){
  this.intfield51 = value;
  }
  public int getIntfield52(){
  return intfield52;
  }
  public void setIntfield52(int value){
  this.intfield52 = value;
  }
  public int getIntfield53(){
  return intfield53;
  }
  public void setIntfield53(int value){
  this.intfield53 = value;
  }
  public int getIntfield54(){
  return intfield54;
  }
  public void setIntfield54(int value){
  this.intfield54 = value;
  }
  public int getIntfield55(){
  return intfield55;
  }
  public void setIntfield55(int value){
  this.intfield55 = value;
  }
  public int getIntfield56(){
  return intfield56;
  }
  public void setIntfield56(int value){
  this.intfield56 = value;
  }
  public int getIntfield57(){
  return intfield57;
  }
  public void setIntfield57(int value){
  this.intfield57 = value;
  }
  public int getIntfield58(){
  return intfield58;
  }
  public void setIntfield58(int value){
  this.intfield58 = value;
  }
  public int getIntfield59(){
  return intfield59;
  }
  public void setIntfield59(int value){
  this.intfield59 = value;
  }
  public int getIntfield60(){
  return intfield60;
  }
  public void setIntfield60(int value){
  this.intfield60 = value;
  }
  public int getIntfield61(){
  return intfield61;
  }
  public void setIntfield61(int value){
  this.intfield61 = value;
  }
  public int getIntfield62(){
  return intfield62;
  }
  public void setIntfield62(int value){
  this.intfield62 = value;
  }
  public int getIntfield63(){
  return intfield63;
  }
  public void setIntfield63(int value){
  this.intfield63 = value;
  }
  public int getIntfield64(){
  return intfield64;
  }
  public void setIntfield64(int value){
  this.intfield64 = value;
  }
  public int getIntfield65(){
  return intfield65;
  }
  public void setIntfield65(int value){
  this.intfield65 = value;
  }
  public int getIntfield66(){
  return intfield66;
  }
  public void setIntfield66(int value){
  this.intfield66 = value;
  }
  public int getIntfield67(){
  return intfield67;
  }
  public void setIntfield67(int value){
  this.intfield67 = value;
  }
  public int getIntfield68(){
  return intfield68;
  }
  public void setIntfield68(int value){
  this.intfield68 = value;
  }
  public int getIntfield69(){
  return intfield69;
  }
  public void setIntfield69(int value){
  this.intfield69 = value;
  }
  public int getIntfield70(){
  return intfield70;
  }
  public void setIntfield70(int value){
  this.intfield70 = value;
  }
  public int getIntfield71(){
  return intfield71;
  }
  public void setIntfield71(int value){
  this.intfield71 = value;
  }
  public int getIntfield72(){
  return intfield72;
  }
  public void setIntfield72(int value){
  this.intfield72 = value;
  }
  public int getIntfield73(){
  return intfield73;
  }
  public void setIntfield73(int value){
  this.intfield73 = value;
  }
  public int getIntfield74(){
  return intfield74;
  }
  public void setIntfield74(int value){
  this.intfield74 = value;
  }
  public int getIntfield75(){
  return intfield75;
  }
  public void setIntfield75(int value){
  this.intfield75 = value;
  }
  public int getIntfield76(){
  return intfield76;
  }
  public void setIntfield76(int value){
  this.intfield76 = value;
  }
  public int getIntfield77(){
  return intfield77;
  }
  public void setIntfield77(int value){
  this.intfield77 = value;
  }
  public int getIntfield78(){
  return intfield78;
  }
  public void setIntfield78(int value){
  this.intfield78 = value;
  }
  public int getIntfield79(){
  return intfield79;
  }
  public void setIntfield79(int value){
  this.intfield79 = value;
  }
  public int getIntfield80(){
  return intfield80;
  }
  public void setIntfield80(int value){
  this.intfield80 = value;
  }
  public int getIntfield81(){
  return intfield81;
  }
  public void setIntfield81(int value){
  this.intfield81 = value;
  }
  public int getIntfield82(){
  return intfield82;
  }
  public void setIntfield82(int value){
  this.intfield82 = value;
  }
  public int getIntfield83(){
  return intfield83;
  }
  public void setIntfield83(int value){
  this.intfield83 = value;
  }
  public int getIntfield84(){
  return intfield84;
  }
  public void setIntfield84(int value){
  this.intfield84 = value;
  }
  public int getIntfield85(){
  return intfield85;
  }
  public void setIntfield85(int value){
  this.intfield85 = value;
  }
  public int getIntfield86(){
  return intfield86;
  }
  public void setIntfield86(int value){
  this.intfield86 = value;
  }
  public int getIntfield87(){
  return intfield87;
  }
  public void setIntfield87(int value){
  this.intfield87 = value;
  }
  public int getIntfield88(){
  return intfield88;
  }
  public void setIntfield88(int value){
  this.intfield88 = value;
  }
  public int getIntfield89(){
  return intfield89;
  }
  public void setIntfield89(int value){
  this.intfield89 = value;
  }
  public int getIntfield90(){
  return intfield90;
  }
  public void setIntfield90(int value){
  this.intfield90 = value;
  }
  public int getIntfield91(){
  return intfield91;
  }
  public void setIntfield91(int value){
  this.intfield91 = value;
  }
  public int getIntfield92(){
  return intfield92;
  }
  public void setIntfield92(int value){
  this.intfield92 = value;
  }
  public int getIntfield93(){
  return intfield93;
  }
  public void setIntfield93(int value){
  this.intfield93 = value;
  }
  public int getIntfield94(){
  return intfield94;
  }
  public void setIntfield94(int value){
  this.intfield94 = value;
  }
  public int getIntfield95(){
  return intfield95;
  }
  public void setIntfield95(int value){
  this.intfield95 = value;
  }
  public int getIntfield96(){
  return intfield96;
  }
  public void setIntfield96(int value){
  this.intfield96 = value;
  }
  public int getIntfield97(){
  return intfield97;
  }
  public void setIntfield97(int value){
  this.intfield97 = value;
  }
  public int getIntfield98(){
  return intfield98;
  }
  public void setIntfield98(int value){
  this.intfield98 = value;
  }
  public int getIntfield99(){
  return intfield99;
  }
  public void setIntfield99(int value){
  this.intfield99 = value;
  }
  public int getIntfield100(){
  return intfield100;
  }
  public void setIntfield100(int value){
  this.intfield100 = value;
  }
  public int getIntfield101(){
  return intfield101;
  }
  public void setIntfield101(int value){
  this.intfield101 = value;
  }
  public int getIntfield102(){
  return intfield102;
  }
  public void setIntfield102(int value){
  this.intfield102 = value;
  }
  public int getIntfield103(){
  return intfield103;
  }
  public void setIntfield103(int value){
  this.intfield103 = value;
  }
  public int getIntfield104(){
  return intfield104;
  }
  public void setIntfield104(int value){
  this.intfield104 = value;
  }
  public int getIntfield105(){
  return intfield105;
  }
  public void setIntfield105(int value){
  this.intfield105 = value;
  }
  public int getIntfield106(){
  return intfield106;
  }
  public void setIntfield106(int value){
  this.intfield106 = value;
  }
  public int getIntfield107(){
  return intfield107;
  }
  public void setIntfield107(int value){
  this.intfield107 = value;
  }
  public int getIntfield108(){
  return intfield108;
  }
  public void setIntfield108(int value){
  this.intfield108 = value;
  }
  public int getIntfield109(){
  return intfield109;
  }
  public void setIntfield109(int value){
  this.intfield109 = value;
  }
  public int getIntfield110(){
  return intfield110;
  }
  public void setIntfield110(int value){
  this.intfield110 = value;
  }
  public int getIntfield111(){
  return intfield111;
  }
  public void setIntfield111(int value){
  this.intfield111 = value;
  }
  public int getIntfield112(){
  return intfield112;
  }
  public void setIntfield112(int value){
  this.intfield112 = value;
  }
  public int getIntfield113(){
  return intfield113;
  }
  public void setIntfield113(int value){
  this.intfield113 = value;
  }
  public int getIntfield114(){
  return intfield114;
  }
  public void setIntfield114(int value){
  this.intfield114 = value;
  }
  public int getIntfield115(){
  return intfield115;
  }
  public void setIntfield115(int value){
  this.intfield115 = value;
  }
  public int getIntfield116(){
  return intfield116;
  }
  public void setIntfield116(int value){
  this.intfield116 = value;
  }
  public int getIntfield117(){
  return intfield117;
  }
  public void setIntfield117(int value){
  this.intfield117 = value;
  }
  public int getIntfield118(){
  return intfield118;
  }
  public void setIntfield118(int value){
  this.intfield118 = value;
  }
  public int getIntfield119(){
  return intfield119;
  }
  public void setIntfield119(int value){
  this.intfield119 = value;
  }
  public int getIntfield120(){
  return intfield120;
  }
  public void setIntfield120(int value){
  this.intfield120 = value;
  }
  public int getIntfield121(){
  return intfield121;
  }
  public void setIntfield121(int value){
  this.intfield121 = value;
  }
  public int getIntfield122(){
  return intfield122;
  }
  public void setIntfield122(int value){
  this.intfield122 = value;
  }
  public int getIntfield123(){
  return intfield123;
  }
  public void setIntfield123(int value){
  this.intfield123 = value;
  }
  public int getIntfield124(){
  return intfield124;
  }
  public void setIntfield124(int value){
  this.intfield124 = value;
  }
  public int getIntfield125(){
  return intfield125;
  }
  public void setIntfield125(int value){
  this.intfield125 = value;
  }
  public int getIntfield126(){
  return intfield126;
  }
  public void setIntfield126(int value){
  this.intfield126 = value;
  }
  public int getIntfield127(){
  return intfield127;
  }
  public void setIntfield127(int value){
  this.intfield127 = value;
  }
  public int getIntfield128(){
  return intfield128;
  }
  public void setIntfield128(int value){
  this.intfield128 = value;
  }
  public int getIntfield129(){
  return intfield129;
  }
  public void setIntfield129(int value){
  this.intfield129 = value;
  }
  public int getIntfield130(){
  return intfield130;
  }
  public void setIntfield130(int value){
  this.intfield130 = value;
  }
  public int getIntfield131(){
  return intfield131;
  }
  public void setIntfield131(int value){
  this.intfield131 = value;
  }
  public int getIntfield132(){
  return intfield132;
  }
  public void setIntfield132(int value){
  this.intfield132 = value;
  }
  public int getIntfield133(){
  return intfield133;
  }
  public void setIntfield133(int value){
  this.intfield133 = value;
  }
  public int getIntfield134(){
  return intfield134;
  }
  public void setIntfield134(int value){
  this.intfield134 = value;
  }
  public int getIntfield135(){
  return intfield135;
  }
  public void setIntfield135(int value){
  this.intfield135 = value;
  }
  public int getIntfield136(){
  return intfield136;
  }
  public void setIntfield136(int value){
  this.intfield136 = value;
  }
  public int getIntfield137(){
  return intfield137;
  }
  public void setIntfield137(int value){
  this.intfield137 = value;
  }
  public int getIntfield138(){
  return intfield138;
  }
  public void setIntfield138(int value){
  this.intfield138 = value;
  }
  public int getIntfield139(){
  return intfield139;
  }
  public void setIntfield139(int value){
  this.intfield139 = value;
  }
  public int getIntfield140(){
  return intfield140;
  }
  public void setIntfield140(int value){
  this.intfield140 = value;
  }
  public int getIntfield141(){
  return intfield141;
  }
  public void setIntfield141(int value){
  this.intfield141 = value;
  }
  public int getIntfield142(){
  return intfield142;
  }
  public void setIntfield142(int value){
  this.intfield142 = value;
  }
  public int getIntfield143(){
  return intfield143;
  }
  public void setIntfield143(int value){
  this.intfield143 = value;
  }
  public int getIntfield144(){
  return intfield144;
  }
  public void setIntfield144(int value){
  this.intfield144 = value;
  }
  public int getIntfield145(){
  return intfield145;
  }
  public void setIntfield145(int value){
  this.intfield145 = value;
  }
  public int getIntfield146(){
  return intfield146;
  }
  public void setIntfield146(int value){
  this.intfield146 = value;
  }
  public int getIntfield147(){
  return intfield147;
  }
  public void setIntfield147(int value){
  this.intfield147 = value;
  }
  public int getIntfield148(){
  return intfield148;
  }
  public void setIntfield148(int value){
  this.intfield148 = value;
  }
  public int getIntfield149(){
  return intfield149;
  }
  public void setIntfield149(int value){
  this.intfield149 = value;
  }
  public int getIntfield150(){
  return intfield150;
  }
  public void setIntfield150(int value){
  this.intfield150 = value;
  }
  public int getIntfield151(){
  return intfield151;
  }
  public void setIntfield151(int value){
  this.intfield151 = value;
  }
  public int getIntfield152(){
  return intfield152;
  }
  public void setIntfield152(int value){
  this.intfield152 = value;
  }
  public int getIntfield153(){
  return intfield153;
  }
  public void setIntfield153(int value){
  this.intfield153 = value;
  }
  public int getIntfield154(){
  return intfield154;
  }
  public void setIntfield154(int value){
  this.intfield154 = value;
  }
  public int getIntfield155(){
  return intfield155;
  }
  public void setIntfield155(int value){
  this.intfield155 = value;
  }
  public int getIntfield156(){
  return intfield156;
  }
  public void setIntfield156(int value){
  this.intfield156 = value;
  }
  public int getIntfield157(){
  return intfield157;
  }
  public void setIntfield157(int value){
  this.intfield157 = value;
  }
  public int getIntfield158(){
  return intfield158;
  }
  public void setIntfield158(int value){
  this.intfield158 = value;
  }
  public int getIntfield159(){
  return intfield159;
  }
  public void setIntfield159(int value){
  this.intfield159 = value;
  }
  public int getIntfield160(){
  return intfield160;
  }
  public void setIntfield160(int value){
  this.intfield160 = value;
  }
  public int getIntfield161(){
  return intfield161;
  }
  public void setIntfield161(int value){
  this.intfield161 = value;
  }
  public int getIntfield162(){
  return intfield162;
  }
  public void setIntfield162(int value){
  this.intfield162 = value;
  }
  public int getIntfield163(){
  return intfield163;
  }
  public void setIntfield163(int value){
  this.intfield163 = value;
  }
  public int getIntfield164(){
  return intfield164;
  }
  public void setIntfield164(int value){
  this.intfield164 = value;
  }
  public int getIntfield165(){
  return intfield165;
  }
  public void setIntfield165(int value){
  this.intfield165 = value;
  }
  public int getIntfield166(){
  return intfield166;
  }
  public void setIntfield166(int value){
  this.intfield166 = value;
  }
  public int getIntfield167(){
  return intfield167;
  }
  public void setIntfield167(int value){
  this.intfield167 = value;
  }
  public int getIntfield168(){
  return intfield168;
  }
  public void setIntfield168(int value){
  this.intfield168 = value;
  }
  public int getIntfield169(){
  return intfield169;
  }
  public void setIntfield169(int value){
  this.intfield169 = value;
  }
  public int getIntfield170(){
  return intfield170;
  }
  public void setIntfield170(int value){
  this.intfield170 = value;
  }
  public int getIntfield171(){
  return intfield171;
  }
  public void setIntfield171(int value){
  this.intfield171 = value;
  }
  public int getIntfield172(){
  return intfield172;
  }
  public void setIntfield172(int value){
  this.intfield172 = value;
  }
  public int getIntfield173(){
  return intfield173;
  }
  public void setIntfield173(int value){
  this.intfield173 = value;
  }
  public int getIntfield174(){
  return intfield174;
  }
  public void setIntfield174(int value){
  this.intfield174 = value;
  }
  public int getIntfield175(){
  return intfield175;
  }
  public void setIntfield175(int value){
  this.intfield175 = value;
  }
  public int getIntfield176(){
  return intfield176;
  }
  public void setIntfield176(int value){
  this.intfield176 = value;
  }
  public double getDoublefield0(){
  return doublefield0;
  }
  public void setDoublefield0(double value){
  this.doublefield0 = value;
  }
  public double getDoublefield1(){
  return doublefield1;
  }
  public void setDoublefield1(double value){
  this.doublefield1 = value;
  }
  public double getDoublefield2(){
  return doublefield2;
  }
  public void setDoublefield2(double value){
  this.doublefield2 = value;
  }
  public double getDoublefield3(){
  return doublefield3;
  }
  public void setDoublefield3(double value){
  this.doublefield3 = value;
  }
  public double getDoublefield4(){
  return doublefield4;
  }
  public void setDoublefield4(double value){
  this.doublefield4 = value;
  }
  public double getDoublefield5(){
  return doublefield5;
  }
  public void setDoublefield5(double value){
  this.doublefield5 = value;
  }
  public double getDoublefield6(){
  return doublefield6;
  }
  public void setDoublefield6(double value){
  this.doublefield6 = value;
  }
  public double getDoublefield7(){
  return doublefield7;
  }
  public void setDoublefield7(double value){
  this.doublefield7 = value;
  }
  public double getDoublefield8(){
  return doublefield8;
  }
  public void setDoublefield8(double value){
  this.doublefield8 = value;
  }
  public double getDoublefield9(){
  return doublefield9;
  }
  public void setDoublefield9(double value){
  this.doublefield9 = value;
  }
  public double getDoublefield10(){
  return doublefield10;
  }
  public void setDoublefield10(double value){
  this.doublefield10 = value;
  }
  public double getDoublefield11(){
  return doublefield11;
  }
  public void setDoublefield11(double value){
  this.doublefield11 = value;
  }
  public double getDoublefield12(){
  return doublefield12;
  }
  public void setDoublefield12(double value){
  this.doublefield12 = value;
  }
  public double getDoublefield13(){
  return doublefield13;
  }
  public void setDoublefield13(double value){
  this.doublefield13 = value;
  }
  public double getDoublefield14(){
  return doublefield14;
  }
  public void setDoublefield14(double value){
  this.doublefield14 = value;
  }
  public double getDoublefield15(){
  return doublefield15;
  }
  public void setDoublefield15(double value){
  this.doublefield15 = value;
  }
  public double getDoublefield16(){
  return doublefield16;
  }
  public void setDoublefield16(double value){
  this.doublefield16 = value;
  }
  public double getDoublefield17(){
  return doublefield17;
  }
  public void setDoublefield17(double value){
  this.doublefield17 = value;
  }
  public double getDoublefield18(){
  return doublefield18;
  }
  public void setDoublefield18(double value){
  this.doublefield18 = value;
  }
  public double getDoublefield19(){
  return doublefield19;
  }
  public void setDoublefield19(double value){
  this.doublefield19 = value;
  }
  public double getDoublefield20(){
  return doublefield20;
  }
  public void setDoublefield20(double value){
  this.doublefield20 = value;
  }
  public double getDoublefield21(){
  return doublefield21;
  }
  public void setDoublefield21(double value){
  this.doublefield21 = value;
  }
  public double getDoublefield22(){
  return doublefield22;
  }
  public void setDoublefield22(double value){
  this.doublefield22 = value;
  }
  public double getDoublefield23(){
  return doublefield23;
  }
  public void setDoublefield23(double value){
  this.doublefield23 = value;
  }
  public double getDoublefield24(){
  return doublefield24;
  }
  public void setDoublefield24(double value){
  this.doublefield24 = value;
  }
  public double getDoublefield25(){
  return doublefield25;
  }
  public void setDoublefield25(double value){
  this.doublefield25 = value;
  }
  public double getDoublefield26(){
  return doublefield26;
  }
  public void setDoublefield26(double value){
  this.doublefield26 = value;
  }
  public double getDoublefield27(){
  return doublefield27;
  }
  public void setDoublefield27(double value){
  this.doublefield27 = value;
  }
  public double getDoublefield28(){
  return doublefield28;
  }
  public void setDoublefield28(double value){
  this.doublefield28 = value;
  }
  public double getDoublefield29(){
  return doublefield29;
  }
  public void setDoublefield29(double value){
  this.doublefield29 = value;
  }
  public double getDoublefield30(){
  return doublefield30;
  }
  public void setDoublefield30(double value){
  this.doublefield30 = value;
  }
  public double getDoublefield31(){
  return doublefield31;
  }
  public void setDoublefield31(double value){
  this.doublefield31 = value;
  }
  public double getDoublefield32(){
  return doublefield32;
  }
  public void setDoublefield32(double value){
  this.doublefield32 = value;
  }
  public double getDoublefield33(){
  return doublefield33;
  }
  public void setDoublefield33(double value){
  this.doublefield33 = value;
  }
  public double getDoublefield34(){
  return doublefield34;
  }
  public void setDoublefield34(double value){
  this.doublefield34 = value;
  }
  public double getDoublefield35(){
  return doublefield35;
  }
  public void setDoublefield35(double value){
  this.doublefield35 = value;
  }
  public double getDoublefield36(){
  return doublefield36;
  }
  public void setDoublefield36(double value){
  this.doublefield36 = value;
  }
  public double getDoublefield37(){
  return doublefield37;
  }
  public void setDoublefield37(double value){
  this.doublefield37 = value;
  }
  public double getDoublefield38(){
  return doublefield38;
  }
  public void setDoublefield38(double value){
  this.doublefield38 = value;
  }
  public double getDoublefield39(){
  return doublefield39;
  }
  public void setDoublefield39(double value){
  this.doublefield39 = value;
  }
  public double getDoublefield40(){
  return doublefield40;
  }
  public void setDoublefield40(double value){
  this.doublefield40 = value;
  }
  public double getDoublefield41(){
  return doublefield41;
  }
  public void setDoublefield41(double value){
  this.doublefield41 = value;
  }
  public double getDoublefield42(){
  return doublefield42;
  }
  public void setDoublefield42(double value){
  this.doublefield42 = value;
  }
  public double getDoublefield43(){
  return doublefield43;
  }
  public void setDoublefield43(double value){
  this.doublefield43 = value;
  }
  public double getDoublefield44(){
  return doublefield44;
  }
  public void setDoublefield44(double value){
  this.doublefield44 = value;
  }
  public double getDoublefield45(){
  return doublefield45;
  }
  public void setDoublefield45(double value){
  this.doublefield45 = value;
  }
  public double getDoublefield46(){
  return doublefield46;
  }
  public void setDoublefield46(double value){
  this.doublefield46 = value;
  }
  public double getDoublefield47(){
  return doublefield47;
  }
  public void setDoublefield47(double value){
  this.doublefield47 = value;
  }
  public double getDoublefield48(){
  return doublefield48;
  }
  public void setDoublefield48(double value){
  this.doublefield48 = value;
  }
  public double getDoublefield49(){
  return doublefield49;
  }
  public void setDoublefield49(double value){
  this.doublefield49 = value;
  }
  public double getDoublefield50(){
  return doublefield50;
  }
  public void setDoublefield50(double value){
  this.doublefield50 = value;
  }
  public double getDoublefield51(){
  return doublefield51;
  }
  public void setDoublefield51(double value){
  this.doublefield51 = value;
  }
  public double getDoublefield52(){
  return doublefield52;
  }
  public void setDoublefield52(double value){
  this.doublefield52 = value;
  }
  public double getDoublefield53(){
  return doublefield53;
  }
  public void setDoublefield53(double value){
  this.doublefield53 = value;
  }
  public double getDoublefield54(){
  return doublefield54;
  }
  public void setDoublefield54(double value){
  this.doublefield54 = value;
  }
  public double getDoublefield55(){
  return doublefield55;
  }
  public void setDoublefield55(double value){
  this.doublefield55 = value;
  }
  public double getDoublefield56(){
  return doublefield56;
  }
  public void setDoublefield56(double value){
  this.doublefield56 = value;
  }
  public double getDoublefield57(){
  return doublefield57;
  }
  public void setDoublefield57(double value){
  this.doublefield57 = value;
  }
  public double getDoublefield58(){
  return doublefield58;
  }
  public void setDoublefield58(double value){
  this.doublefield58 = value;
  }
  public double getDoublefield59(){
  return doublefield59;
  }
  public void setDoublefield59(double value){
  this.doublefield59 = value;
  }
  public double getDoublefield60(){
  return doublefield60;
  }
  public void setDoublefield60(double value){
  this.doublefield60 = value;
  }
  public double getDoublefield61(){
  return doublefield61;
  }
  public void setDoublefield61(double value){
  this.doublefield61 = value;
  }
  public double getDoublefield62(){
  return doublefield62;
  }
  public void setDoublefield62(double value){
  this.doublefield62 = value;
  }
  public double getDoublefield63(){
  return doublefield63;
  }
  public void setDoublefield63(double value){
  this.doublefield63 = value;
  }
  public double getDoublefield64(){
  return doublefield64;
  }
  public void setDoublefield64(double value){
  this.doublefield64 = value;
  }
  public double getDoublefield65(){
  return doublefield65;
  }
  public void setDoublefield65(double value){
  this.doublefield65 = value;
  }
  public double getDoublefield66(){
  return doublefield66;
  }
  public void setDoublefield66(double value){
  this.doublefield66 = value;
  }
  public double getDoublefield67(){
  return doublefield67;
  }
  public void setDoublefield67(double value){
  this.doublefield67 = value;
  }
  public double getDoublefield68(){
  return doublefield68;
  }
  public void setDoublefield68(double value){
  this.doublefield68 = value;
  }
  public double getDoublefield69(){
  return doublefield69;
  }
  public void setDoublefield69(double value){
  this.doublefield69 = value;
  }
  public double getDoublefield70(){
  return doublefield70;
  }
  public void setDoublefield70(double value){
  this.doublefield70 = value;
  }
  public double getDoublefield71(){
  return doublefield71;
  }
  public void setDoublefield71(double value){
  this.doublefield71 = value;
  }
  public double getDoublefield72(){
  return doublefield72;
  }
  public void setDoublefield72(double value){
  this.doublefield72 = value;
  }
  public double getDoublefield73(){
  return doublefield73;
  }
  public void setDoublefield73(double value){
  this.doublefield73 = value;
  }
  public double getDoublefield74(){
  return doublefield74;
  }
  public void setDoublefield74(double value){
  this.doublefield74 = value;
  }
  public double getDoublefield75(){
  return doublefield75;
  }
  public void setDoublefield75(double value){
  this.doublefield75 = value;
  }
  public double getDoublefield76(){
  return doublefield76;
  }
  public void setDoublefield76(double value){
  this.doublefield76 = value;
  }
  public double getDoublefield77(){
  return doublefield77;
  }
  public void setDoublefield77(double value){
  this.doublefield77 = value;
  }
  public double getDoublefield78(){
  return doublefield78;
  }
  public void setDoublefield78(double value){
  this.doublefield78 = value;
  }
  public double getDoublefield79(){
  return doublefield79;
  }
  public void setDoublefield79(double value){
  this.doublefield79 = value;
  }
  public double getDoublefield80(){
  return doublefield80;
  }
  public void setDoublefield80(double value){
  this.doublefield80 = value;
  }
  public double getDoublefield81(){
  return doublefield81;
  }
  public void setDoublefield81(double value){
  this.doublefield81 = value;
  }
  public double getDoublefield82(){
  return doublefield82;
  }
  public void setDoublefield82(double value){
  this.doublefield82 = value;
  }
  public double getDoublefield83(){
  return doublefield83;
  }
  public void setDoublefield83(double value){
  this.doublefield83 = value;
  }
  public double getDoublefield84(){
  return doublefield84;
  }
  public void setDoublefield84(double value){
  this.doublefield84 = value;
  }
  public double getDoublefield85(){
  return doublefield85;
  }
  public void setDoublefield85(double value){
  this.doublefield85 = value;
  }
  public double getDoublefield86(){
  return doublefield86;
  }
  public void setDoublefield86(double value){
  this.doublefield86 = value;
  }
  public double getDoublefield87(){
  return doublefield87;
  }
  public void setDoublefield87(double value){
  this.doublefield87 = value;
  }
  public double getDoublefield88(){
  return doublefield88;
  }
  public void setDoublefield88(double value){
  this.doublefield88 = value;
  }
  public double getDoublefield89(){
  return doublefield89;
  }
  public void setDoublefield89(double value){
  this.doublefield89 = value;
  }
  public double getDoublefield90(){
  return doublefield90;
  }
  public void setDoublefield90(double value){
  this.doublefield90 = value;
  }
  public double getDoublefield91(){
  return doublefield91;
  }
  public void setDoublefield91(double value){
  this.doublefield91 = value;
  }
  public double getDoublefield92(){
  return doublefield92;
  }
  public void setDoublefield92(double value){
  this.doublefield92 = value;
  }
  public double getDoublefield93(){
  return doublefield93;
  }
  public void setDoublefield93(double value){
  this.doublefield93 = value;
  }
  public double getDoublefield94(){
  return doublefield94;
  }
  public void setDoublefield94(double value){
  this.doublefield94 = value;
  }
  public double getDoublefield95(){
  return doublefield95;
  }
  public void setDoublefield95(double value){
  this.doublefield95 = value;
  }
  public double getDoublefield96(){
  return doublefield96;
  }
  public void setDoublefield96(double value){
  this.doublefield96 = value;
  }
  public double getDoublefield97(){
  return doublefield97;
  }
  public void setDoublefield97(double value){
  this.doublefield97 = value;
  }
  public double getDoublefield98(){
  return doublefield98;
  }
  public void setDoublefield98(double value){
  this.doublefield98 = value;
  }
  public double getDoublefield99(){
  return doublefield99;
  }
  public void setDoublefield99(double value){
  this.doublefield99 = value;
  }
  public double getDoublefield100(){
  return doublefield100;
  }
  public void setDoublefield100(double value){
  this.doublefield100 = value;
  }
  public double getDoublefield101(){
  return doublefield101;
  }
  public void setDoublefield101(double value){
  this.doublefield101 = value;
  }
  public double getDoublefield102(){
  return doublefield102;
  }
  public void setDoublefield102(double value){
  this.doublefield102 = value;
  }
  public double getDoublefield103(){
  return doublefield103;
  }
  public void setDoublefield103(double value){
  this.doublefield103 = value;
  }
  public double getDoublefield104(){
  return doublefield104;
  }
  public void setDoublefield104(double value){
  this.doublefield104 = value;
  }
  public double getDoublefield105(){
  return doublefield105;
  }
  public void setDoublefield105(double value){
  this.doublefield105 = value;
  }
  public double getDoublefield106(){
  return doublefield106;
  }
  public void setDoublefield106(double value){
  this.doublefield106 = value;
  }
  public double getDoublefield107(){
  return doublefield107;
  }
  public void setDoublefield107(double value){
  this.doublefield107 = value;
  }
  public double getDoublefield108(){
  return doublefield108;
  }
  public void setDoublefield108(double value){
  this.doublefield108 = value;
  }
  public double getDoublefield109(){
  return doublefield109;
  }
  public void setDoublefield109(double value){
  this.doublefield109 = value;
  }
  public double getDoublefield110(){
  return doublefield110;
  }
  public void setDoublefield110(double value){
  this.doublefield110 = value;
  }
  public double getDoublefield111(){
  return doublefield111;
  }
  public void setDoublefield111(double value){
  this.doublefield111 = value;
  }
  public double getDoublefield112(){
  return doublefield112;
  }
  public void setDoublefield112(double value){
  this.doublefield112 = value;
  }
  public double getDoublefield113(){
  return doublefield113;
  }
  public void setDoublefield113(double value){
  this.doublefield113 = value;
  }
  public double getDoublefield114(){
  return doublefield114;
  }
  public void setDoublefield114(double value){
  this.doublefield114 = value;
  }
  public double getDoublefield115(){
  return doublefield115;
  }
  public void setDoublefield115(double value){
  this.doublefield115 = value;
  }
  public double getDoublefield116(){
  return doublefield116;
  }
  public void setDoublefield116(double value){
  this.doublefield116 = value;
  }
  public double getDoublefield117(){
  return doublefield117;
  }
  public void setDoublefield117(double value){
  this.doublefield117 = value;
  }
  public double getDoublefield118(){
  return doublefield118;
  }
  public void setDoublefield118(double value){
  this.doublefield118 = value;
  }
  public double getDoublefield119(){
  return doublefield119;
  }
  public void setDoublefield119(double value){
  this.doublefield119 = value;
  }
  public double getDoublefield120(){
  return doublefield120;
  }
  public void setDoublefield120(double value){
  this.doublefield120 = value;
  }
  public double getDoublefield121(){
  return doublefield121;
  }
  public void setDoublefield121(double value){
  this.doublefield121 = value;
  }
  public double getDoublefield122(){
  return doublefield122;
  }
  public void setDoublefield122(double value){
  this.doublefield122 = value;
  }
  public double getDoublefield123(){
  return doublefield123;
  }
  public void setDoublefield123(double value){
  this.doublefield123 = value;
  }
  public double getDoublefield124(){
  return doublefield124;
  }
  public void setDoublefield124(double value){
  this.doublefield124 = value;
  }
  public double getDoublefield125(){
  return doublefield125;
  }
  public void setDoublefield125(double value){
  this.doublefield125 = value;
  }
  public double getDoublefield126(){
  return doublefield126;
  }
  public void setDoublefield126(double value){
  this.doublefield126 = value;
  }
  public double getDoublefield127(){
  return doublefield127;
  }
  public void setDoublefield127(double value){
  this.doublefield127 = value;
  }
  public double getDoublefield128(){
  return doublefield128;
  }
  public void setDoublefield128(double value){
  this.doublefield128 = value;
  }
  public double getDoublefield129(){
  return doublefield129;
  }
  public void setDoublefield129(double value){
  this.doublefield129 = value;
  }
  public double getDoublefield130(){
  return doublefield130;
  }
  public void setDoublefield130(double value){
  this.doublefield130 = value;
  }
  public double getDoublefield131(){
  return doublefield131;
  }
  public void setDoublefield131(double value){
  this.doublefield131 = value;
  }
  public double getDoublefield132(){
  return doublefield132;
  }
  public void setDoublefield132(double value){
  this.doublefield132 = value;
  }
  public double getDoublefield133(){
  return doublefield133;
  }
  public void setDoublefield133(double value){
  this.doublefield133 = value;
  }
  public double getDoublefield134(){
  return doublefield134;
  }
  public void setDoublefield134(double value){
  this.doublefield134 = value;
  }
  public double getDoublefield135(){
  return doublefield135;
  }
  public void setDoublefield135(double value){
  this.doublefield135 = value;
  }
  public double getDoublefield136(){
  return doublefield136;
  }
  public void setDoublefield136(double value){
  this.doublefield136 = value;
  }
  public double getDoublefield137(){
  return doublefield137;
  }
  public void setDoublefield137(double value){
  this.doublefield137 = value;
  }
  public double getDoublefield138(){
  return doublefield138;
  }
  public void setDoublefield138(double value){
  this.doublefield138 = value;
  }
  public double getDoublefield139(){
  return doublefield139;
  }
  public void setDoublefield139(double value){
  this.doublefield139 = value;
  }
  public double getDoublefield140(){
  return doublefield140;
  }
  public void setDoublefield140(double value){
  this.doublefield140 = value;
  }
  public double getDoublefield141(){
  return doublefield141;
  }
  public void setDoublefield141(double value){
  this.doublefield141 = value;
  }
  public double getDoublefield142(){
  return doublefield142;
  }
  public void setDoublefield142(double value){
  this.doublefield142 = value;
  }
  public double getDoublefield143(){
  return doublefield143;
  }
  public void setDoublefield143(double value){
  this.doublefield143 = value;
  }
  public double getDoublefield144(){
  return doublefield144;
  }
  public void setDoublefield144(double value){
  this.doublefield144 = value;
  }
  public double getDoublefield145(){
  return doublefield145;
  }
  public void setDoublefield145(double value){
  this.doublefield145 = value;
  }
  public double getDoublefield146(){
  return doublefield146;
  }
  public void setDoublefield146(double value){
  this.doublefield146 = value;
  }
  public double getDoublefield147(){
  return doublefield147;
  }
  public void setDoublefield147(double value){
  this.doublefield147 = value;
  }
  public double getDoublefield148(){
  return doublefield148;
  }
  public void setDoublefield148(double value){
  this.doublefield148 = value;
  }
  public double getDoublefield149(){
  return doublefield149;
  }
  public void setDoublefield149(double value){
  this.doublefield149 = value;
  }
  public double getDoublefield150(){
  return doublefield150;
  }
  public void setDoublefield150(double value){
  this.doublefield150 = value;
  }
  public double getDoublefield151(){
  return doublefield151;
  }
  public void setDoublefield151(double value){
  this.doublefield151 = value;
  }
  public double getDoublefield152(){
  return doublefield152;
  }
  public void setDoublefield152(double value){
  this.doublefield152 = value;
  }
  public double getDoublefield153(){
  return doublefield153;
  }
  public void setDoublefield153(double value){
  this.doublefield153 = value;
  }
  public double getDoublefield154(){
  return doublefield154;
  }
  public void setDoublefield154(double value){
  this.doublefield154 = value;
  }
  public double getDoublefield155(){
  return doublefield155;
  }
  public void setDoublefield155(double value){
  this.doublefield155 = value;
  }
  public double getDoublefield156(){
  return doublefield156;
  }
  public void setDoublefield156(double value){
  this.doublefield156 = value;
  }
  public double getDoublefield157(){
  return doublefield157;
  }
  public void setDoublefield157(double value){
  this.doublefield157 = value;
  }
  public double getDoublefield158(){
  return doublefield158;
  }
  public void setDoublefield158(double value){
  this.doublefield158 = value;
  }
  public double getDoublefield159(){
  return doublefield159;
  }
  public void setDoublefield159(double value){
  this.doublefield159 = value;
  }
  public byte getBytefield0(){
  return bytefield0;
  }
  public void setBytefield0(byte value){
  this.bytefield0 = value;
  }
  public byte getBytefield1(){
  return bytefield1;
  }
  public void setBytefield1(byte value){
  this.bytefield1 = value;
  }
  public byte getBytefield2(){
  return bytefield2;
  }
  public void setBytefield2(byte value){
  this.bytefield2 = value;
  }
  public byte getBytefield3(){
  return bytefield3;
  }
  public void setBytefield3(byte value){
  this.bytefield3 = value;
  }
  public byte getBytefield4(){
  return bytefield4;
  }
  public void setBytefield4(byte value){
  this.bytefield4 = value;
  }
  public byte getBytefield5(){
  return bytefield5;
  }
  public void setBytefield5(byte value){
  this.bytefield5 = value;
  }
  public byte getBytefield6(){
  return bytefield6;
  }
  public void setBytefield6(byte value){
  this.bytefield6 = value;
  }
  public byte getBytefield7(){
  return bytefield7;
  }
  public void setBytefield7(byte value){
  this.bytefield7 = value;
  }
  public byte getBytefield8(){
  return bytefield8;
  }
  public void setBytefield8(byte value){
  this.bytefield8 = value;
  }
  public byte getBytefield9(){
  return bytefield9;
  }
  public void setBytefield9(byte value){
  this.bytefield9 = value;
  }
  public byte getBytefield10(){
  return bytefield10;
  }
  public void setBytefield10(byte value){
  this.bytefield10 = value;
  }
  public byte getBytefield11(){
  return bytefield11;
  }
  public void setBytefield11(byte value){
  this.bytefield11 = value;
  }
  public byte getBytefield12(){
  return bytefield12;
  }
  public void setBytefield12(byte value){
  this.bytefield12 = value;
  }
  public byte getBytefield13(){
  return bytefield13;
  }
  public void setBytefield13(byte value){
  this.bytefield13 = value;
  }
  public byte getBytefield14(){
  return bytefield14;
  }
  public void setBytefield14(byte value){
  this.bytefield14 = value;
  }
  public byte getBytefield15(){
  return bytefield15;
  }
  public void setBytefield15(byte value){
  this.bytefield15 = value;
  }
  public byte getBytefield16(){
  return bytefield16;
  }
  public void setBytefield16(byte value){
  this.bytefield16 = value;
  }
  public byte getBytefield17(){
  return bytefield17;
  }
  public void setBytefield17(byte value){
  this.bytefield17 = value;
  }
  public byte getBytefield18(){
  return bytefield18;
  }
  public void setBytefield18(byte value){
  this.bytefield18 = value;
  }
  public byte getBytefield19(){
  return bytefield19;
  }
  public void setBytefield19(byte value){
  this.bytefield19 = value;
  }
  public byte getBytefield20(){
  return bytefield20;
  }
  public void setBytefield20(byte value){
  this.bytefield20 = value;
  }
  public byte getBytefield21(){
  return bytefield21;
  }
  public void setBytefield21(byte value){
  this.bytefield21 = value;
  }
  public byte getBytefield22(){
  return bytefield22;
  }
  public void setBytefield22(byte value){
  this.bytefield22 = value;
  }
  public byte getBytefield23(){
  return bytefield23;
  }
  public void setBytefield23(byte value){
  this.bytefield23 = value;
  }
  public byte getBytefield24(){
  return bytefield24;
  }
  public void setBytefield24(byte value){
  this.bytefield24 = value;
  }
  public byte getBytefield25(){
  return bytefield25;
  }
  public void setBytefield25(byte value){
  this.bytefield25 = value;
  }
  public byte getBytefield26(){
  return bytefield26;
  }
  public void setBytefield26(byte value){
  this.bytefield26 = value;
  }
  public byte getBytefield27(){
  return bytefield27;
  }
  public void setBytefield27(byte value){
  this.bytefield27 = value;
  }
  public byte getBytefield28(){
  return bytefield28;
  }
  public void setBytefield28(byte value){
  this.bytefield28 = value;
  }
  public byte getBytefield29(){
  return bytefield29;
  }
  public void setBytefield29(byte value){
  this.bytefield29 = value;
  }
  public byte getBytefield30(){
  return bytefield30;
  }
  public void setBytefield30(byte value){
  this.bytefield30 = value;
  }
  public byte getBytefield31(){
  return bytefield31;
  }
  public void setBytefield31(byte value){
  this.bytefield31 = value;
  }
  public byte getBytefield32(){
  return bytefield32;
  }
  public void setBytefield32(byte value){
  this.bytefield32 = value;
  }
  public byte getBytefield33(){
  return bytefield33;
  }
  public void setBytefield33(byte value){
  this.bytefield33 = value;
  }
  public byte getBytefield34(){
  return bytefield34;
  }
  public void setBytefield34(byte value){
  this.bytefield34 = value;
  }
  public byte getBytefield35(){
  return bytefield35;
  }
  public void setBytefield35(byte value){
  this.bytefield35 = value;
  }
  public byte getBytefield36(){
  return bytefield36;
  }
  public void setBytefield36(byte value){
  this.bytefield36 = value;
  }
  public byte getBytefield37(){
  return bytefield37;
  }
  public void setBytefield37(byte value){
  this.bytefield37 = value;
  }
  public byte getBytefield38(){
  return bytefield38;
  }
  public void setBytefield38(byte value){
  this.bytefield38 = value;
  }
  public byte getBytefield39(){
  return bytefield39;
  }
  public void setBytefield39(byte value){
  this.bytefield39 = value;
  }
  public byte getBytefield40(){
  return bytefield40;
  }
  public void setBytefield40(byte value){
  this.bytefield40 = value;
  }
  public byte getBytefield41(){
  return bytefield41;
  }
  public void setBytefield41(byte value){
  this.bytefield41 = value;
  }
  public byte getBytefield42(){
  return bytefield42;
  }
  public void setBytefield42(byte value){
  this.bytefield42 = value;
  }
  public byte getBytefield43(){
  return bytefield43;
  }
  public void setBytefield43(byte value){
  this.bytefield43 = value;
  }
  public byte getBytefield44(){
  return bytefield44;
  }
  public void setBytefield44(byte value){
  this.bytefield44 = value;
  }
  public byte getBytefield45(){
  return bytefield45;
  }
  public void setBytefield45(byte value){
  this.bytefield45 = value;
  }
  public byte getBytefield46(){
  return bytefield46;
  }
  public void setBytefield46(byte value){
  this.bytefield46 = value;
  }
  public byte getBytefield47(){
  return bytefield47;
  }
  public void setBytefield47(byte value){
  this.bytefield47 = value;
  }
  public byte getBytefield48(){
  return bytefield48;
  }
  public void setBytefield48(byte value){
  this.bytefield48 = value;
  }
  public byte getBytefield49(){
  return bytefield49;
  }
  public void setBytefield49(byte value){
  this.bytefield49 = value;
  }
  public byte getBytefield50(){
  return bytefield50;
  }
  public void setBytefield50(byte value){
  this.bytefield50 = value;
  }
  public byte getBytefield51(){
  return bytefield51;
  }
  public void setBytefield51(byte value){
  this.bytefield51 = value;
  }
  public byte getBytefield52(){
  return bytefield52;
  }
  public void setBytefield52(byte value){
  this.bytefield52 = value;
  }
  public byte getBytefield53(){
  return bytefield53;
  }
  public void setBytefield53(byte value){
  this.bytefield53 = value;
  }
  public byte getBytefield54(){
  return bytefield54;
  }
  public void setBytefield54(byte value){
  this.bytefield54 = value;
  }
  public byte getBytefield55(){
  return bytefield55;
  }
  public void setBytefield55(byte value){
  this.bytefield55 = value;
  }
  public byte getBytefield56(){
  return bytefield56;
  }
  public void setBytefield56(byte value){
  this.bytefield56 = value;
  }
  public byte getBytefield57(){
  return bytefield57;
  }
  public void setBytefield57(byte value){
  this.bytefield57 = value;
  }
  public byte getBytefield58(){
  return bytefield58;
  }
  public void setBytefield58(byte value){
  this.bytefield58 = value;
  }
  public byte getBytefield59(){
  return bytefield59;
  }
  public void setBytefield59(byte value){
  this.bytefield59 = value;
  }
  public byte getBytefield60(){
  return bytefield60;
  }
  public void setBytefield60(byte value){
  this.bytefield60 = value;
  }
  public byte getBytefield61(){
  return bytefield61;
  }
  public void setBytefield61(byte value){
  this.bytefield61 = value;
  }
  public byte getBytefield62(){
  return bytefield62;
  }
  public void setBytefield62(byte value){
  this.bytefield62 = value;
  }
  public byte getBytefield63(){
  return bytefield63;
  }
  public void setBytefield63(byte value){
  this.bytefield63 = value;
  }
  public byte getBytefield64(){
  return bytefield64;
  }
  public void setBytefield64(byte value){
  this.bytefield64 = value;
  }
  public byte getBytefield65(){
  return bytefield65;
  }
  public void setBytefield65(byte value){
  this.bytefield65 = value;
  }
  public byte getBytefield66(){
  return bytefield66;
  }
  public void setBytefield66(byte value){
  this.bytefield66 = value;
  }
  public byte getBytefield67(){
  return bytefield67;
  }
  public void setBytefield67(byte value){
  this.bytefield67 = value;
  }
  public byte getBytefield68(){
  return bytefield68;
  }
  public void setBytefield68(byte value){
  this.bytefield68 = value;
  }
  public byte getBytefield69(){
  return bytefield69;
  }
  public void setBytefield69(byte value){
  this.bytefield69 = value;
  }
  public byte getBytefield70(){
  return bytefield70;
  }
  public void setBytefield70(byte value){
  this.bytefield70 = value;
  }
  public byte getBytefield71(){
  return bytefield71;
  }
  public void setBytefield71(byte value){
  this.bytefield71 = value;
  }
  public byte getBytefield72(){
  return bytefield72;
  }
  public void setBytefield72(byte value){
  this.bytefield72 = value;
  }
  public byte getBytefield73(){
  return bytefield73;
  }
  public void setBytefield73(byte value){
  this.bytefield73 = value;
  }
  public byte getBytefield74(){
  return bytefield74;
  }
  public void setBytefield74(byte value){
  this.bytefield74 = value;
  }
  public byte getBytefield75(){
  return bytefield75;
  }
  public void setBytefield75(byte value){
  this.bytefield75 = value;
  }
  public byte getBytefield76(){
  return bytefield76;
  }
  public void setBytefield76(byte value){
  this.bytefield76 = value;
  }
  public byte getBytefield77(){
  return bytefield77;
  }
  public void setBytefield77(byte value){
  this.bytefield77 = value;
  }
  public byte getBytefield78(){
  return bytefield78;
  }
  public void setBytefield78(byte value){
  this.bytefield78 = value;
  }
  public byte getBytefield79(){
  return bytefield79;
  }
  public void setBytefield79(byte value){
  this.bytefield79 = value;
  }
  public byte getBytefield80(){
  return bytefield80;
  }
  public void setBytefield80(byte value){
  this.bytefield80 = value;
  }
  public byte getBytefield81(){
  return bytefield81;
  }
  public void setBytefield81(byte value){
  this.bytefield81 = value;
  }
  public byte getBytefield82(){
  return bytefield82;
  }
  public void setBytefield82(byte value){
  this.bytefield82 = value;
  }
  public byte getBytefield83(){
  return bytefield83;
  }
  public void setBytefield83(byte value){
  this.bytefield83 = value;
  }
  public byte getBytefield84(){
  return bytefield84;
  }
  public void setBytefield84(byte value){
  this.bytefield84 = value;
  }
  public byte getBytefield85(){
  return bytefield85;
  }
  public void setBytefield85(byte value){
  this.bytefield85 = value;
  }
  public byte getBytefield86(){
  return bytefield86;
  }
  public void setBytefield86(byte value){
  this.bytefield86 = value;
  }
  public byte getBytefield87(){
  return bytefield87;
  }
  public void setBytefield87(byte value){
  this.bytefield87 = value;
  }
  public byte getBytefield88(){
  return bytefield88;
  }
  public void setBytefield88(byte value){
  this.bytefield88 = value;
  }
  public byte getBytefield89(){
  return bytefield89;
  }
  public void setBytefield89(byte value){
  this.bytefield89 = value;
  }
  public byte getBytefield90(){
  return bytefield90;
  }
  public void setBytefield90(byte value){
  this.bytefield90 = value;
  }
  public byte getBytefield91(){
  return bytefield91;
  }
  public void setBytefield91(byte value){
  this.bytefield91 = value;
  }
  public byte getBytefield92(){
  return bytefield92;
  }
  public void setBytefield92(byte value){
  this.bytefield92 = value;
  }
  public byte getBytefield93(){
  return bytefield93;
  }
  public void setBytefield93(byte value){
  this.bytefield93 = value;
  }
  public byte getBytefield94(){
  return bytefield94;
  }
  public void setBytefield94(byte value){
  this.bytefield94 = value;
  }
  public byte getBytefield95(){
  return bytefield95;
  }
  public void setBytefield95(byte value){
  this.bytefield95 = value;
  }
  public byte getBytefield96(){
  return bytefield96;
  }
  public void setBytefield96(byte value){
  this.bytefield96 = value;
  }
  public byte getBytefield97(){
  return bytefield97;
  }
  public void setBytefield97(byte value){
  this.bytefield97 = value;
  }
  public byte getBytefield98(){
  return bytefield98;
  }
  public void setBytefield98(byte value){
  this.bytefield98 = value;
  }
  public byte getBytefield99(){
  return bytefield99;
  }
  public void setBytefield99(byte value){
  this.bytefield99 = value;
  }
  public byte getBytefield100(){
  return bytefield100;
  }
  public void setBytefield100(byte value){
  this.bytefield100 = value;
  }
  public byte getBytefield101(){
  return bytefield101;
  }
  public void setBytefield101(byte value){
  this.bytefield101 = value;
  }
  public byte getBytefield102(){
  return bytefield102;
  }
  public void setBytefield102(byte value){
  this.bytefield102 = value;
  }
  public byte getBytefield103(){
  return bytefield103;
  }
  public void setBytefield103(byte value){
  this.bytefield103 = value;
  }
  public byte getBytefield104(){
  return bytefield104;
  }
  public void setBytefield104(byte value){
  this.bytefield104 = value;
  }
  public byte getBytefield105(){
  return bytefield105;
  }
  public void setBytefield105(byte value){
  this.bytefield105 = value;
  }
  public byte getBytefield106(){
  return bytefield106;
  }
  public void setBytefield106(byte value){
  this.bytefield106 = value;
  }
  public byte getBytefield107(){
  return bytefield107;
  }
  public void setBytefield107(byte value){
  this.bytefield107 = value;
  }
  public byte getBytefield108(){
  return bytefield108;
  }
  public void setBytefield108(byte value){
  this.bytefield108 = value;
  }
  public byte getBytefield109(){
  return bytefield109;
  }
  public void setBytefield109(byte value){
  this.bytefield109 = value;
  }
  public byte getBytefield110(){
  return bytefield110;
  }
  public void setBytefield110(byte value){
  this.bytefield110 = value;
  }
  public byte getBytefield111(){
  return bytefield111;
  }
  public void setBytefield111(byte value){
  this.bytefield111 = value;
  }
  public byte getBytefield112(){
  return bytefield112;
  }
  public void setBytefield112(byte value){
  this.bytefield112 = value;
  }
  public byte getBytefield113(){
  return bytefield113;
  }
  public void setBytefield113(byte value){
  this.bytefield113 = value;
  }
  public byte getBytefield114(){
  return bytefield114;
  }
  public void setBytefield114(byte value){
  this.bytefield114 = value;
  }
  public byte getBytefield115(){
  return bytefield115;
  }
  public void setBytefield115(byte value){
  this.bytefield115 = value;
  }
  public byte getBytefield116(){
  return bytefield116;
  }
  public void setBytefield116(byte value){
  this.bytefield116 = value;
  }
  public byte getBytefield117(){
  return bytefield117;
  }
  public void setBytefield117(byte value){
  this.bytefield117 = value;
  }
  public byte getBytefield118(){
  return bytefield118;
  }
  public void setBytefield118(byte value){
  this.bytefield118 = value;
  }
  public byte getBytefield119(){
  return bytefield119;
  }
  public void setBytefield119(byte value){
  this.bytefield119 = value;
  }
  public byte getBytefield120(){
  return bytefield120;
  }
  public void setBytefield120(byte value){
  this.bytefield120 = value;
  }
  public byte getBytefield121(){
  return bytefield121;
  }
  public void setBytefield121(byte value){
  this.bytefield121 = value;
  }
  public byte getBytefield122(){
  return bytefield122;
  }
  public void setBytefield122(byte value){
  this.bytefield122 = value;
  }
  public byte getBytefield123(){
  return bytefield123;
  }
  public void setBytefield123(byte value){
  this.bytefield123 = value;
  }
  public byte getBytefield124(){
  return bytefield124;
  }
  public void setBytefield124(byte value){
  this.bytefield124 = value;
  }
  public byte getBytefield125(){
  return bytefield125;
  }
  public void setBytefield125(byte value){
  this.bytefield125 = value;
  }
  public byte getBytefield126(){
  return bytefield126;
  }
  public void setBytefield126(byte value){
  this.bytefield126 = value;
  }
  public byte getBytefield127(){
  return bytefield127;
  }
  public void setBytefield127(byte value){
  this.bytefield127 = value;
  }
  public byte getBytefield128(){
  return bytefield128;
  }
  public void setBytefield128(byte value){
  this.bytefield128 = value;
  }
  public byte getBytefield129(){
  return bytefield129;
  }
  public void setBytefield129(byte value){
  this.bytefield129 = value;
  }
  public byte getBytefield130(){
  return bytefield130;
  }
  public void setBytefield130(byte value){
  this.bytefield130 = value;
  }
  public byte getBytefield131(){
  return bytefield131;
  }
  public void setBytefield131(byte value){
  this.bytefield131 = value;
  }
  public byte getBytefield132(){
  return bytefield132;
  }
  public void setBytefield132(byte value){
  this.bytefield132 = value;
  }
  public byte getBytefield133(){
  return bytefield133;
  }
  public void setBytefield133(byte value){
  this.bytefield133 = value;
  }
  public byte getBytefield134(){
  return bytefield134;
  }
  public void setBytefield134(byte value){
  this.bytefield134 = value;
  }
  public byte getBytefield135(){
  return bytefield135;
  }
  public void setBytefield135(byte value){
  this.bytefield135 = value;
  }
  public byte getBytefield136(){
  return bytefield136;
  }
  public void setBytefield136(byte value){
  this.bytefield136 = value;
  }
  public byte getBytefield137(){
  return bytefield137;
  }
  public void setBytefield137(byte value){
  this.bytefield137 = value;
  }
  public byte getBytefield138(){
  return bytefield138;
  }
  public void setBytefield138(byte value){
  this.bytefield138 = value;
  }
  public byte getBytefield139(){
  return bytefield139;
  }
  public void setBytefield139(byte value){
  this.bytefield139 = value;
  }
  public byte getBytefield140(){
  return bytefield140;
  }
  public void setBytefield140(byte value){
  this.bytefield140 = value;
  }
  public byte getBytefield141(){
  return bytefield141;
  }
  public void setBytefield141(byte value){
  this.bytefield141 = value;
  }
  public byte getBytefield142(){
  return bytefield142;
  }
  public void setBytefield142(byte value){
  this.bytefield142 = value;
  }
  public byte getBytefield143(){
  return bytefield143;
  }
  public void setBytefield143(byte value){
  this.bytefield143 = value;
  }
  public byte getBytefield144(){
  return bytefield144;
  }
  public void setBytefield144(byte value){
  this.bytefield144 = value;
  }
  public byte getBytefield145(){
  return bytefield145;
  }
  public void setBytefield145(byte value){
  this.bytefield145 = value;
  }
  public byte getBytefield146(){
  return bytefield146;
  }
  public void setBytefield146(byte value){
  this.bytefield146 = value;
  }
  public byte getBytefield147(){
  return bytefield147;
  }
  public void setBytefield147(byte value){
  this.bytefield147 = value;
  }
  public byte getBytefield148(){
  return bytefield148;
  }
  public void setBytefield148(byte value){
  this.bytefield148 = value;
  }
  public byte getBytefield149(){
  return bytefield149;
  }
  public void setBytefield149(byte value){
  this.bytefield149 = value;
  }
  public char getCharfield0(){
  return charfield0;
  }
  public void setCharfield0(char value){
  this.charfield0 = value;
  }
  public char getCharfield1(){
  return charfield1;
  }
  public void setCharfield1(char value){
  this.charfield1 = value;
  }
  public char getCharfield2(){
  return charfield2;
  }
  public void setCharfield2(char value){
  this.charfield2 = value;
  }
  public char getCharfield3(){
  return charfield3;
  }
  public void setCharfield3(char value){
  this.charfield3 = value;
  }
  public char getCharfield4(){
  return charfield4;
  }
  public void setCharfield4(char value){
  this.charfield4 = value;
  }
  public char getCharfield5(){
  return charfield5;
  }
  public void setCharfield5(char value){
  this.charfield5 = value;
  }
  public char getCharfield6(){
  return charfield6;
  }
  public void setCharfield6(char value){
  this.charfield6 = value;
  }
  public char getCharfield7(){
  return charfield7;
  }
  public void setCharfield7(char value){
  this.charfield7 = value;
  }
  public char getCharfield8(){
  return charfield8;
  }
  public void setCharfield8(char value){
  this.charfield8 = value;
  }
  public char getCharfield9(){
  return charfield9;
  }
  public void setCharfield9(char value){
  this.charfield9 = value;
  }
  public char getCharfield10(){
  return charfield10;
  }
  public void setCharfield10(char value){
  this.charfield10 = value;
  }
  public char getCharfield11(){
  return charfield11;
  }
  public void setCharfield11(char value){
  this.charfield11 = value;
  }
  public char getCharfield12(){
  return charfield12;
  }
  public void setCharfield12(char value){
  this.charfield12 = value;
  }
  public char getCharfield13(){
  return charfield13;
  }
  public void setCharfield13(char value){
  this.charfield13 = value;
  }
  public char getCharfield14(){
  return charfield14;
  }
  public void setCharfield14(char value){
  this.charfield14 = value;
  }
  public char getCharfield15(){
  return charfield15;
  }
  public void setCharfield15(char value){
  this.charfield15 = value;
  }
  public char getCharfield16(){
  return charfield16;
  }
  public void setCharfield16(char value){
  this.charfield16 = value;
  }
  public char getCharfield17(){
  return charfield17;
  }
  public void setCharfield17(char value){
  this.charfield17 = value;
  }
  public char getCharfield18(){
  return charfield18;
  }
  public void setCharfield18(char value){
  this.charfield18 = value;
  }
  public char getCharfield19(){
  return charfield19;
  }
  public void setCharfield19(char value){
  this.charfield19 = value;
  }
  public char getCharfield20(){
  return charfield20;
  }
  public void setCharfield20(char value){
  this.charfield20 = value;
  }
  public char getCharfield21(){
  return charfield21;
  }
  public void setCharfield21(char value){
  this.charfield21 = value;
  }
  public char getCharfield22(){
  return charfield22;
  }
  public void setCharfield22(char value){
  this.charfield22 = value;
  }
  public char getCharfield23(){
  return charfield23;
  }
  public void setCharfield23(char value){
  this.charfield23 = value;
  }
  public char getCharfield24(){
  return charfield24;
  }
  public void setCharfield24(char value){
  this.charfield24 = value;
  }
  public char getCharfield25(){
  return charfield25;
  }
  public void setCharfield25(char value){
  this.charfield25 = value;
  }
  public char getCharfield26(){
  return charfield26;
  }
  public void setCharfield26(char value){
  this.charfield26 = value;
  }
  public char getCharfield27(){
  return charfield27;
  }
  public void setCharfield27(char value){
  this.charfield27 = value;
  }
  public char getCharfield28(){
  return charfield28;
  }
  public void setCharfield28(char value){
  this.charfield28 = value;
  }
  public char getCharfield29(){
  return charfield29;
  }
  public void setCharfield29(char value){
  this.charfield29 = value;
  }
  public char getCharfield30(){
  return charfield30;
  }
  public void setCharfield30(char value){
  this.charfield30 = value;
  }
  public char getCharfield31(){
  return charfield31;
  }
  public void setCharfield31(char value){
  this.charfield31 = value;
  }
  public char getCharfield32(){
  return charfield32;
  }
  public void setCharfield32(char value){
  this.charfield32 = value;
  }
  public char getCharfield33(){
  return charfield33;
  }
  public void setCharfield33(char value){
  this.charfield33 = value;
  }
  public char getCharfield34(){
  return charfield34;
  }
  public void setCharfield34(char value){
  this.charfield34 = value;
  }
  public char getCharfield35(){
  return charfield35;
  }
  public void setCharfield35(char value){
  this.charfield35 = value;
  }
  public char getCharfield36(){
  return charfield36;
  }
  public void setCharfield36(char value){
  this.charfield36 = value;
  }
  public char getCharfield37(){
  return charfield37;
  }
  public void setCharfield37(char value){
  this.charfield37 = value;
  }
  public char getCharfield38(){
  return charfield38;
  }
  public void setCharfield38(char value){
  this.charfield38 = value;
  }
  public char getCharfield39(){
  return charfield39;
  }
  public void setCharfield39(char value){
  this.charfield39 = value;
  }
  public char getCharfield40(){
  return charfield40;
  }
  public void setCharfield40(char value){
  this.charfield40 = value;
  }
  public char getCharfield41(){
  return charfield41;
  }
  public void setCharfield41(char value){
  this.charfield41 = value;
  }
  public char getCharfield42(){
  return charfield42;
  }
  public void setCharfield42(char value){
  this.charfield42 = value;
  }
  public char getCharfield43(){
  return charfield43;
  }
  public void setCharfield43(char value){
  this.charfield43 = value;
  }
  public char getCharfield44(){
  return charfield44;
  }
  public void setCharfield44(char value){
  this.charfield44 = value;
  }
  public char getCharfield45(){
  return charfield45;
  }
  public void setCharfield45(char value){
  this.charfield45 = value;
  }
  public char getCharfield46(){
  return charfield46;
  }
  public void setCharfield46(char value){
  this.charfield46 = value;
  }
  public char getCharfield47(){
  return charfield47;
  }
  public void setCharfield47(char value){
  this.charfield47 = value;
  }
  public char getCharfield48(){
  return charfield48;
  }
  public void setCharfield48(char value){
  this.charfield48 = value;
  }
  public char getCharfield49(){
  return charfield49;
  }
  public void setCharfield49(char value){
  this.charfield49 = value;
  }
  public char getCharfield50(){
  return charfield50;
  }
  public void setCharfield50(char value){
  this.charfield50 = value;
  }
  public char getCharfield51(){
  return charfield51;
  }
  public void setCharfield51(char value){
  this.charfield51 = value;
  }
  public char getCharfield52(){
  return charfield52;
  }
  public void setCharfield52(char value){
  this.charfield52 = value;
  }
  public char getCharfield53(){
  return charfield53;
  }
  public void setCharfield53(char value){
  this.charfield53 = value;
  }
  public char getCharfield54(){
  return charfield54;
  }
  public void setCharfield54(char value){
  this.charfield54 = value;
  }
  public char getCharfield55(){
  return charfield55;
  }
  public void setCharfield55(char value){
  this.charfield55 = value;
  }
  public char getCharfield56(){
  return charfield56;
  }
  public void setCharfield56(char value){
  this.charfield56 = value;
  }
  public char getCharfield57(){
  return charfield57;
  }
  public void setCharfield57(char value){
  this.charfield57 = value;
  }
  public char getCharfield58(){
  return charfield58;
  }
  public void setCharfield58(char value){
  this.charfield58 = value;
  }
  public char getCharfield59(){
  return charfield59;
  }
  public void setCharfield59(char value){
  this.charfield59 = value;
  }
  public char getCharfield60(){
  return charfield60;
  }
  public void setCharfield60(char value){
  this.charfield60 = value;
  }
  public char getCharfield61(){
  return charfield61;
  }
  public void setCharfield61(char value){
  this.charfield61 = value;
  }
  public char getCharfield62(){
  return charfield62;
  }
  public void setCharfield62(char value){
  this.charfield62 = value;
  }
  public char getCharfield63(){
  return charfield63;
  }
  public void setCharfield63(char value){
  this.charfield63 = value;
  }
  public char getCharfield64(){
  return charfield64;
  }
  public void setCharfield64(char value){
  this.charfield64 = value;
  }
  public char getCharfield65(){
  return charfield65;
  }
  public void setCharfield65(char value){
  this.charfield65 = value;
  }
  public char getCharfield66(){
  return charfield66;
  }
  public void setCharfield66(char value){
  this.charfield66 = value;
  }
  public char getCharfield67(){
  return charfield67;
  }
  public void setCharfield67(char value){
  this.charfield67 = value;
  }
  public char getCharfield68(){
  return charfield68;
  }
  public void setCharfield68(char value){
  this.charfield68 = value;
  }
  public char getCharfield69(){
  return charfield69;
  }
  public void setCharfield69(char value){
  this.charfield69 = value;
  }
  public char getCharfield70(){
  return charfield70;
  }
  public void setCharfield70(char value){
  this.charfield70 = value;
  }
  public char getCharfield71(){
  return charfield71;
  }
  public void setCharfield71(char value){
  this.charfield71 = value;
  }
  public char getCharfield72(){
  return charfield72;
  }
  public void setCharfield72(char value){
  this.charfield72 = value;
  }
  public char getCharfield73(){
  return charfield73;
  }
  public void setCharfield73(char value){
  this.charfield73 = value;
  }
  public char getCharfield74(){
  return charfield74;
  }
  public void setCharfield74(char value){
  this.charfield74 = value;
  }
  public char getCharfield75(){
  return charfield75;
  }
  public void setCharfield75(char value){
  this.charfield75 = value;
  }
  public char getCharfield76(){
  return charfield76;
  }
  public void setCharfield76(char value){
  this.charfield76 = value;
  }
  public char getCharfield77(){
  return charfield77;
  }
  public void setCharfield77(char value){
  this.charfield77 = value;
  }
  public char getCharfield78(){
  return charfield78;
  }
  public void setCharfield78(char value){
  this.charfield78 = value;
  }
  public char getCharfield79(){
  return charfield79;
  }
  public void setCharfield79(char value){
  this.charfield79 = value;
  }
  public char getCharfield80(){
  return charfield80;
  }
  public void setCharfield80(char value){
  this.charfield80 = value;
  }
  public char getCharfield81(){
  return charfield81;
  }
  public void setCharfield81(char value){
  this.charfield81 = value;
  }
  public char getCharfield82(){
  return charfield82;
  }
  public void setCharfield82(char value){
  this.charfield82 = value;
  }
  public char getCharfield83(){
  return charfield83;
  }
  public void setCharfield83(char value){
  this.charfield83 = value;
  }
  public char getCharfield84(){
  return charfield84;
  }
  public void setCharfield84(char value){
  this.charfield84 = value;
  }
  public char getCharfield85(){
  return charfield85;
  }
  public void setCharfield85(char value){
  this.charfield85 = value;
  }
  public char getCharfield86(){
  return charfield86;
  }
  public void setCharfield86(char value){
  this.charfield86 = value;
  }
  public char getCharfield87(){
  return charfield87;
  }
  public void setCharfield87(char value){
  this.charfield87 = value;
  }
  public char getCharfield88(){
  return charfield88;
  }
  public void setCharfield88(char value){
  this.charfield88 = value;
  }
  public char getCharfield89(){
  return charfield89;
  }
  public void setCharfield89(char value){
  this.charfield89 = value;
  }
  public char getCharfield90(){
  return charfield90;
  }
  public void setCharfield90(char value){
  this.charfield90 = value;
  }
  public char getCharfield91(){
  return charfield91;
  }
  public void setCharfield91(char value){
  this.charfield91 = value;
  }
  public char getCharfield92(){
  return charfield92;
  }
  public void setCharfield92(char value){
  this.charfield92 = value;
  }
  public char getCharfield93(){
  return charfield93;
  }
  public void setCharfield93(char value){
  this.charfield93 = value;
  }
  public char getCharfield94(){
  return charfield94;
  }
  public void setCharfield94(char value){
  this.charfield94 = value;
  }
  public char getCharfield95(){
  return charfield95;
  }
  public void setCharfield95(char value){
  this.charfield95 = value;
  }
  public char getCharfield96(){
  return charfield96;
  }
  public void setCharfield96(char value){
  this.charfield96 = value;
  }
  public char getCharfield97(){
  return charfield97;
  }
  public void setCharfield97(char value){
  this.charfield97 = value;
  }
  public char getCharfield98(){
  return charfield98;
  }
  public void setCharfield98(char value){
  this.charfield98 = value;
  }
  public char getCharfield99(){
  return charfield99;
  }
  public void setCharfield99(char value){
  this.charfield99 = value;
  }
  public char getCharfield100(){
  return charfield100;
  }
  public void setCharfield100(char value){
  this.charfield100 = value;
  }
  public char getCharfield101(){
  return charfield101;
  }
  public void setCharfield101(char value){
  this.charfield101 = value;
  }
  public char getCharfield102(){
  return charfield102;
  }
  public void setCharfield102(char value){
  this.charfield102 = value;
  }
  public char getCharfield103(){
  return charfield103;
  }
  public void setCharfield103(char value){
  this.charfield103 = value;
  }
  public char getCharfield104(){
  return charfield104;
  }
  public void setCharfield104(char value){
  this.charfield104 = value;
  }
  public char getCharfield105(){
  return charfield105;
  }
  public void setCharfield105(char value){
  this.charfield105 = value;
  }
  public char getCharfield106(){
  return charfield106;
  }
  public void setCharfield106(char value){
  this.charfield106 = value;
  }
  public char getCharfield107(){
  return charfield107;
  }
  public void setCharfield107(char value){
  this.charfield107 = value;
  }
  public char getCharfield108(){
  return charfield108;
  }
  public void setCharfield108(char value){
  this.charfield108 = value;
  }
  public char getCharfield109(){
  return charfield109;
  }
  public void setCharfield109(char value){
  this.charfield109 = value;
  }
  public char getCharfield110(){
  return charfield110;
  }
  public void setCharfield110(char value){
  this.charfield110 = value;
  }
  public char getCharfield111(){
  return charfield111;
  }
  public void setCharfield111(char value){
  this.charfield111 = value;
  }
  public char getCharfield112(){
  return charfield112;
  }
  public void setCharfield112(char value){
  this.charfield112 = value;
  }
  public char getCharfield113(){
  return charfield113;
  }
  public void setCharfield113(char value){
  this.charfield113 = value;
  }
  public char getCharfield114(){
  return charfield114;
  }
  public void setCharfield114(char value){
  this.charfield114 = value;
  }
  public char getCharfield115(){
  return charfield115;
  }
  public void setCharfield115(char value){
  this.charfield115 = value;
  }
  public char getCharfield116(){
  return charfield116;
  }
  public void setCharfield116(char value){
  this.charfield116 = value;
  }
  public char getCharfield117(){
  return charfield117;
  }
  public void setCharfield117(char value){
  this.charfield117 = value;
  }
  public char getCharfield118(){
  return charfield118;
  }
  public void setCharfield118(char value){
  this.charfield118 = value;
  }
  public char getCharfield119(){
  return charfield119;
  }
  public void setCharfield119(char value){
  this.charfield119 = value;
  }
  public char getCharfield120(){
  return charfield120;
  }
  public void setCharfield120(char value){
  this.charfield120 = value;
  }
  public char getCharfield121(){
  return charfield121;
  }
  public void setCharfield121(char value){
  this.charfield121 = value;
  }
  public char getCharfield122(){
  return charfield122;
  }
  public void setCharfield122(char value){
  this.charfield122 = value;
  }
  public char getCharfield123(){
  return charfield123;
  }
  public void setCharfield123(char value){
  this.charfield123 = value;
  }
  public char getCharfield124(){
  return charfield124;
  }
  public void setCharfield124(char value){
  this.charfield124 = value;
  }
  public char getCharfield125(){
  return charfield125;
  }
  public void setCharfield125(char value){
  this.charfield125 = value;
  }
  public char getCharfield126(){
  return charfield126;
  }
  public void setCharfield126(char value){
  this.charfield126 = value;
  }
  public char getCharfield127(){
  return charfield127;
  }
  public void setCharfield127(char value){
  this.charfield127 = value;
  }
  public char getCharfield128(){
  return charfield128;
  }
  public void setCharfield128(char value){
  this.charfield128 = value;
  }
  public char getCharfield129(){
  return charfield129;
  }
  public void setCharfield129(char value){
  this.charfield129 = value;
  }
  public char getCharfield130(){
  return charfield130;
  }
  public void setCharfield130(char value){
  this.charfield130 = value;
  }
  public char getCharfield131(){
  return charfield131;
  }
  public void setCharfield131(char value){
  this.charfield131 = value;
  }
  public char getCharfield132(){
  return charfield132;
  }
  public void setCharfield132(char value){
  this.charfield132 = value;
  }
  public char getCharfield133(){
  return charfield133;
  }
  public void setCharfield133(char value){
  this.charfield133 = value;
  }
  public char getCharfield134(){
  return charfield134;
  }
  public void setCharfield134(char value){
  this.charfield134 = value;
  }
  public char getCharfield135(){
  return charfield135;
  }
  public void setCharfield135(char value){
  this.charfield135 = value;
  }
  public char getCharfield136(){
  return charfield136;
  }
  public void setCharfield136(char value){
  this.charfield136 = value;
  }
  public char getCharfield137(){
  return charfield137;
  }
  public void setCharfield137(char value){
  this.charfield137 = value;
  }
  public char getCharfield138(){
  return charfield138;
  }
  public void setCharfield138(char value){
  this.charfield138 = value;
  }
  public char getCharfield139(){
  return charfield139;
  }
  public void setCharfield139(char value){
  this.charfield139 = value;
  }
  public char getCharfield140(){
  return charfield140;
  }
  public void setCharfield140(char value){
  this.charfield140 = value;
  }
  public char getCharfield141(){
  return charfield141;
  }
  public void setCharfield141(char value){
  this.charfield141 = value;
  }
  public char getCharfield142(){
  return charfield142;
  }
  public void setCharfield142(char value){
  this.charfield142 = value;
  }
  public char getCharfield143(){
  return charfield143;
  }
  public void setCharfield143(char value){
  this.charfield143 = value;
  }
  public char getCharfield144(){
  return charfield144;
  }
  public void setCharfield144(char value){
  this.charfield144 = value;
  }
  public char getCharfield145(){
  return charfield145;
  }
  public void setCharfield145(char value){
  this.charfield145 = value;
  }
  public char getCharfield146(){
  return charfield146;
  }
  public void setCharfield146(char value){
  this.charfield146 = value;
  }
  public char getCharfield147(){
  return charfield147;
  }
  public void setCharfield147(char value){
  this.charfield147 = value;
  }
  public char getCharfield148(){
  return charfield148;
  }
  public void setCharfield148(char value){
  this.charfield148 = value;
  }
  public char getCharfield149(){
  return charfield149;
  }
  public void setCharfield149(char value){
  this.charfield149 = value;
  }
  public boolean getBooleanfield0(){
  return booleanfield0;
  }
  public void setBooleanfield0(boolean value){
  this.booleanfield0 = value;
  }
  public boolean getBooleanfield1(){
  return booleanfield1;
  }
  public void setBooleanfield1(boolean value){
  this.booleanfield1 = value;
  }
  public boolean getBooleanfield2(){
  return booleanfield2;
  }
  public void setBooleanfield2(boolean value){
  this.booleanfield2 = value;
  }
  public boolean getBooleanfield3(){
  return booleanfield3;
  }
  public void setBooleanfield3(boolean value){
  this.booleanfield3 = value;
  }
  public boolean getBooleanfield4(){
  return booleanfield4;
  }
  public void setBooleanfield4(boolean value){
  this.booleanfield4 = value;
  }
  public boolean getBooleanfield5(){
  return booleanfield5;
  }
  public void setBooleanfield5(boolean value){
  this.booleanfield5 = value;
  }
  public boolean getBooleanfield6(){
  return booleanfield6;
  }
  public void setBooleanfield6(boolean value){
  this.booleanfield6 = value;
  }
  public boolean getBooleanfield7(){
  return booleanfield7;
  }
  public void setBooleanfield7(boolean value){
  this.booleanfield7 = value;
  }
  public boolean getBooleanfield8(){
  return booleanfield8;
  }
  public void setBooleanfield8(boolean value){
  this.booleanfield8 = value;
  }
  public boolean getBooleanfield9(){
  return booleanfield9;
  }
  public void setBooleanfield9(boolean value){
  this.booleanfield9 = value;
  }
  public boolean getBooleanfield10(){
  return booleanfield10;
  }
  public void setBooleanfield10(boolean value){
  this.booleanfield10 = value;
  }
  public boolean getBooleanfield11(){
  return booleanfield11;
  }
  public void setBooleanfield11(boolean value){
  this.booleanfield11 = value;
  }
  public boolean getBooleanfield12(){
  return booleanfield12;
  }
  public void setBooleanfield12(boolean value){
  this.booleanfield12 = value;
  }
  public boolean getBooleanfield13(){
  return booleanfield13;
  }
  public void setBooleanfield13(boolean value){
  this.booleanfield13 = value;
  }
  public boolean getBooleanfield14(){
  return booleanfield14;
  }
  public void setBooleanfield14(boolean value){
  this.booleanfield14 = value;
  }
  public boolean getBooleanfield15(){
  return booleanfield15;
  }
  public void setBooleanfield15(boolean value){
  this.booleanfield15 = value;
  }
  public boolean getBooleanfield16(){
  return booleanfield16;
  }
  public void setBooleanfield16(boolean value){
  this.booleanfield16 = value;
  }
  public boolean getBooleanfield17(){
  return booleanfield17;
  }
  public void setBooleanfield17(boolean value){
  this.booleanfield17 = value;
  }
  public boolean getBooleanfield18(){
  return booleanfield18;
  }
  public void setBooleanfield18(boolean value){
  this.booleanfield18 = value;
  }
  public boolean getBooleanfield19(){
  return booleanfield19;
  }
  public void setBooleanfield19(boolean value){
  this.booleanfield19 = value;
  }
  public boolean getBooleanfield20(){
  return booleanfield20;
  }
  public void setBooleanfield20(boolean value){
  this.booleanfield20 = value;
  }
  public boolean getBooleanfield21(){
  return booleanfield21;
  }
  public void setBooleanfield21(boolean value){
  this.booleanfield21 = value;
  }
  public boolean getBooleanfield22(){
  return booleanfield22;
  }
  public void setBooleanfield22(boolean value){
  this.booleanfield22 = value;
  }
  public boolean getBooleanfield23(){
  return booleanfield23;
  }
  public void setBooleanfield23(boolean value){
  this.booleanfield23 = value;
  }
  public boolean getBooleanfield24(){
  return booleanfield24;
  }
  public void setBooleanfield24(boolean value){
  this.booleanfield24 = value;
  }
  public boolean getBooleanfield25(){
  return booleanfield25;
  }
  public void setBooleanfield25(boolean value){
  this.booleanfield25 = value;
  }
  public boolean getBooleanfield26(){
  return booleanfield26;
  }
  public void setBooleanfield26(boolean value){
  this.booleanfield26 = value;
  }
  public boolean getBooleanfield27(){
  return booleanfield27;
  }
  public void setBooleanfield27(boolean value){
  this.booleanfield27 = value;
  }
  public boolean getBooleanfield28(){
  return booleanfield28;
  }
  public void setBooleanfield28(boolean value){
  this.booleanfield28 = value;
  }
  public boolean getBooleanfield29(){
  return booleanfield29;
  }
  public void setBooleanfield29(boolean value){
  this.booleanfield29 = value;
  }
  public boolean getBooleanfield30(){
  return booleanfield30;
  }
  public void setBooleanfield30(boolean value){
  this.booleanfield30 = value;
  }
  public boolean getBooleanfield31(){
  return booleanfield31;
  }
  public void setBooleanfield31(boolean value){
  this.booleanfield31 = value;
  }
  public boolean getBooleanfield32(){
  return booleanfield32;
  }
  public void setBooleanfield32(boolean value){
  this.booleanfield32 = value;
  }
  public boolean getBooleanfield33(){
  return booleanfield33;
  }
  public void setBooleanfield33(boolean value){
  this.booleanfield33 = value;
  }
  public boolean getBooleanfield34(){
  return booleanfield34;
  }
  public void setBooleanfield34(boolean value){
  this.booleanfield34 = value;
  }
  public boolean getBooleanfield35(){
  return booleanfield35;
  }
  public void setBooleanfield35(boolean value){
  this.booleanfield35 = value;
  }
  public boolean getBooleanfield36(){
  return booleanfield36;
  }
  public void setBooleanfield36(boolean value){
  this.booleanfield36 = value;
  }
  public boolean getBooleanfield37(){
  return booleanfield37;
  }
  public void setBooleanfield37(boolean value){
  this.booleanfield37 = value;
  }
  public boolean getBooleanfield38(){
  return booleanfield38;
  }
  public void setBooleanfield38(boolean value){
  this.booleanfield38 = value;
  }
  public boolean getBooleanfield39(){
  return booleanfield39;
  }
  public void setBooleanfield39(boolean value){
  this.booleanfield39 = value;
  }
  public boolean getBooleanfield40(){
  return booleanfield40;
  }
  public void setBooleanfield40(boolean value){
  this.booleanfield40 = value;
  }
  public boolean getBooleanfield41(){
  return booleanfield41;
  }
  public void setBooleanfield41(boolean value){
  this.booleanfield41 = value;
  }
  public boolean getBooleanfield42(){
  return booleanfield42;
  }
  public void setBooleanfield42(boolean value){
  this.booleanfield42 = value;
  }
  public boolean getBooleanfield43(){
  return booleanfield43;
  }
  public void setBooleanfield43(boolean value){
  this.booleanfield43 = value;
  }
  public boolean getBooleanfield44(){
  return booleanfield44;
  }
  public void setBooleanfield44(boolean value){
  this.booleanfield44 = value;
  }
  public boolean getBooleanfield45(){
  return booleanfield45;
  }
  public void setBooleanfield45(boolean value){
  this.booleanfield45 = value;
  }
  public boolean getBooleanfield46(){
  return booleanfield46;
  }
  public void setBooleanfield46(boolean value){
  this.booleanfield46 = value;
  }
  public boolean getBooleanfield47(){
  return booleanfield47;
  }
  public void setBooleanfield47(boolean value){
  this.booleanfield47 = value;
  }
  public boolean getBooleanfield48(){
  return booleanfield48;
  }
  public void setBooleanfield48(boolean value){
  this.booleanfield48 = value;
  }
  public boolean getBooleanfield49(){
  return booleanfield49;
  }
  public void setBooleanfield49(boolean value){
  this.booleanfield49 = value;
  }
  public boolean getBooleanfield50(){
  return booleanfield50;
  }
  public void setBooleanfield50(boolean value){
  this.booleanfield50 = value;
  }
  public boolean getBooleanfield51(){
  return booleanfield51;
  }
  public void setBooleanfield51(boolean value){
  this.booleanfield51 = value;
  }
  public boolean getBooleanfield52(){
  return booleanfield52;
  }
  public void setBooleanfield52(boolean value){
  this.booleanfield52 = value;
  }
  public boolean getBooleanfield53(){
  return booleanfield53;
  }
  public void setBooleanfield53(boolean value){
  this.booleanfield53 = value;
  }
  public boolean getBooleanfield54(){
  return booleanfield54;
  }
  public void setBooleanfield54(boolean value){
  this.booleanfield54 = value;
  }
  public boolean getBooleanfield55(){
  return booleanfield55;
  }
  public void setBooleanfield55(boolean value){
  this.booleanfield55 = value;
  }
  public boolean getBooleanfield56(){
  return booleanfield56;
  }
  public void setBooleanfield56(boolean value){
  this.booleanfield56 = value;
  }
  public boolean getBooleanfield57(){
  return booleanfield57;
  }
  public void setBooleanfield57(boolean value){
  this.booleanfield57 = value;
  }
  public boolean getBooleanfield58(){
  return booleanfield58;
  }
  public void setBooleanfield58(boolean value){
  this.booleanfield58 = value;
  }
  public boolean getBooleanfield59(){
  return booleanfield59;
  }
  public void setBooleanfield59(boolean value){
  this.booleanfield59 = value;
  }
  public boolean getBooleanfield60(){
  return booleanfield60;
  }
  public void setBooleanfield60(boolean value){
  this.booleanfield60 = value;
  }
  public boolean getBooleanfield61(){
  return booleanfield61;
  }
  public void setBooleanfield61(boolean value){
  this.booleanfield61 = value;
  }
  public boolean getBooleanfield62(){
  return booleanfield62;
  }
  public void setBooleanfield62(boolean value){
  this.booleanfield62 = value;
  }
  public boolean getBooleanfield63(){
  return booleanfield63;
  }
  public void setBooleanfield63(boolean value){
  this.booleanfield63 = value;
  }
  public boolean getBooleanfield64(){
  return booleanfield64;
  }
  public void setBooleanfield64(boolean value){
  this.booleanfield64 = value;
  }
  public boolean getBooleanfield65(){
  return booleanfield65;
  }
  public void setBooleanfield65(boolean value){
  this.booleanfield65 = value;
  }
  public boolean getBooleanfield66(){
  return booleanfield66;
  }
  public void setBooleanfield66(boolean value){
  this.booleanfield66 = value;
  }
  public boolean getBooleanfield67(){
  return booleanfield67;
  }
  public void setBooleanfield67(boolean value){
  this.booleanfield67 = value;
  }
  public boolean getBooleanfield68(){
  return booleanfield68;
  }
  public void setBooleanfield68(boolean value){
  this.booleanfield68 = value;
  }
  public boolean getBooleanfield69(){
  return booleanfield69;
  }
  public void setBooleanfield69(boolean value){
  this.booleanfield69 = value;
  }
  public boolean getBooleanfield70(){
  return booleanfield70;
  }
  public void setBooleanfield70(boolean value){
  this.booleanfield70 = value;
  }
  public boolean getBooleanfield71(){
  return booleanfield71;
  }
  public void setBooleanfield71(boolean value){
  this.booleanfield71 = value;
  }
  public boolean getBooleanfield72(){
  return booleanfield72;
  }
  public void setBooleanfield72(boolean value){
  this.booleanfield72 = value;
  }
  public boolean getBooleanfield73(){
  return booleanfield73;
  }
  public void setBooleanfield73(boolean value){
  this.booleanfield73 = value;
  }
  public boolean getBooleanfield74(){
  return booleanfield74;
  }
  public void setBooleanfield74(boolean value){
  this.booleanfield74 = value;
  }
  public boolean getBooleanfield75(){
  return booleanfield75;
  }
  public void setBooleanfield75(boolean value){
  this.booleanfield75 = value;
  }
  public boolean getBooleanfield76(){
  return booleanfield76;
  }
  public void setBooleanfield76(boolean value){
  this.booleanfield76 = value;
  }
  public boolean getBooleanfield77(){
  return booleanfield77;
  }
  public void setBooleanfield77(boolean value){
  this.booleanfield77 = value;
  }
  public boolean getBooleanfield78(){
  return booleanfield78;
  }
  public void setBooleanfield78(boolean value){
  this.booleanfield78 = value;
  }
  public boolean getBooleanfield79(){
  return booleanfield79;
  }
  public void setBooleanfield79(boolean value){
  this.booleanfield79 = value;
  }
  public boolean getBooleanfield80(){
  return booleanfield80;
  }
  public void setBooleanfield80(boolean value){
  this.booleanfield80 = value;
  }
  public boolean getBooleanfield81(){
  return booleanfield81;
  }
  public void setBooleanfield81(boolean value){
  this.booleanfield81 = value;
  }
  public boolean getBooleanfield82(){
  return booleanfield82;
  }
  public void setBooleanfield82(boolean value){
  this.booleanfield82 = value;
  }
  public boolean getBooleanfield83(){
  return booleanfield83;
  }
  public void setBooleanfield83(boolean value){
  this.booleanfield83 = value;
  }
  public boolean getBooleanfield84(){
  return booleanfield84;
  }
  public void setBooleanfield84(boolean value){
  this.booleanfield84 = value;
  }
  public boolean getBooleanfield85(){
  return booleanfield85;
  }
  public void setBooleanfield85(boolean value){
  this.booleanfield85 = value;
  }
  public boolean getBooleanfield86(){
  return booleanfield86;
  }
  public void setBooleanfield86(boolean value){
  this.booleanfield86 = value;
  }
  public boolean getBooleanfield87(){
  return booleanfield87;
  }
  public void setBooleanfield87(boolean value){
  this.booleanfield87 = value;
  }
  public boolean getBooleanfield88(){
  return booleanfield88;
  }
  public void setBooleanfield88(boolean value){
  this.booleanfield88 = value;
  }
  public boolean getBooleanfield89(){
  return booleanfield89;
  }
  public void setBooleanfield89(boolean value){
  this.booleanfield89 = value;
  }
  public boolean getBooleanfield90(){
  return booleanfield90;
  }
  public void setBooleanfield90(boolean value){
  this.booleanfield90 = value;
  }
  public boolean getBooleanfield91(){
  return booleanfield91;
  }
  public void setBooleanfield91(boolean value){
  this.booleanfield91 = value;
  }
  public boolean getBooleanfield92(){
  return booleanfield92;
  }
  public void setBooleanfield92(boolean value){
  this.booleanfield92 = value;
  }
  public boolean getBooleanfield93(){
  return booleanfield93;
  }
  public void setBooleanfield93(boolean value){
  this.booleanfield93 = value;
  }
  public boolean getBooleanfield94(){
  return booleanfield94;
  }
  public void setBooleanfield94(boolean value){
  this.booleanfield94 = value;
  }
  public boolean getBooleanfield95(){
  return booleanfield95;
  }
  public void setBooleanfield95(boolean value){
  this.booleanfield95 = value;
  }
  public boolean getBooleanfield96(){
  return booleanfield96;
  }
  public void setBooleanfield96(boolean value){
  this.booleanfield96 = value;
  }
  public boolean getBooleanfield97(){
  return booleanfield97;
  }
  public void setBooleanfield97(boolean value){
  this.booleanfield97 = value;
  }
  public boolean getBooleanfield98(){
  return booleanfield98;
  }
  public void setBooleanfield98(boolean value){
  this.booleanfield98 = value;
  }
  public boolean getBooleanfield99(){
  return booleanfield99;
  }
  public void setBooleanfield99(boolean value){
  this.booleanfield99 = value;
  }
  public boolean getBooleanfield100(){
  return booleanfield100;
  }
  public void setBooleanfield100(boolean value){
  this.booleanfield100 = value;
  }
  public boolean getBooleanfield101(){
  return booleanfield101;
  }
  public void setBooleanfield101(boolean value){
  this.booleanfield101 = value;
  }
  public boolean getBooleanfield102(){
  return booleanfield102;
  }
  public void setBooleanfield102(boolean value){
  this.booleanfield102 = value;
  }
  public boolean getBooleanfield103(){
  return booleanfield103;
  }
  public void setBooleanfield103(boolean value){
  this.booleanfield103 = value;
  }
  public boolean getBooleanfield104(){
  return booleanfield104;
  }
  public void setBooleanfield104(boolean value){
  this.booleanfield104 = value;
  }
  public boolean getBooleanfield105(){
  return booleanfield105;
  }
  public void setBooleanfield105(boolean value){
  this.booleanfield105 = value;
  }
  public boolean getBooleanfield106(){
  return booleanfield106;
  }
  public void setBooleanfield106(boolean value){
  this.booleanfield106 = value;
  }
  public boolean getBooleanfield107(){
  return booleanfield107;
  }
  public void setBooleanfield107(boolean value){
  this.booleanfield107 = value;
  }
  public boolean getBooleanfield108(){
  return booleanfield108;
  }
  public void setBooleanfield108(boolean value){
  this.booleanfield108 = value;
  }
  public boolean getBooleanfield109(){
  return booleanfield109;
  }
  public void setBooleanfield109(boolean value){
  this.booleanfield109 = value;
  }
  public boolean getBooleanfield110(){
  return booleanfield110;
  }
  public void setBooleanfield110(boolean value){
  this.booleanfield110 = value;
  }
  public boolean getBooleanfield111(){
  return booleanfield111;
  }
  public void setBooleanfield111(boolean value){
  this.booleanfield111 = value;
  }
  public boolean getBooleanfield112(){
  return booleanfield112;
  }
  public void setBooleanfield112(boolean value){
  this.booleanfield112 = value;
  }
  public boolean getBooleanfield113(){
  return booleanfield113;
  }
  public void setBooleanfield113(boolean value){
  this.booleanfield113 = value;
  }
  public boolean getBooleanfield114(){
  return booleanfield114;
  }
  public void setBooleanfield114(boolean value){
  this.booleanfield114 = value;
  }
  public boolean getBooleanfield115(){
  return booleanfield115;
  }
  public void setBooleanfield115(boolean value){
  this.booleanfield115 = value;
  }
  public boolean getBooleanfield116(){
  return booleanfield116;
  }
  public void setBooleanfield116(boolean value){
  this.booleanfield116 = value;
  }
  public boolean getBooleanfield117(){
  return booleanfield117;
  }
  public void setBooleanfield117(boolean value){
  this.booleanfield117 = value;
  }
  public boolean getBooleanfield118(){
  return booleanfield118;
  }
  public void setBooleanfield118(boolean value){
  this.booleanfield118 = value;
  }
  public boolean getBooleanfield119(){
  return booleanfield119;
  }
  public void setBooleanfield119(boolean value){
  this.booleanfield119 = value;
  }
  public boolean getBooleanfield120(){
  return booleanfield120;
  }
  public void setBooleanfield120(boolean value){
  this.booleanfield120 = value;
  }
  public boolean getBooleanfield121(){
  return booleanfield121;
  }
  public void setBooleanfield121(boolean value){
  this.booleanfield121 = value;
  }
  public boolean getBooleanfield122(){
  return booleanfield122;
  }
  public void setBooleanfield122(boolean value){
  this.booleanfield122 = value;
  }
  public boolean getBooleanfield123(){
  return booleanfield123;
  }
  public void setBooleanfield123(boolean value){
  this.booleanfield123 = value;
  }
  public boolean getBooleanfield124(){
  return booleanfield124;
  }
  public void setBooleanfield124(boolean value){
  this.booleanfield124 = value;
  }
  public boolean getBooleanfield125(){
  return booleanfield125;
  }
  public void setBooleanfield125(boolean value){
  this.booleanfield125 = value;
  }
  public boolean getBooleanfield126(){
  return booleanfield126;
  }
  public void setBooleanfield126(boolean value){
  this.booleanfield126 = value;
  }
  public boolean getBooleanfield127(){
  return booleanfield127;
  }
  public void setBooleanfield127(boolean value){
  this.booleanfield127 = value;
  }
  public boolean getBooleanfield128(){
  return booleanfield128;
  }
  public void setBooleanfield128(boolean value){
  this.booleanfield128 = value;
  }
  public boolean getBooleanfield129(){
  return booleanfield129;
  }
  public void setBooleanfield129(boolean value){
  this.booleanfield129 = value;
  }
  public boolean getBooleanfield130(){
  return booleanfield130;
  }
  public void setBooleanfield130(boolean value){
  this.booleanfield130 = value;
  }
  public boolean getBooleanfield131(){
  return booleanfield131;
  }
  public void setBooleanfield131(boolean value){
  this.booleanfield131 = value;
  }
  public boolean getBooleanfield132(){
  return booleanfield132;
  }
  public void setBooleanfield132(boolean value){
  this.booleanfield132 = value;
  }
  public boolean getBooleanfield133(){
  return booleanfield133;
  }
  public void setBooleanfield133(boolean value){
  this.booleanfield133 = value;
  }
  public boolean getBooleanfield134(){
  return booleanfield134;
  }
  public void setBooleanfield134(boolean value){
  this.booleanfield134 = value;
  }
  public boolean getBooleanfield135(){
  return booleanfield135;
  }
  public void setBooleanfield135(boolean value){
  this.booleanfield135 = value;
  }
  public boolean getBooleanfield136(){
  return booleanfield136;
  }
  public void setBooleanfield136(boolean value){
  this.booleanfield136 = value;
  }
  public boolean getBooleanfield137(){
  return booleanfield137;
  }
  public void setBooleanfield137(boolean value){
  this.booleanfield137 = value;
  }
  public boolean getBooleanfield138(){
  return booleanfield138;
  }
  public void setBooleanfield138(boolean value){
  this.booleanfield138 = value;
  }
  public boolean getBooleanfield139(){
  return booleanfield139;
  }
  public void setBooleanfield139(boolean value){
  this.booleanfield139 = value;
  }
  public boolean getBooleanfield140(){
  return booleanfield140;
  }
  public void setBooleanfield140(boolean value){
  this.booleanfield140 = value;
  }
  public boolean getBooleanfield141(){
  return booleanfield141;
  }
  public void setBooleanfield141(boolean value){
  this.booleanfield141 = value;
  }
  public boolean getBooleanfield142(){
  return booleanfield142;
  }
  public void setBooleanfield142(boolean value){
  this.booleanfield142 = value;
  }
  public boolean getBooleanfield143(){
  return booleanfield143;
  }
  public void setBooleanfield143(boolean value){
  this.booleanfield143 = value;
  }
  public boolean getBooleanfield144(){
  return booleanfield144;
  }
  public void setBooleanfield144(boolean value){
  this.booleanfield144 = value;
  }
  public boolean getBooleanfield145(){
  return booleanfield145;
  }
  public void setBooleanfield145(boolean value){
  this.booleanfield145 = value;
  }
  public boolean getBooleanfield146(){
  return booleanfield146;
  }
  public void setBooleanfield146(boolean value){
  this.booleanfield146 = value;
  }
  public boolean getBooleanfield147(){
  return booleanfield147;
  }
  public void setBooleanfield147(boolean value){
  this.booleanfield147 = value;
  }
  public boolean getBooleanfield148(){
  return booleanfield148;
  }
  public void setBooleanfield148(boolean value){
  this.booleanfield148 = value;
  }
  public boolean getBooleanfield149(){
  return booleanfield149;
  }
  public void setBooleanfield149(boolean value){
  this.booleanfield149 = value;
  }
  public float getFloatfield0(){
  return floatfield0;
  }
  public void setFloatfield0(float value){
  this.floatfield0 = value;
  }
  public float getFloatfield1(){
  return floatfield1;
  }
  public void setFloatfield1(float value){
  this.floatfield1 = value;
  }
  public float getFloatfield2(){
  return floatfield2;
  }
  public void setFloatfield2(float value){
  this.floatfield2 = value;
  }
  public float getFloatfield3(){
  return floatfield3;
  }
  public void setFloatfield3(float value){
  this.floatfield3 = value;
  }
  public float getFloatfield4(){
  return floatfield4;
  }
  public void setFloatfield4(float value){
  this.floatfield4 = value;
  }
  public float getFloatfield5(){
  return floatfield5;
  }
  public void setFloatfield5(float value){
  this.floatfield5 = value;
  }
  public float getFloatfield6(){
  return floatfield6;
  }
  public void setFloatfield6(float value){
  this.floatfield6 = value;
  }
  public float getFloatfield7(){
  return floatfield7;
  }
  public void setFloatfield7(float value){
  this.floatfield7 = value;
  }
  public float getFloatfield8(){
  return floatfield8;
  }
  public void setFloatfield8(float value){
  this.floatfield8 = value;
  }
  public float getFloatfield9(){
  return floatfield9;
  }
  public void setFloatfield9(float value){
  this.floatfield9 = value;
  }
  public float getFloatfield10(){
  return floatfield10;
  }
  public void setFloatfield10(float value){
  this.floatfield10 = value;
  }
  public float getFloatfield11(){
  return floatfield11;
  }
  public void setFloatfield11(float value){
  this.floatfield11 = value;
  }
  public float getFloatfield12(){
  return floatfield12;
  }
  public void setFloatfield12(float value){
  this.floatfield12 = value;
  }
  public float getFloatfield13(){
  return floatfield13;
  }
  public void setFloatfield13(float value){
  this.floatfield13 = value;
  }
  public float getFloatfield14(){
  return floatfield14;
  }
  public void setFloatfield14(float value){
  this.floatfield14 = value;
  }
  public float getFloatfield15(){
  return floatfield15;
  }
  public void setFloatfield15(float value){
  this.floatfield15 = value;
  }
  public float getFloatfield16(){
  return floatfield16;
  }
  public void setFloatfield16(float value){
  this.floatfield16 = value;
  }
  public float getFloatfield17(){
  return floatfield17;
  }
  public void setFloatfield17(float value){
  this.floatfield17 = value;
  }
  public float getFloatfield18(){
  return floatfield18;
  }
  public void setFloatfield18(float value){
  this.floatfield18 = value;
  }
  public float getFloatfield19(){
  return floatfield19;
  }
  public void setFloatfield19(float value){
  this.floatfield19 = value;
  }
  public float getFloatfield20(){
  return floatfield20;
  }
  public void setFloatfield20(float value){
  this.floatfield20 = value;
  }
  public float getFloatfield21(){
  return floatfield21;
  }
  public void setFloatfield21(float value){
  this.floatfield21 = value;
  }
  public float getFloatfield22(){
  return floatfield22;
  }
  public void setFloatfield22(float value){
  this.floatfield22 = value;
  }
  public float getFloatfield23(){
  return floatfield23;
  }
  public void setFloatfield23(float value){
  this.floatfield23 = value;
  }
  public float getFloatfield24(){
  return floatfield24;
  }
  public void setFloatfield24(float value){
  this.floatfield24 = value;
  }
  public float getFloatfield25(){
  return floatfield25;
  }
  public void setFloatfield25(float value){
  this.floatfield25 = value;
  }
  public float getFloatfield26(){
  return floatfield26;
  }
  public void setFloatfield26(float value){
  this.floatfield26 = value;
  }
  public float getFloatfield27(){
  return floatfield27;
  }
  public void setFloatfield27(float value){
  this.floatfield27 = value;
  }
  public float getFloatfield28(){
  return floatfield28;
  }
  public void setFloatfield28(float value){
  this.floatfield28 = value;
  }
  public float getFloatfield29(){
  return floatfield29;
  }
  public void setFloatfield29(float value){
  this.floatfield29 = value;
  }
  public float getFloatfield30(){
  return floatfield30;
  }
  public void setFloatfield30(float value){
  this.floatfield30 = value;
  }
  public float getFloatfield31(){
  return floatfield31;
  }
  public void setFloatfield31(float value){
  this.floatfield31 = value;
  }
  public float getFloatfield32(){
  return floatfield32;
  }
  public void setFloatfield32(float value){
  this.floatfield32 = value;
  }
  public float getFloatfield33(){
  return floatfield33;
  }
  public void setFloatfield33(float value){
  this.floatfield33 = value;
  }
  public float getFloatfield34(){
  return floatfield34;
  }
  public void setFloatfield34(float value){
  this.floatfield34 = value;
  }
  public float getFloatfield35(){
  return floatfield35;
  }
  public void setFloatfield35(float value){
  this.floatfield35 = value;
  }
  public float getFloatfield36(){
  return floatfield36;
  }
  public void setFloatfield36(float value){
  this.floatfield36 = value;
  }
  public float getFloatfield37(){
  return floatfield37;
  }
  public void setFloatfield37(float value){
  this.floatfield37 = value;
  }
  public float getFloatfield38(){
  return floatfield38;
  }
  public void setFloatfield38(float value){
  this.floatfield38 = value;
  }
  public float getFloatfield39(){
  return floatfield39;
  }
  public void setFloatfield39(float value){
  this.floatfield39 = value;
  }
  public float getFloatfield40(){
  return floatfield40;
  }
  public void setFloatfield40(float value){
  this.floatfield40 = value;
  }
  public float getFloatfield41(){
  return floatfield41;
  }
  public void setFloatfield41(float value){
  this.floatfield41 = value;
  }
  public float getFloatfield42(){
  return floatfield42;
  }
  public void setFloatfield42(float value){
  this.floatfield42 = value;
  }
  public float getFloatfield43(){
  return floatfield43;
  }
  public void setFloatfield43(float value){
  this.floatfield43 = value;
  }
  public float getFloatfield44(){
  return floatfield44;
  }
  public void setFloatfield44(float value){
  this.floatfield44 = value;
  }
  public float getFloatfield45(){
  return floatfield45;
  }
  public void setFloatfield45(float value){
  this.floatfield45 = value;
  }
  public float getFloatfield46(){
  return floatfield46;
  }
  public void setFloatfield46(float value){
  this.floatfield46 = value;
  }
  public float getFloatfield47(){
  return floatfield47;
  }
  public void setFloatfield47(float value){
  this.floatfield47 = value;
  }
  public float getFloatfield48(){
  return floatfield48;
  }
  public void setFloatfield48(float value){
  this.floatfield48 = value;
  }
  public float getFloatfield49(){
  return floatfield49;
  }
  public void setFloatfield49(float value){
  this.floatfield49 = value;
  }
  public float getFloatfield50(){
  return floatfield50;
  }
  public void setFloatfield50(float value){
  this.floatfield50 = value;
  }
  public float getFloatfield51(){
  return floatfield51;
  }
  public void setFloatfield51(float value){
  this.floatfield51 = value;
  }
  public float getFloatfield52(){
  return floatfield52;
  }
  public void setFloatfield52(float value){
  this.floatfield52 = value;
  }
  public float getFloatfield53(){
  return floatfield53;
  }
  public void setFloatfield53(float value){
  this.floatfield53 = value;
  }
  public float getFloatfield54(){
  return floatfield54;
  }
  public void setFloatfield54(float value){
  this.floatfield54 = value;
  }
  public float getFloatfield55(){
  return floatfield55;
  }
  public void setFloatfield55(float value){
  this.floatfield55 = value;
  }
  public float getFloatfield56(){
  return floatfield56;
  }
  public void setFloatfield56(float value){
  this.floatfield56 = value;
  }
  public float getFloatfield57(){
  return floatfield57;
  }
  public void setFloatfield57(float value){
  this.floatfield57 = value;
  }
  public float getFloatfield58(){
  return floatfield58;
  }
  public void setFloatfield58(float value){
  this.floatfield58 = value;
  }
  public float getFloatfield59(){
  return floatfield59;
  }
  public void setFloatfield59(float value){
  this.floatfield59 = value;
  }
  public float getFloatfield60(){
  return floatfield60;
  }
  public void setFloatfield60(float value){
  this.floatfield60 = value;
  }
  public float getFloatfield61(){
  return floatfield61;
  }
  public void setFloatfield61(float value){
  this.floatfield61 = value;
  }
  public float getFloatfield62(){
  return floatfield62;
  }
  public void setFloatfield62(float value){
  this.floatfield62 = value;
  }
  public float getFloatfield63(){
  return floatfield63;
  }
  public void setFloatfield63(float value){
  this.floatfield63 = value;
  }
  public float getFloatfield64(){
  return floatfield64;
  }
  public void setFloatfield64(float value){
  this.floatfield64 = value;
  }
  public float getFloatfield65(){
  return floatfield65;
  }
  public void setFloatfield65(float value){
  this.floatfield65 = value;
  }
  public float getFloatfield66(){
  return floatfield66;
  }
  public void setFloatfield66(float value){
  this.floatfield66 = value;
  }
  public float getFloatfield67(){
  return floatfield67;
  }
  public void setFloatfield67(float value){
  this.floatfield67 = value;
  }
  public float getFloatfield68(){
  return floatfield68;
  }
  public void setFloatfield68(float value){
  this.floatfield68 = value;
  }
  public float getFloatfield69(){
  return floatfield69;
  }
  public void setFloatfield69(float value){
  this.floatfield69 = value;
  }
  public float getFloatfield70(){
  return floatfield70;
  }
  public void setFloatfield70(float value){
  this.floatfield70 = value;
  }
  public float getFloatfield71(){
  return floatfield71;
  }
  public void setFloatfield71(float value){
  this.floatfield71 = value;
  }
  public float getFloatfield72(){
  return floatfield72;
  }
  public void setFloatfield72(float value){
  this.floatfield72 = value;
  }
  public float getFloatfield73(){
  return floatfield73;
  }
  public void setFloatfield73(float value){
  this.floatfield73 = value;
  }
  public float getFloatfield74(){
  return floatfield74;
  }
  public void setFloatfield74(float value){
  this.floatfield74 = value;
  }
  public float getFloatfield75(){
  return floatfield75;
  }
  public void setFloatfield75(float value){
  this.floatfield75 = value;
  }
  public float getFloatfield76(){
  return floatfield76;
  }
  public void setFloatfield76(float value){
  this.floatfield76 = value;
  }
  public float getFloatfield77(){
  return floatfield77;
  }
  public void setFloatfield77(float value){
  this.floatfield77 = value;
  }
  public float getFloatfield78(){
  return floatfield78;
  }
  public void setFloatfield78(float value){
  this.floatfield78 = value;
  }
  public float getFloatfield79(){
  return floatfield79;
  }
  public void setFloatfield79(float value){
  this.floatfield79 = value;
  }
  public float getFloatfield80(){
  return floatfield80;
  }
  public void setFloatfield80(float value){
  this.floatfield80 = value;
  }
  public float getFloatfield81(){
  return floatfield81;
  }
  public void setFloatfield81(float value){
  this.floatfield81 = value;
  }
  public float getFloatfield82(){
  return floatfield82;
  }
  public void setFloatfield82(float value){
  this.floatfield82 = value;
  }
  public float getFloatfield83(){
  return floatfield83;
  }
  public void setFloatfield83(float value){
  this.floatfield83 = value;
  }
  public float getFloatfield84(){
  return floatfield84;
  }
  public void setFloatfield84(float value){
  this.floatfield84 = value;
  }
  public float getFloatfield85(){
  return floatfield85;
  }
  public void setFloatfield85(float value){
  this.floatfield85 = value;
  }
  public float getFloatfield86(){
  return floatfield86;
  }
  public void setFloatfield86(float value){
  this.floatfield86 = value;
  }
  public float getFloatfield87(){
  return floatfield87;
  }
  public void setFloatfield87(float value){
  this.floatfield87 = value;
  }
  public float getFloatfield88(){
  return floatfield88;
  }
  public void setFloatfield88(float value){
  this.floatfield88 = value;
  }
  public float getFloatfield89(){
  return floatfield89;
  }
  public void setFloatfield89(float value){
  this.floatfield89 = value;
  }
  public float getFloatfield90(){
  return floatfield90;
  }
  public void setFloatfield90(float value){
  this.floatfield90 = value;
  }
  public float getFloatfield91(){
  return floatfield91;
  }
  public void setFloatfield91(float value){
  this.floatfield91 = value;
  }
  public float getFloatfield92(){
  return floatfield92;
  }
  public void setFloatfield92(float value){
  this.floatfield92 = value;
  }
  public float getFloatfield93(){
  return floatfield93;
  }
  public void setFloatfield93(float value){
  this.floatfield93 = value;
  }
  public float getFloatfield94(){
  return floatfield94;
  }
  public void setFloatfield94(float value){
  this.floatfield94 = value;
  }
  public float getFloatfield95(){
  return floatfield95;
  }
  public void setFloatfield95(float value){
  this.floatfield95 = value;
  }
  public float getFloatfield96(){
  return floatfield96;
  }
  public void setFloatfield96(float value){
  this.floatfield96 = value;
  }
  public float getFloatfield97(){
  return floatfield97;
  }
  public void setFloatfield97(float value){
  this.floatfield97 = value;
  }
  public float getFloatfield98(){
  return floatfield98;
  }
  public void setFloatfield98(float value){
  this.floatfield98 = value;
  }
  public float getFloatfield99(){
  return floatfield99;
  }
  public void setFloatfield99(float value){
  this.floatfield99 = value;
  }
  public float getFloatfield100(){
  return floatfield100;
  }
  public void setFloatfield100(float value){
  this.floatfield100 = value;
  }
  public float getFloatfield101(){
  return floatfield101;
  }
  public void setFloatfield101(float value){
  this.floatfield101 = value;
  }
  public float getFloatfield102(){
  return floatfield102;
  }
  public void setFloatfield102(float value){
  this.floatfield102 = value;
  }
  public float getFloatfield103(){
  return floatfield103;
  }
  public void setFloatfield103(float value){
  this.floatfield103 = value;
  }
  public float getFloatfield104(){
  return floatfield104;
  }
  public void setFloatfield104(float value){
  this.floatfield104 = value;
  }
  public float getFloatfield105(){
  return floatfield105;
  }
  public void setFloatfield105(float value){
  this.floatfield105 = value;
  }
  public float getFloatfield106(){
  return floatfield106;
  }
  public void setFloatfield106(float value){
  this.floatfield106 = value;
  }
  public float getFloatfield107(){
  return floatfield107;
  }
  public void setFloatfield107(float value){
  this.floatfield107 = value;
  }
  public float getFloatfield108(){
  return floatfield108;
  }
  public void setFloatfield108(float value){
  this.floatfield108 = value;
  }
  public float getFloatfield109(){
  return floatfield109;
  }
  public void setFloatfield109(float value){
  this.floatfield109 = value;
  }
  public float getFloatfield110(){
  return floatfield110;
  }
  public void setFloatfield110(float value){
  this.floatfield110 = value;
  }
  public float getFloatfield111(){
  return floatfield111;
  }
  public void setFloatfield111(float value){
  this.floatfield111 = value;
  }
  public float getFloatfield112(){
  return floatfield112;
  }
  public void setFloatfield112(float value){
  this.floatfield112 = value;
  }
  public float getFloatfield113(){
  return floatfield113;
  }
  public void setFloatfield113(float value){
  this.floatfield113 = value;
  }
  public float getFloatfield114(){
  return floatfield114;
  }
  public void setFloatfield114(float value){
  this.floatfield114 = value;
  }
  public float getFloatfield115(){
  return floatfield115;
  }
  public void setFloatfield115(float value){
  this.floatfield115 = value;
  }
  public float getFloatfield116(){
  return floatfield116;
  }
  public void setFloatfield116(float value){
  this.floatfield116 = value;
  }
  public float getFloatfield117(){
  return floatfield117;
  }
  public void setFloatfield117(float value){
  this.floatfield117 = value;
  }
  public float getFloatfield118(){
  return floatfield118;
  }
  public void setFloatfield118(float value){
  this.floatfield118 = value;
  }
  public float getFloatfield119(){
  return floatfield119;
  }
  public void setFloatfield119(float value){
  this.floatfield119 = value;
  }
  public float getFloatfield120(){
  return floatfield120;
  }
  public void setFloatfield120(float value){
  this.floatfield120 = value;
  }
  public float getFloatfield121(){
  return floatfield121;
  }
  public void setFloatfield121(float value){
  this.floatfield121 = value;
  }
  public float getFloatfield122(){
  return floatfield122;
  }
  public void setFloatfield122(float value){
  this.floatfield122 = value;
  }
  public float getFloatfield123(){
  return floatfield123;
  }
  public void setFloatfield123(float value){
  this.floatfield123 = value;
  }
  public float getFloatfield124(){
  return floatfield124;
  }
  public void setFloatfield124(float value){
  this.floatfield124 = value;
  }
  public float getFloatfield125(){
  return floatfield125;
  }
  public void setFloatfield125(float value){
  this.floatfield125 = value;
  }
  public float getFloatfield126(){
  return floatfield126;
  }
  public void setFloatfield126(float value){
  this.floatfield126 = value;
  }
  public float getFloatfield127(){
  return floatfield127;
  }
  public void setFloatfield127(float value){
  this.floatfield127 = value;
  }
  public float getFloatfield128(){
  return floatfield128;
  }
  public void setFloatfield128(float value){
  this.floatfield128 = value;
  }
  public float getFloatfield129(){
  return floatfield129;
  }
  public void setFloatfield129(float value){
  this.floatfield129 = value;
  }
  public float getFloatfield130(){
  return floatfield130;
  }
  public void setFloatfield130(float value){
  this.floatfield130 = value;
  }
  public float getFloatfield131(){
  return floatfield131;
  }
  public void setFloatfield131(float value){
  this.floatfield131 = value;
  }
  public float getFloatfield132(){
  return floatfield132;
  }
  public void setFloatfield132(float value){
  this.floatfield132 = value;
  }
  public float getFloatfield133(){
  return floatfield133;
  }
  public void setFloatfield133(float value){
  this.floatfield133 = value;
  }
  public float getFloatfield134(){
  return floatfield134;
  }
  public void setFloatfield134(float value){
  this.floatfield134 = value;
  }
  public float getFloatfield135(){
  return floatfield135;
  }
  public void setFloatfield135(float value){
  this.floatfield135 = value;
  }
  public float getFloatfield136(){
  return floatfield136;
  }
  public void setFloatfield136(float value){
  this.floatfield136 = value;
  }
  public float getFloatfield137(){
  return floatfield137;
  }
  public void setFloatfield137(float value){
  this.floatfield137 = value;
  }
  public float getFloatfield138(){
  return floatfield138;
  }
  public void setFloatfield138(float value){
  this.floatfield138 = value;
  }
  public float getFloatfield139(){
  return floatfield139;
  }
  public void setFloatfield139(float value){
  this.floatfield139 = value;
  }
  public float getFloatfield140(){
  return floatfield140;
  }
  public void setFloatfield140(float value){
  this.floatfield140 = value;
  }
  public float getFloatfield141(){
  return floatfield141;
  }
  public void setFloatfield141(float value){
  this.floatfield141 = value;
  }
  public float getFloatfield142(){
  return floatfield142;
  }
  public void setFloatfield142(float value){
  this.floatfield142 = value;
  }
  public float getFloatfield143(){
  return floatfield143;
  }
  public void setFloatfield143(float value){
  this.floatfield143 = value;
  }
  public float getFloatfield144(){
  return floatfield144;
  }
  public void setFloatfield144(float value){
  this.floatfield144 = value;
  }
  public float getFloatfield145(){
  return floatfield145;
  }
  public void setFloatfield145(float value){
  this.floatfield145 = value;
  }
  public float getFloatfield146(){
  return floatfield146;
  }
  public void setFloatfield146(float value){
  this.floatfield146 = value;
  }
  public float getFloatfield147(){
  return floatfield147;
  }
  public void setFloatfield147(float value){
  this.floatfield147 = value;
  }
  public float getFloatfield148(){
  return floatfield148;
  }
  public void setFloatfield148(float value){
  this.floatfield148 = value;
  }
  public float getFloatfield149(){
  return floatfield149;
  }
  public void setFloatfield149(float value){
  this.floatfield149 = value;
  }
  public long getLongfield0(){
  return longfield0;
  }
  public void setLongfield0(long value){
  this.longfield0 = value;
  }
  public long getLongfield1(){
  return longfield1;
  }
  public void setLongfield1(long value){
  this.longfield1 = value;
  }
  public long getLongfield2(){
  return longfield2;
  }
  public void setLongfield2(long value){
  this.longfield2 = value;
  }
  public long getLongfield3(){
  return longfield3;
  }
  public void setLongfield3(long value){
  this.longfield3 = value;
  }
  public long getLongfield4(){
  return longfield4;
  }
  public void setLongfield4(long value){
  this.longfield4 = value;
  }
  public long getLongfield5(){
  return longfield5;
  }
  public void setLongfield5(long value){
  this.longfield5 = value;
  }
  public long getLongfield6(){
  return longfield6;
  }
  public void setLongfield6(long value){
  this.longfield6 = value;
  }
  public long getLongfield7(){
  return longfield7;
  }
  public void setLongfield7(long value){
  this.longfield7 = value;
  }
  public long getLongfield8(){
  return longfield8;
  }
  public void setLongfield8(long value){
  this.longfield8 = value;
  }
  public long getLongfield9(){
  return longfield9;
  }
  public void setLongfield9(long value){
  this.longfield9 = value;
  }
  public long getLongfield10(){
  return longfield10;
  }
  public void setLongfield10(long value){
  this.longfield10 = value;
  }
  public long getLongfield11(){
  return longfield11;
  }
  public void setLongfield11(long value){
  this.longfield11 = value;
  }
  public long getLongfield12(){
  return longfield12;
  }
  public void setLongfield12(long value){
  this.longfield12 = value;
  }
  public long getLongfield13(){
  return longfield13;
  }
  public void setLongfield13(long value){
  this.longfield13 = value;
  }
  public long getLongfield14(){
  return longfield14;
  }
  public void setLongfield14(long value){
  this.longfield14 = value;
  }
  public long getLongfield15(){
  return longfield15;
  }
  public void setLongfield15(long value){
  this.longfield15 = value;
  }
  public long getLongfield16(){
  return longfield16;
  }
  public void setLongfield16(long value){
  this.longfield16 = value;
  }
  public long getLongfield17(){
  return longfield17;
  }
  public void setLongfield17(long value){
  this.longfield17 = value;
  }
  public long getLongfield18(){
  return longfield18;
  }
  public void setLongfield18(long value){
  this.longfield18 = value;
  }
  public long getLongfield19(){
  return longfield19;
  }
  public void setLongfield19(long value){
  this.longfield19 = value;
  }
  public long getLongfield20(){
  return longfield20;
  }
  public void setLongfield20(long value){
  this.longfield20 = value;
  }
  public long getLongfield21(){
  return longfield21;
  }
  public void setLongfield21(long value){
  this.longfield21 = value;
  }
  public long getLongfield22(){
  return longfield22;
  }
  public void setLongfield22(long value){
  this.longfield22 = value;
  }
  public long getLongfield23(){
  return longfield23;
  }
  public void setLongfield23(long value){
  this.longfield23 = value;
  }
  public long getLongfield24(){
  return longfield24;
  }
  public void setLongfield24(long value){
  this.longfield24 = value;
  }
  public long getLongfield25(){
  return longfield25;
  }
  public void setLongfield25(long value){
  this.longfield25 = value;
  }
  public long getLongfield26(){
  return longfield26;
  }
  public void setLongfield26(long value){
  this.longfield26 = value;
  }
  public long getLongfield27(){
  return longfield27;
  }
  public void setLongfield27(long value){
  this.longfield27 = value;
  }
  public long getLongfield28(){
  return longfield28;
  }
  public void setLongfield28(long value){
  this.longfield28 = value;
  }
  public long getLongfield29(){
  return longfield29;
  }
  public void setLongfield29(long value){
  this.longfield29 = value;
  }
  public long getLongfield30(){
  return longfield30;
  }
  public void setLongfield30(long value){
  this.longfield30 = value;
  }
  public long getLongfield31(){
  return longfield31;
  }
  public void setLongfield31(long value){
  this.longfield31 = value;
  }
  public long getLongfield32(){
  return longfield32;
  }
  public void setLongfield32(long value){
  this.longfield32 = value;
  }
  public long getLongfield33(){
  return longfield33;
  }
  public void setLongfield33(long value){
  this.longfield33 = value;
  }
  public long getLongfield34(){
  return longfield34;
  }
  public void setLongfield34(long value){
  this.longfield34 = value;
  }
  public long getLongfield35(){
  return longfield35;
  }
  public void setLongfield35(long value){
  this.longfield35 = value;
  }
  public long getLongfield36(){
  return longfield36;
  }
  public void setLongfield36(long value){
  this.longfield36 = value;
  }
  public long getLongfield37(){
  return longfield37;
  }
  public void setLongfield37(long value){
  this.longfield37 = value;
  }
  public long getLongfield38(){
  return longfield38;
  }
  public void setLongfield38(long value){
  this.longfield38 = value;
  }
  public long getLongfield39(){
  return longfield39;
  }
  public void setLongfield39(long value){
  this.longfield39 = value;
  }
  public long getLongfield40(){
  return longfield40;
  }
  public void setLongfield40(long value){
  this.longfield40 = value;
  }
  public long getLongfield41(){
  return longfield41;
  }
  public void setLongfield41(long value){
  this.longfield41 = value;
  }
  public long getLongfield42(){
  return longfield42;
  }
  public void setLongfield42(long value){
  this.longfield42 = value;
  }
  public long getLongfield43(){
  return longfield43;
  }
  public void setLongfield43(long value){
  this.longfield43 = value;
  }
  public long getLongfield44(){
  return longfield44;
  }
  public void setLongfield44(long value){
  this.longfield44 = value;
  }
  public long getLongfield45(){
  return longfield45;
  }
  public void setLongfield45(long value){
  this.longfield45 = value;
  }
  public long getLongfield46(){
  return longfield46;
  }
  public void setLongfield46(long value){
  this.longfield46 = value;
  }
  public long getLongfield47(){
  return longfield47;
  }
  public void setLongfield47(long value){
  this.longfield47 = value;
  }
  public long getLongfield48(){
  return longfield48;
  }
  public void setLongfield48(long value){
  this.longfield48 = value;
  }
  public long getLongfield49(){
  return longfield49;
  }
  public void setLongfield49(long value){
  this.longfield49 = value;
  }
  public long getLongfield50(){
  return longfield50;
  }
  public void setLongfield50(long value){
  this.longfield50 = value;
  }
  public long getLongfield51(){
  return longfield51;
  }
  public void setLongfield51(long value){
  this.longfield51 = value;
  }
  public long getLongfield52(){
  return longfield52;
  }
  public void setLongfield52(long value){
  this.longfield52 = value;
  }
  public long getLongfield53(){
  return longfield53;
  }
  public void setLongfield53(long value){
  this.longfield53 = value;
  }
  public long getLongfield54(){
  return longfield54;
  }
  public void setLongfield54(long value){
  this.longfield54 = value;
  }
  public long getLongfield55(){
  return longfield55;
  }
  public void setLongfield55(long value){
  this.longfield55 = value;
  }
  public long getLongfield56(){
  return longfield56;
  }
  public void setLongfield56(long value){
  this.longfield56 = value;
  }
  public long getLongfield57(){
  return longfield57;
  }
  public void setLongfield57(long value){
  this.longfield57 = value;
  }
  public long getLongfield58(){
  return longfield58;
  }
  public void setLongfield58(long value){
  this.longfield58 = value;
  }
  public long getLongfield59(){
  return longfield59;
  }
  public void setLongfield59(long value){
  this.longfield59 = value;
  }
  public long getLongfield60(){
  return longfield60;
  }
  public void setLongfield60(long value){
  this.longfield60 = value;
  }
  public long getLongfield61(){
  return longfield61;
  }
  public void setLongfield61(long value){
  this.longfield61 = value;
  }
  public long getLongfield62(){
  return longfield62;
  }
  public void setLongfield62(long value){
  this.longfield62 = value;
  }
  public long getLongfield63(){
  return longfield63;
  }
  public void setLongfield63(long value){
  this.longfield63 = value;
  }
  public long getLongfield64(){
  return longfield64;
  }
  public void setLongfield64(long value){
  this.longfield64 = value;
  }
  public long getLongfield65(){
  return longfield65;
  }
  public void setLongfield65(long value){
  this.longfield65 = value;
  }
  public long getLongfield66(){
  return longfield66;
  }
  public void setLongfield66(long value){
  this.longfield66 = value;
  }
  public long getLongfield67(){
  return longfield67;
  }
  public void setLongfield67(long value){
  this.longfield67 = value;
  }
  public long getLongfield68(){
  return longfield68;
  }
  public void setLongfield68(long value){
  this.longfield68 = value;
  }
  public long getLongfield69(){
  return longfield69;
  }
  public void setLongfield69(long value){
  this.longfield69 = value;
  }
  public long getLongfield70(){
  return longfield70;
  }
  public void setLongfield70(long value){
  this.longfield70 = value;
  }
  public long getLongfield71(){
  return longfield71;
  }
  public void setLongfield71(long value){
  this.longfield71 = value;
  }
  public long getLongfield72(){
  return longfield72;
  }
  public void setLongfield72(long value){
  this.longfield72 = value;
  }
  public long getLongfield73(){
  return longfield73;
  }
  public void setLongfield73(long value){
  this.longfield73 = value;
  }
  public long getLongfield74(){
  return longfield74;
  }
  public void setLongfield74(long value){
  this.longfield74 = value;
  }
  public long getLongfield75(){
  return longfield75;
  }
  public void setLongfield75(long value){
  this.longfield75 = value;
  }
  public long getLongfield76(){
  return longfield76;
  }
  public void setLongfield76(long value){
  this.longfield76 = value;
  }
  public long getLongfield77(){
  return longfield77;
  }
  public void setLongfield77(long value){
  this.longfield77 = value;
  }
  public long getLongfield78(){
  return longfield78;
  }
  public void setLongfield78(long value){
  this.longfield78 = value;
  }
  public long getLongfield79(){
  return longfield79;
  }
  public void setLongfield79(long value){
  this.longfield79 = value;
  }
  public long getLongfield80(){
  return longfield80;
  }
  public void setLongfield80(long value){
  this.longfield80 = value;
  }
  public long getLongfield81(){
  return longfield81;
  }
  public void setLongfield81(long value){
  this.longfield81 = value;
  }
  public long getLongfield82(){
  return longfield82;
  }
  public void setLongfield82(long value){
  this.longfield82 = value;
  }
  public long getLongfield83(){
  return longfield83;
  }
  public void setLongfield83(long value){
  this.longfield83 = value;
  }
  public long getLongfield84(){
  return longfield84;
  }
  public void setLongfield84(long value){
  this.longfield84 = value;
  }
  public long getLongfield85(){
  return longfield85;
  }
  public void setLongfield85(long value){
  this.longfield85 = value;
  }
  public long getLongfield86(){
  return longfield86;
  }
  public void setLongfield86(long value){
  this.longfield86 = value;
  }
  public long getLongfield87(){
  return longfield87;
  }
  public void setLongfield87(long value){
  this.longfield87 = value;
  }
  public long getLongfield88(){
  return longfield88;
  }
  public void setLongfield88(long value){
  this.longfield88 = value;
  }
  public long getLongfield89(){
  return longfield89;
  }
  public void setLongfield89(long value){
  this.longfield89 = value;
  }
  public long getLongfield90(){
  return longfield90;
  }
  public void setLongfield90(long value){
  this.longfield90 = value;
  }
  public long getLongfield91(){
  return longfield91;
  }
  public void setLongfield91(long value){
  this.longfield91 = value;
  }
  public long getLongfield92(){
  return longfield92;
  }
  public void setLongfield92(long value){
  this.longfield92 = value;
  }
  public long getLongfield93(){
  return longfield93;
  }
  public void setLongfield93(long value){
  this.longfield93 = value;
  }
  public long getLongfield94(){
  return longfield94;
  }
  public void setLongfield94(long value){
  this.longfield94 = value;
  }
  public long getLongfield95(){
  return longfield95;
  }
  public void setLongfield95(long value){
  this.longfield95 = value;
  }
  public long getLongfield96(){
  return longfield96;
  }
  public void setLongfield96(long value){
  this.longfield96 = value;
  }
  public long getLongfield97(){
  return longfield97;
  }
  public void setLongfield97(long value){
  this.longfield97 = value;
  }
  public long getLongfield98(){
  return longfield98;
  }
  public void setLongfield98(long value){
  this.longfield98 = value;
  }
  public long getLongfield99(){
  return longfield99;
  }
  public void setLongfield99(long value){
  this.longfield99 = value;
  }
  public long getLongfield100(){
  return longfield100;
  }
  public void setLongfield100(long value){
  this.longfield100 = value;
  }
  public long getLongfield101(){
  return longfield101;
  }
  public void setLongfield101(long value){
  this.longfield101 = value;
  }
  public long getLongfield102(){
  return longfield102;
  }
  public void setLongfield102(long value){
  this.longfield102 = value;
  }
  public long getLongfield103(){
  return longfield103;
  }
  public void setLongfield103(long value){
  this.longfield103 = value;
  }
  public long getLongfield104(){
  return longfield104;
  }
  public void setLongfield104(long value){
  this.longfield104 = value;
  }
  public long getLongfield105(){
  return longfield105;
  }
  public void setLongfield105(long value){
  this.longfield105 = value;
  }
  public long getLongfield106(){
  return longfield106;
  }
  public void setLongfield106(long value){
  this.longfield106 = value;
  }
  public long getLongfield107(){
  return longfield107;
  }
  public void setLongfield107(long value){
  this.longfield107 = value;
  }
  public long getLongfield108(){
  return longfield108;
  }
  public void setLongfield108(long value){
  this.longfield108 = value;
  }
  public long getLongfield109(){
  return longfield109;
  }
  public void setLongfield109(long value){
  this.longfield109 = value;
  }
  public long getLongfield110(){
  return longfield110;
  }
  public void setLongfield110(long value){
  this.longfield110 = value;
  }
  public long getLongfield111(){
  return longfield111;
  }
  public void setLongfield111(long value){
  this.longfield111 = value;
  }
  public long getLongfield112(){
  return longfield112;
  }
  public void setLongfield112(long value){
  this.longfield112 = value;
  }
  public long getLongfield113(){
  return longfield113;
  }
  public void setLongfield113(long value){
  this.longfield113 = value;
  }
  public long getLongfield114(){
  return longfield114;
  }
  public void setLongfield114(long value){
  this.longfield114 = value;
  }
  public long getLongfield115(){
  return longfield115;
  }
  public void setLongfield115(long value){
  this.longfield115 = value;
  }
  public long getLongfield116(){
  return longfield116;
  }
  public void setLongfield116(long value){
  this.longfield116 = value;
  }
  public long getLongfield117(){
  return longfield117;
  }
  public void setLongfield117(long value){
  this.longfield117 = value;
  }
  public long getLongfield118(){
  return longfield118;
  }
  public void setLongfield118(long value){
  this.longfield118 = value;
  }
  public long getLongfield119(){
  return longfield119;
  }
  public void setLongfield119(long value){
  this.longfield119 = value;
  }
  public long getLongfield120(){
  return longfield120;
  }
  public void setLongfield120(long value){
  this.longfield120 = value;
  }
  public long getLongfield121(){
  return longfield121;
  }
  public void setLongfield121(long value){
  this.longfield121 = value;
  }
  public long getLongfield122(){
  return longfield122;
  }
  public void setLongfield122(long value){
  this.longfield122 = value;
  }
  public long getLongfield123(){
  return longfield123;
  }
  public void setLongfield123(long value){
  this.longfield123 = value;
  }
  public long getLongfield124(){
  return longfield124;
  }
  public void setLongfield124(long value){
  this.longfield124 = value;
  }
  public long getLongfield125(){
  return longfield125;
  }
  public void setLongfield125(long value){
  this.longfield125 = value;
  }
  public long getLongfield126(){
  return longfield126;
  }
  public void setLongfield126(long value){
  this.longfield126 = value;
  }
  public long getLongfield127(){
  return longfield127;
  }
  public void setLongfield127(long value){
  this.longfield127 = value;
  }
  public long getLongfield128(){
  return longfield128;
  }
  public void setLongfield128(long value){
  this.longfield128 = value;
  }
  public long getLongfield129(){
  return longfield129;
  }
  public void setLongfield129(long value){
  this.longfield129 = value;
  }
  public long getLongfield130(){
  return longfield130;
  }
  public void setLongfield130(long value){
  this.longfield130 = value;
  }
  public long getLongfield131(){
  return longfield131;
  }
  public void setLongfield131(long value){
  this.longfield131 = value;
  }
  public long getLongfield132(){
  return longfield132;
  }
  public void setLongfield132(long value){
  this.longfield132 = value;
  }
  public long getLongfield133(){
  return longfield133;
  }
  public void setLongfield133(long value){
  this.longfield133 = value;
  }
  public long getLongfield134(){
  return longfield134;
  }
  public void setLongfield134(long value){
  this.longfield134 = value;
  }
  public long getLongfield135(){
  return longfield135;
  }
  public void setLongfield135(long value){
  this.longfield135 = value;
  }
  public long getLongfield136(){
  return longfield136;
  }
  public void setLongfield136(long value){
  this.longfield136 = value;
  }
  public long getLongfield137(){
  return longfield137;
  }
  public void setLongfield137(long value){
  this.longfield137 = value;
  }
  public long getLongfield138(){
  return longfield138;
  }
  public void setLongfield138(long value){
  this.longfield138 = value;
  }
  public long getLongfield139(){
  return longfield139;
  }
  public void setLongfield139(long value){
  this.longfield139 = value;
  }
  public long getLongfield140(){
  return longfield140;
  }
  public void setLongfield140(long value){
  this.longfield140 = value;
  }
  public long getLongfield141(){
  return longfield141;
  }
  public void setLongfield141(long value){
  this.longfield141 = value;
  }
  public long getLongfield142(){
  return longfield142;
  }
  public void setLongfield142(long value){
  this.longfield142 = value;
  }
  public long getLongfield143(){
  return longfield143;
  }
  public void setLongfield143(long value){
  this.longfield143 = value;
  }
  public long getLongfield144(){
  return longfield144;
  }
  public void setLongfield144(long value){
  this.longfield144 = value;
  }
  public long getLongfield145(){
  return longfield145;
  }
  public void setLongfield145(long value){
  this.longfield145 = value;
  }
  public long getLongfield146(){
  return longfield146;
  }
  public void setLongfield146(long value){
  this.longfield146 = value;
  }
  public long getLongfield147(){
  return longfield147;
  }
  public void setLongfield147(long value){
  this.longfield147 = value;
  }
  public long getLongfield148(){
  return longfield148;
  }
  public void setLongfield148(long value){
  this.longfield148 = value;
  }
  public long getLongfield149(){
  return longfield149;
  }
  public void setLongfield149(long value){
  this.longfield149 = value;
  }
  public short getShortfield0(){
  return shortfield0;
  }
  public void setShortfield0(short value){
  this.shortfield0 = value;
  }
  public short getShortfield1(){
  return shortfield1;
  }
  public void setShortfield1(short value){
  this.shortfield1 = value;
  }
  public short getShortfield2(){
  return shortfield2;
  }
  public void setShortfield2(short value){
  this.shortfield2 = value;
  }
  public short getShortfield3(){
  return shortfield3;
  }
  public void setShortfield3(short value){
  this.shortfield3 = value;
  }
  public short getShortfield4(){
  return shortfield4;
  }
  public void setShortfield4(short value){
  this.shortfield4 = value;
  }
  public short getShortfield5(){
  return shortfield5;
  }
  public void setShortfield5(short value){
  this.shortfield5 = value;
  }
  public short getShortfield6(){
  return shortfield6;
  }
  public void setShortfield6(short value){
  this.shortfield6 = value;
  }
  public short getShortfield7(){
  return shortfield7;
  }
  public void setShortfield7(short value){
  this.shortfield7 = value;
  }
  public short getShortfield8(){
  return shortfield8;
  }
  public void setShortfield8(short value){
  this.shortfield8 = value;
  }
  public short getShortfield9(){
  return shortfield9;
  }
  public void setShortfield9(short value){
  this.shortfield9 = value;
  }
  public short getShortfield10(){
  return shortfield10;
  }
  public void setShortfield10(short value){
  this.shortfield10 = value;
  }
  public short getShortfield11(){
  return shortfield11;
  }
  public void setShortfield11(short value){
  this.shortfield11 = value;
  }
  public short getShortfield12(){
  return shortfield12;
  }
  public void setShortfield12(short value){
  this.shortfield12 = value;
  }
  public short getShortfield13(){
  return shortfield13;
  }
  public void setShortfield13(short value){
  this.shortfield13 = value;
  }
  public short getShortfield14(){
  return shortfield14;
  }
  public void setShortfield14(short value){
  this.shortfield14 = value;
  }
  public short getShortfield15(){
  return shortfield15;
  }
  public void setShortfield15(short value){
  this.shortfield15 = value;
  }
  public short getShortfield16(){
  return shortfield16;
  }
  public void setShortfield16(short value){
  this.shortfield16 = value;
  }
  public short getShortfield17(){
  return shortfield17;
  }
  public void setShortfield17(short value){
  this.shortfield17 = value;
  }
  public short getShortfield18(){
  return shortfield18;
  }
  public void setShortfield18(short value){
  this.shortfield18 = value;
  }
  public short getShortfield19(){
  return shortfield19;
  }
  public void setShortfield19(short value){
  this.shortfield19 = value;
  }
  public short getShortfield20(){
  return shortfield20;
  }
  public void setShortfield20(short value){
  this.shortfield20 = value;
  }
  public short getShortfield21(){
  return shortfield21;
  }
  public void setShortfield21(short value){
  this.shortfield21 = value;
  }
  public short getShortfield22(){
  return shortfield22;
  }
  public void setShortfield22(short value){
  this.shortfield22 = value;
  }
  public short getShortfield23(){
  return shortfield23;
  }
  public void setShortfield23(short value){
  this.shortfield23 = value;
  }
  public short getShortfield24(){
  return shortfield24;
  }
  public void setShortfield24(short value){
  this.shortfield24 = value;
  }
  public short getShortfield25(){
  return shortfield25;
  }
  public void setShortfield25(short value){
  this.shortfield25 = value;
  }
  public short getShortfield26(){
  return shortfield26;
  }
  public void setShortfield26(short value){
  this.shortfield26 = value;
  }
  public short getShortfield27(){
  return shortfield27;
  }
  public void setShortfield27(short value){
  this.shortfield27 = value;
  }
  public short getShortfield28(){
  return shortfield28;
  }
  public void setShortfield28(short value){
  this.shortfield28 = value;
  }
  public short getShortfield29(){
  return shortfield29;
  }
  public void setShortfield29(short value){
  this.shortfield29 = value;
  }
  public short getShortfield30(){
  return shortfield30;
  }
  public void setShortfield30(short value){
  this.shortfield30 = value;
  }
  public short getShortfield31(){
  return shortfield31;
  }
  public void setShortfield31(short value){
  this.shortfield31 = value;
  }
  public short getShortfield32(){
  return shortfield32;
  }
  public void setShortfield32(short value){
  this.shortfield32 = value;
  }
  public short getShortfield33(){
  return shortfield33;
  }
  public void setShortfield33(short value){
  this.shortfield33 = value;
  }
  public short getShortfield34(){
  return shortfield34;
  }
  public void setShortfield34(short value){
  this.shortfield34 = value;
  }
  public short getShortfield35(){
  return shortfield35;
  }
  public void setShortfield35(short value){
  this.shortfield35 = value;
  }
  public short getShortfield36(){
  return shortfield36;
  }
  public void setShortfield36(short value){
  this.shortfield36 = value;
  }
  public short getShortfield37(){
  return shortfield37;
  }
  public void setShortfield37(short value){
  this.shortfield37 = value;
  }
  public short getShortfield38(){
  return shortfield38;
  }
  public void setShortfield38(short value){
  this.shortfield38 = value;
  }
  public short getShortfield39(){
  return shortfield39;
  }
  public void setShortfield39(short value){
  this.shortfield39 = value;
  }
  public short getShortfield40(){
  return shortfield40;
  }
  public void setShortfield40(short value){
  this.shortfield40 = value;
  }
  public short getShortfield41(){
  return shortfield41;
  }
  public void setShortfield41(short value){
  this.shortfield41 = value;
  }
  public short getShortfield42(){
  return shortfield42;
  }
  public void setShortfield42(short value){
  this.shortfield42 = value;
  }
  public short getShortfield43(){
  return shortfield43;
  }
  public void setShortfield43(short value){
  this.shortfield43 = value;
  }
  public short getShortfield44(){
  return shortfield44;
  }
  public void setShortfield44(short value){
  this.shortfield44 = value;
  }
  public short getShortfield45(){
  return shortfield45;
  }
  public void setShortfield45(short value){
  this.shortfield45 = value;
  }
  public short getShortfield46(){
  return shortfield46;
  }
  public void setShortfield46(short value){
  this.shortfield46 = value;
  }
  public short getShortfield47(){
  return shortfield47;
  }
  public void setShortfield47(short value){
  this.shortfield47 = value;
  }
  public short getShortfield48(){
  return shortfield48;
  }
  public void setShortfield48(short value){
  this.shortfield48 = value;
  }
  public short getShortfield49(){
  return shortfield49;
  }
  public void setShortfield49(short value){
  this.shortfield49 = value;
  }
  public short getShortfield50(){
  return shortfield50;
  }
  public void setShortfield50(short value){
  this.shortfield50 = value;
  }
  public short getShortfield51(){
  return shortfield51;
  }
  public void setShortfield51(short value){
  this.shortfield51 = value;
  }
  public short getShortfield52(){
  return shortfield52;
  }
  public void setShortfield52(short value){
  this.shortfield52 = value;
  }
  public short getShortfield53(){
  return shortfield53;
  }
  public void setShortfield53(short value){
  this.shortfield53 = value;
  }
  public short getShortfield54(){
  return shortfield54;
  }
  public void setShortfield54(short value){
  this.shortfield54 = value;
  }
  public short getShortfield55(){
  return shortfield55;
  }
  public void setShortfield55(short value){
  this.shortfield55 = value;
  }
  public short getShortfield56(){
  return shortfield56;
  }
  public void setShortfield56(short value){
  this.shortfield56 = value;
  }
  public short getShortfield57(){
  return shortfield57;
  }
  public void setShortfield57(short value){
  this.shortfield57 = value;
  }
  public short getShortfield58(){
  return shortfield58;
  }
  public void setShortfield58(short value){
  this.shortfield58 = value;
  }
  public short getShortfield59(){
  return shortfield59;
  }
  public void setShortfield59(short value){
  this.shortfield59 = value;
  }
  public short getShortfield60(){
  return shortfield60;
  }
  public void setShortfield60(short value){
  this.shortfield60 = value;
  }
  public short getShortfield61(){
  return shortfield61;
  }
  public void setShortfield61(short value){
  this.shortfield61 = value;
  }
  public short getShortfield62(){
  return shortfield62;
  }
  public void setShortfield62(short value){
  this.shortfield62 = value;
  }
  public short getShortfield63(){
  return shortfield63;
  }
  public void setShortfield63(short value){
  this.shortfield63 = value;
  }
  public short getShortfield64(){
  return shortfield64;
  }
  public void setShortfield64(short value){
  this.shortfield64 = value;
  }
  public short getShortfield65(){
  return shortfield65;
  }
  public void setShortfield65(short value){
  this.shortfield65 = value;
  }
  public short getShortfield66(){
  return shortfield66;
  }
  public void setShortfield66(short value){
  this.shortfield66 = value;
  }
  public short getShortfield67(){
  return shortfield67;
  }
  public void setShortfield67(short value){
  this.shortfield67 = value;
  }
  public short getShortfield68(){
  return shortfield68;
  }
  public void setShortfield68(short value){
  this.shortfield68 = value;
  }
  public short getShortfield69(){
  return shortfield69;
  }
  public void setShortfield69(short value){
  this.shortfield69 = value;
  }
  public short getShortfield70(){
  return shortfield70;
  }
  public void setShortfield70(short value){
  this.shortfield70 = value;
  }
  public short getShortfield71(){
  return shortfield71;
  }
  public void setShortfield71(short value){
  this.shortfield71 = value;
  }
  public short getShortfield72(){
  return shortfield72;
  }
  public void setShortfield72(short value){
  this.shortfield72 = value;
  }
  public short getShortfield73(){
  return shortfield73;
  }
  public void setShortfield73(short value){
  this.shortfield73 = value;
  }
  public short getShortfield74(){
  return shortfield74;
  }
  public void setShortfield74(short value){
  this.shortfield74 = value;
  }
  public short getShortfield75(){
  return shortfield75;
  }
  public void setShortfield75(short value){
  this.shortfield75 = value;
  }
  public short getShortfield76(){
  return shortfield76;
  }
  public void setShortfield76(short value){
  this.shortfield76 = value;
  }
  public short getShortfield77(){
  return shortfield77;
  }
  public void setShortfield77(short value){
  this.shortfield77 = value;
  }
  public short getShortfield78(){
  return shortfield78;
  }
  public void setShortfield78(short value){
  this.shortfield78 = value;
  }
  public short getShortfield79(){
  return shortfield79;
  }
  public void setShortfield79(short value){
  this.shortfield79 = value;
  }
  public short getShortfield80(){
  return shortfield80;
  }
  public void setShortfield80(short value){
  this.shortfield80 = value;
  }
  public short getShortfield81(){
  return shortfield81;
  }
  public void setShortfield81(short value){
  this.shortfield81 = value;
  }
  public short getShortfield82(){
  return shortfield82;
  }
  public void setShortfield82(short value){
  this.shortfield82 = value;
  }
  public short getShortfield83(){
  return shortfield83;
  }
  public void setShortfield83(short value){
  this.shortfield83 = value;
  }
  public short getShortfield84(){
  return shortfield84;
  }
  public void setShortfield84(short value){
  this.shortfield84 = value;
  }
  public short getShortfield85(){
  return shortfield85;
  }
  public void setShortfield85(short value){
  this.shortfield85 = value;
  }
  public short getShortfield86(){
  return shortfield86;
  }
  public void setShortfield86(short value){
  this.shortfield86 = value;
  }
  public short getShortfield87(){
  return shortfield87;
  }
  public void setShortfield87(short value){
  this.shortfield87 = value;
  }
  public short getShortfield88(){
  return shortfield88;
  }
  public void setShortfield88(short value){
  this.shortfield88 = value;
  }
  public short getShortfield89(){
  return shortfield89;
  }
  public void setShortfield89(short value){
  this.shortfield89 = value;
  }
  public short getShortfield90(){
  return shortfield90;
  }
  public void setShortfield90(short value){
  this.shortfield90 = value;
  }
  public short getShortfield91(){
  return shortfield91;
  }
  public void setShortfield91(short value){
  this.shortfield91 = value;
  }
  public short getShortfield92(){
  return shortfield92;
  }
  public void setShortfield92(short value){
  this.shortfield92 = value;
  }
  public short getShortfield93(){
  return shortfield93;
  }
  public void setShortfield93(short value){
  this.shortfield93 = value;
  }
  public short getShortfield94(){
  return shortfield94;
  }
  public void setShortfield94(short value){
  this.shortfield94 = value;
  }
  public short getShortfield95(){
  return shortfield95;
  }
  public void setShortfield95(short value){
  this.shortfield95 = value;
  }
  public short getShortfield96(){
  return shortfield96;
  }
  public void setShortfield96(short value){
  this.shortfield96 = value;
  }
  public short getShortfield97(){
  return shortfield97;
  }
  public void setShortfield97(short value){
  this.shortfield97 = value;
  }
  public short getShortfield98(){
  return shortfield98;
  }
  public void setShortfield98(short value){
  this.shortfield98 = value;
  }
  public short getShortfield99(){
  return shortfield99;
  }
  public void setShortfield99(short value){
  this.shortfield99 = value;
  }
  public short getShortfield100(){
  return shortfield100;
  }
  public void setShortfield100(short value){
  this.shortfield100 = value;
  }
  public short getShortfield101(){
  return shortfield101;
  }
  public void setShortfield101(short value){
  this.shortfield101 = value;
  }
  public short getShortfield102(){
  return shortfield102;
  }
  public void setShortfield102(short value){
  this.shortfield102 = value;
  }
  public short getShortfield103(){
  return shortfield103;
  }
  public void setShortfield103(short value){
  this.shortfield103 = value;
  }
  public short getShortfield104(){
  return shortfield104;
  }
  public void setShortfield104(short value){
  this.shortfield104 = value;
  }
  public short getShortfield105(){
  return shortfield105;
  }
  public void setShortfield105(short value){
  this.shortfield105 = value;
  }
  public short getShortfield106(){
  return shortfield106;
  }
  public void setShortfield106(short value){
  this.shortfield106 = value;
  }
  public short getShortfield107(){
  return shortfield107;
  }
  public void setShortfield107(short value){
  this.shortfield107 = value;
  }
  public short getShortfield108(){
  return shortfield108;
  }
  public void setShortfield108(short value){
  this.shortfield108 = value;
  }
  public short getShortfield109(){
  return shortfield109;
  }
  public void setShortfield109(short value){
  this.shortfield109 = value;
  }
  public short getShortfield110(){
  return shortfield110;
  }
  public void setShortfield110(short value){
  this.shortfield110 = value;
  }
  public short getShortfield111(){
  return shortfield111;
  }
  public void setShortfield111(short value){
  this.shortfield111 = value;
  }
  public short getShortfield112(){
  return shortfield112;
  }
  public void setShortfield112(short value){
  this.shortfield112 = value;
  }
  public short getShortfield113(){
  return shortfield113;
  }
  public void setShortfield113(short value){
  this.shortfield113 = value;
  }
  public short getShortfield114(){
  return shortfield114;
  }
  public void setShortfield114(short value){
  this.shortfield114 = value;
  }
  public short getShortfield115(){
  return shortfield115;
  }
  public void setShortfield115(short value){
  this.shortfield115 = value;
  }
  public short getShortfield116(){
  return shortfield116;
  }
  public void setShortfield116(short value){
  this.shortfield116 = value;
  }
  public short getShortfield117(){
  return shortfield117;
  }
  public void setShortfield117(short value){
  this.shortfield117 = value;
  }
  public short getShortfield118(){
  return shortfield118;
  }
  public void setShortfield118(short value){
  this.shortfield118 = value;
  }
  public short getShortfield119(){
  return shortfield119;
  }
  public void setShortfield119(short value){
  this.shortfield119 = value;
  }
  public short getShortfield120(){
  return shortfield120;
  }
  public void setShortfield120(short value){
  this.shortfield120 = value;
  }
  public short getShortfield121(){
  return shortfield121;
  }
  public void setShortfield121(short value){
  this.shortfield121 = value;
  }
  public short getShortfield122(){
  return shortfield122;
  }
  public void setShortfield122(short value){
  this.shortfield122 = value;
  }
  public short getShortfield123(){
  return shortfield123;
  }
  public void setShortfield123(short value){
  this.shortfield123 = value;
  }
  public short getShortfield124(){
  return shortfield124;
  }
  public void setShortfield124(short value){
  this.shortfield124 = value;
  }
  public short getShortfield125(){
  return shortfield125;
  }
  public void setShortfield125(short value){
  this.shortfield125 = value;
  }
  public short getShortfield126(){
  return shortfield126;
  }
  public void setShortfield126(short value){
  this.shortfield126 = value;
  }
  public short getShortfield127(){
  return shortfield127;
  }
  public void setShortfield127(short value){
  this.shortfield127 = value;
  }
  public short getShortfield128(){
  return shortfield128;
  }
  public void setShortfield128(short value){
  this.shortfield128 = value;
  }
  public short getShortfield129(){
  return shortfield129;
  }
  public void setShortfield129(short value){
  this.shortfield129 = value;
  }
  public short getShortfield130(){
  return shortfield130;
  }
  public void setShortfield130(short value){
  this.shortfield130 = value;
  }
  public short getShortfield131(){
  return shortfield131;
  }
  public void setShortfield131(short value){
  this.shortfield131 = value;
  }
  public short getShortfield132(){
  return shortfield132;
  }
  public void setShortfield132(short value){
  this.shortfield132 = value;
  }
  public short getShortfield133(){
  return shortfield133;
  }
  public void setShortfield133(short value){
  this.shortfield133 = value;
  }
  public short getShortfield134(){
  return shortfield134;
  }
  public void setShortfield134(short value){
  this.shortfield134 = value;
  }
  public short getShortfield135(){
  return shortfield135;
  }
  public void setShortfield135(short value){
  this.shortfield135 = value;
  }
  public short getShortfield136(){
  return shortfield136;
  }
  public void setShortfield136(short value){
  this.shortfield136 = value;
  }
  public short getShortfield137(){
  return shortfield137;
  }
  public void setShortfield137(short value){
  this.shortfield137 = value;
  }
  public short getShortfield138(){
  return shortfield138;
  }
  public void setShortfield138(short value){
  this.shortfield138 = value;
  }
  public short getShortfield139(){
  return shortfield139;
  }
  public void setShortfield139(short value){
  this.shortfield139 = value;
  }
  public short getShortfield140(){
  return shortfield140;
  }
  public void setShortfield140(short value){
  this.shortfield140 = value;
  }
  public short getShortfield141(){
  return shortfield141;
  }
  public void setShortfield141(short value){
  this.shortfield141 = value;
  }
  public short getShortfield142(){
  return shortfield142;
  }
  public void setShortfield142(short value){
  this.shortfield142 = value;
  }
  public short getShortfield143(){
  return shortfield143;
  }
  public void setShortfield143(short value){
  this.shortfield143 = value;
  }
  public short getShortfield144(){
  return shortfield144;
  }
  public void setShortfield144(short value){
  this.shortfield144 = value;
  }
  public short getShortfield145(){
  return shortfield145;
  }
  public void setShortfield145(short value){
  this.shortfield145 = value;
  }
  public short getShortfield146(){
  return shortfield146;
  }
  public void setShortfield146(short value){
  this.shortfield146 = value;
  }
  public short getShortfield147(){
  return shortfield147;
  }
  public void setShortfield147(short value){
  this.shortfield147 = value;
  }
  public short getShortfield148(){
  return shortfield148;
  }
  public void setShortfield148(short value){
  this.shortfield148 = value;
  }
  public short getShortfield149(){
  return shortfield149;
  }
  public void setShortfield149(short value){
  this.shortfield149 = value;
  }
  public void toData(DataOutput out) throws IOException{
  DataSerializer.writeString(strfield0, out);
  DataSerializer.writeString(strfield1, out);
  DataSerializer.writeString(strfield2, out);
  DataSerializer.writeString(strfield3, out);
  DataSerializer.writeString(strfield4, out);
  DataSerializer.writeString(strfield5, out);
  DataSerializer.writeString(strfield6, out);
  DataSerializer.writeString(strfield7, out);
  DataSerializer.writeString(strfield8, out);
  DataSerializer.writeString(strfield9, out);
  DataSerializer.writeString(strfield10, out);
  DataSerializer.writeString(strfield11, out);
  DataSerializer.writeString(strfield12, out);
  DataSerializer.writeString(strfield13, out);
  DataSerializer.writeString(strfield14, out);
  DataSerializer.writeString(strfield15, out);
  DataSerializer.writeString(strfield16, out);
  DataSerializer.writeString(strfield17, out);
  DataSerializer.writeString(strfield18, out);
  DataSerializer.writeString(strfield19, out);
  DataSerializer.writeString(strfield20, out);
  DataSerializer.writeString(strfield21, out);
  DataSerializer.writeString(strfield22, out);
  DataSerializer.writeString(strfield23, out);
  DataSerializer.writeString(strfield24, out);
  DataSerializer.writeString(strfield25, out);
  DataSerializer.writeString(strfield26, out);
  DataSerializer.writeString(strfield27, out);
  DataSerializer.writeString(strfield28, out);
  DataSerializer.writeString(strfield29, out);
  DataSerializer.writeString(strfield30, out);
  DataSerializer.writeString(strfield31, out);
  DataSerializer.writeString(strfield32, out);
  DataSerializer.writeString(strfield33, out);
  DataSerializer.writeString(strfield34, out);
  DataSerializer.writeString(strfield35, out);
  DataSerializer.writeString(strfield36, out);
  DataSerializer.writeString(strfield37, out);
  DataSerializer.writeString(strfield38, out);
  DataSerializer.writeString(strfield39, out);
  DataSerializer.writeString(strfield40, out);
  DataSerializer.writeString(strfield41, out);
  DataSerializer.writeString(strfield42, out);
  DataSerializer.writeString(strfield43, out);
  DataSerializer.writeString(strfield44, out);
  DataSerializer.writeString(strfield45, out);
  DataSerializer.writeString(strfield46, out);
  DataSerializer.writeString(strfield47, out);
  DataSerializer.writeString(strfield48, out);
  DataSerializer.writeString(strfield49, out);
  DataSerializer.writeString(strfield50, out);
  DataSerializer.writeString(strfield51, out);
  DataSerializer.writeString(strfield52, out);
  DataSerializer.writeString(strfield53, out);
  DataSerializer.writeString(strfield54, out);
  DataSerializer.writeString(strfield55, out);
  DataSerializer.writeString(strfield56, out);
  DataSerializer.writeString(strfield57, out);
  DataSerializer.writeString(strfield58, out);
  DataSerializer.writeString(strfield59, out);
  DataSerializer.writeString(strfield60, out);
  DataSerializer.writeString(strfield61, out);
  DataSerializer.writeString(strfield62, out);
  DataSerializer.writeString(strfield63, out);
  DataSerializer.writeString(strfield64, out);
  DataSerializer.writeString(strfield65, out);
  DataSerializer.writeString(strfield66, out);
  DataSerializer.writeString(strfield67, out);
  DataSerializer.writeString(strfield68, out);
  DataSerializer.writeString(strfield69, out);
  DataSerializer.writeString(strfield70, out);
  DataSerializer.writeString(strfield71, out);
  DataSerializer.writeString(strfield72, out);
  DataSerializer.writeString(strfield73, out);
  DataSerializer.writeString(strfield74, out);
  DataSerializer.writeString(strfield75, out);
  DataSerializer.writeString(strfield76, out);
  DataSerializer.writeString(strfield77, out);
  DataSerializer.writeString(strfield78, out);
  DataSerializer.writeString(strfield79, out);
  DataSerializer.writeString(strfield80, out);
  DataSerializer.writeString(strfield81, out);
  DataSerializer.writeString(strfield82, out);
  DataSerializer.writeString(strfield83, out);
  DataSerializer.writeString(strfield84, out);
  DataSerializer.writeString(strfield85, out);
  DataSerializer.writeString(strfield86, out);
  DataSerializer.writeString(strfield87, out);
  DataSerializer.writeString(strfield88, out);
  DataSerializer.writeString(strfield89, out);
  DataSerializer.writeString(strfield90, out);
  DataSerializer.writeString(strfield91, out);
  DataSerializer.writeString(strfield92, out);
  DataSerializer.writeString(strfield93, out);
  DataSerializer.writeString(strfield94, out);
  DataSerializer.writeString(strfield95, out);
  DataSerializer.writeString(strfield96, out);
  DataSerializer.writeString(strfield97, out);
  DataSerializer.writeString(strfield98, out);
  DataSerializer.writeString(strfield99, out);
  DataSerializer.writeString(strfield100, out);
  DataSerializer.writeString(strfield101, out);
  DataSerializer.writeString(strfield102, out);
  DataSerializer.writeString(strfield103, out);
  DataSerializer.writeString(strfield104, out);
  DataSerializer.writeString(strfield105, out);
  DataSerializer.writeString(strfield106, out);
  DataSerializer.writeString(strfield107, out);
  DataSerializer.writeString(strfield108, out);
  DataSerializer.writeString(strfield109, out);
  DataSerializer.writeString(strfield110, out);
  DataSerializer.writeString(strfield111, out);
  DataSerializer.writeString(strfield112, out);
  DataSerializer.writeString(strfield113, out);
  DataSerializer.writeString(strfield114, out);
  DataSerializer.writeString(strfield115, out);
  DataSerializer.writeString(strfield116, out);
  DataSerializer.writeString(strfield117, out);
  DataSerializer.writeString(strfield118, out);
  DataSerializer.writeString(strfield119, out);
  DataSerializer.writeString(strfield120, out);
  DataSerializer.writeString(strfield121, out);
  DataSerializer.writeString(strfield122, out);
  DataSerializer.writeString(strfield123, out);
  DataSerializer.writeString(strfield124, out);
  DataSerializer.writeString(strfield125, out);
  DataSerializer.writeString(strfield126, out);
  DataSerializer.writeString(strfield127, out);
  DataSerializer.writeString(strfield128, out);
  DataSerializer.writeString(strfield129, out);
  DataSerializer.writeString(strfield130, out);
  DataSerializer.writeString(strfield131, out);
  DataSerializer.writeString(strfield132, out);
  DataSerializer.writeString(strfield133, out);
  DataSerializer.writeString(strfield134, out);
  DataSerializer.writeString(strfield135, out);
  DataSerializer.writeString(strfield136, out);
  DataSerializer.writeString(strfield137, out);
  DataSerializer.writeString(strfield138, out);
  DataSerializer.writeString(strfield139, out);
  DataSerializer.writeString(strfield140, out);
  DataSerializer.writeString(strfield141, out);
  DataSerializer.writeString(strfield142, out);
  DataSerializer.writeString(strfield143, out);
  DataSerializer.writeString(strfield144, out);
  DataSerializer.writeString(strfield145, out);
  DataSerializer.writeString(strfield146, out);
  DataSerializer.writeString(strfield147, out);
  DataSerializer.writeString(strfield148, out);
  DataSerializer.writeString(strfield149, out);
  DataSerializer.writeString(strfield150, out);
  DataSerializer.writeString(strfield151, out);
  DataSerializer.writeString(strfield152, out);
  DataSerializer.writeString(strfield153, out);
  DataSerializer.writeString(strfield154, out);
  DataSerializer.writeString(strfield155, out);
  DataSerializer.writeString(strfield156, out);
  DataSerializer.writeString(strfield157, out);
  DataSerializer.writeString(strfield158, out);
  DataSerializer.writeString(strfield159, out);
  DataSerializer.writeString(strfield160, out);
  DataSerializer.writeString(strfield161, out);
  DataSerializer.writeString(strfield162, out);
  DataSerializer.writeString(strfield163, out);
  DataSerializer.writeString(strfield164, out);
  DataSerializer.writeString(strfield165, out);
  DataSerializer.writeString(strfield166, out);
  DataSerializer.writeString(strfield167, out);
  DataSerializer.writeString(strfield168, out);
  DataSerializer.writeString(strfield169, out);
  DataSerializer.writeString(strfield170, out);
  DataSerializer.writeString(strfield171, out);
  DataSerializer.writeString(strfield172, out);
  DataSerializer.writeString(strfield173, out);
  DataSerializer.writeString(strfield174, out);
  DataSerializer.writeString(strfield175, out);
  DataSerializer.writeString(strfield176, out);
  DataSerializer.writeString(strfield177, out);
  DataSerializer.writeString(strfield178, out);
  DataSerializer.writeString(strfield179, out);
  out.writeInt(id);
  out.writeInt(intfield1);
  out.writeInt(intfield2);
  out.writeInt(intfield3);
  out.writeInt(intfield4);
  out.writeInt(intfield5);
  out.writeInt(intfield6);
  out.writeInt(intfield7);
  out.writeInt(intfield8);
  out.writeInt(intfield9);
  out.writeInt(intfield10);
  out.writeInt(intfield11);
  out.writeInt(intfield12);
  out.writeInt(intfield13);
  out.writeInt(intfield14);
  out.writeInt(intfield15);
  out.writeInt(intfield16);
  out.writeInt(intfield17);
  out.writeInt(intfield18);
  out.writeInt(intfield19);
  out.writeInt(intfield20);
  out.writeInt(intfield21);
  out.writeInt(intfield22);
  out.writeInt(intfield23);
  out.writeInt(intfield24);
  out.writeInt(intfield25);
  out.writeInt(intfield26);
  out.writeInt(intfield27);
  out.writeInt(intfield28);
  out.writeInt(intfield29);
  out.writeInt(intfield30);
  out.writeInt(intfield31);
  out.writeInt(intfield32);
  out.writeInt(intfield33);
  out.writeInt(intfield34);
  out.writeInt(intfield35);
  out.writeInt(intfield36);
  out.writeInt(intfield37);
  out.writeInt(intfield38);
  out.writeInt(intfield39);
  out.writeInt(intfield40);
  out.writeInt(intfield41);
  out.writeInt(intfield42);
  out.writeInt(intfield43);
  out.writeInt(intfield44);
  out.writeInt(intfield45);
  out.writeInt(intfield46);
  out.writeInt(intfield47);
  out.writeInt(intfield48);
  out.writeInt(intfield49);
  out.writeInt(intfield50);
  out.writeInt(intfield51);
  out.writeInt(intfield52);
  out.writeInt(intfield53);
  out.writeInt(intfield54);
  out.writeInt(intfield55);
  out.writeInt(intfield56);
  out.writeInt(intfield57);
  out.writeInt(intfield58);
  out.writeInt(intfield59);
  out.writeInt(intfield60);
  out.writeInt(intfield61);
  out.writeInt(intfield62);
  out.writeInt(intfield63);
  out.writeInt(intfield64);
  out.writeInt(intfield65);
  out.writeInt(intfield66);
  out.writeInt(intfield67);
  out.writeInt(intfield68);
  out.writeInt(intfield69);
  out.writeInt(intfield70);
  out.writeInt(intfield71);
  out.writeInt(intfield72);
  out.writeInt(intfield73);
  out.writeInt(intfield74);
  out.writeInt(intfield75);
  out.writeInt(intfield76);
  out.writeInt(intfield77);
  out.writeInt(intfield78);
  out.writeInt(intfield79);
  out.writeInt(intfield80);
  out.writeInt(intfield81);
  out.writeInt(intfield82);
  out.writeInt(intfield83);
  out.writeInt(intfield84);
  out.writeInt(intfield85);
  out.writeInt(intfield86);
  out.writeInt(intfield87);
  out.writeInt(intfield88);
  out.writeInt(intfield89);
  out.writeInt(intfield90);
  out.writeInt(intfield91);
  out.writeInt(intfield92);
  out.writeInt(intfield93);
  out.writeInt(intfield94);
  out.writeInt(intfield95);
  out.writeInt(intfield96);
  out.writeInt(intfield97);
  out.writeInt(intfield98);
  out.writeInt(intfield99);
  out.writeInt(intfield100);
  out.writeInt(intfield101);
  out.writeInt(intfield102);
  out.writeInt(intfield103);
  out.writeInt(intfield104);
  out.writeInt(intfield105);
  out.writeInt(intfield106);
  out.writeInt(intfield107);
  out.writeInt(intfield108);
  out.writeInt(intfield109);
  out.writeInt(intfield110);
  out.writeInt(intfield111);
  out.writeInt(intfield112);
  out.writeInt(intfield113);
  out.writeInt(intfield114);
  out.writeInt(intfield115);
  out.writeInt(intfield116);
  out.writeInt(intfield117);
  out.writeInt(intfield118);
  out.writeInt(intfield119);
  out.writeInt(intfield120);
  out.writeInt(intfield121);
  out.writeInt(intfield122);
  out.writeInt(intfield123);
  out.writeInt(intfield124);
  out.writeInt(intfield125);
  out.writeInt(intfield126);
  out.writeInt(intfield127);
  out.writeInt(intfield128);
  out.writeInt(intfield129);
  out.writeInt(intfield130);
  out.writeInt(intfield131);
  out.writeInt(intfield132);
  out.writeInt(intfield133);
  out.writeInt(intfield134);
  out.writeInt(intfield135);
  out.writeInt(intfield136);
  out.writeInt(intfield137);
  out.writeInt(intfield138);
  out.writeInt(intfield139);
  out.writeInt(intfield140);
  out.writeInt(intfield141);
  out.writeInt(intfield142);
  out.writeInt(intfield143);
  out.writeInt(intfield144);
  out.writeInt(intfield145);
  out.writeInt(intfield146);
  out.writeInt(intfield147);
  out.writeInt(intfield148);
  out.writeInt(intfield149);
  out.writeInt(intfield150);
  out.writeInt(intfield151);
  out.writeInt(intfield152);
  out.writeInt(intfield153);
  out.writeInt(intfield154);
  out.writeInt(intfield155);
  out.writeInt(intfield156);
  out.writeInt(intfield157);
  out.writeInt(intfield158);
  out.writeInt(intfield159);
  out.writeInt(intfield160);
  out.writeInt(intfield161);
  out.writeInt(intfield162);
  out.writeInt(intfield163);
  out.writeInt(intfield164);
  out.writeInt(intfield165);
  out.writeInt(intfield166);
  out.writeInt(intfield167);
  out.writeInt(intfield168);
  out.writeInt(intfield169);
  out.writeInt(intfield170);
  out.writeInt(intfield171);
  out.writeInt(intfield172);
  out.writeInt(intfield173);
  out.writeInt(intfield174);
  out.writeInt(intfield175);
  out.writeInt(intfield176);
  out.writeDouble(doublefield0);
  out.writeDouble(doublefield1);
  out.writeDouble(doublefield2);
  out.writeDouble(doublefield3);
  out.writeDouble(doublefield4);
  out.writeDouble(doublefield5);
  out.writeDouble(doublefield6);
  out.writeDouble(doublefield7);
  out.writeDouble(doublefield8);
  out.writeDouble(doublefield9);
  out.writeDouble(doublefield10);
  out.writeDouble(doublefield11);
  out.writeDouble(doublefield12);
  out.writeDouble(doublefield13);
  out.writeDouble(doublefield14);
  out.writeDouble(doublefield15);
  out.writeDouble(doublefield16);
  out.writeDouble(doublefield17);
  out.writeDouble(doublefield18);
  out.writeDouble(doublefield19);
  out.writeDouble(doublefield20);
  out.writeDouble(doublefield21);
  out.writeDouble(doublefield22);
  out.writeDouble(doublefield23);
  out.writeDouble(doublefield24);
  out.writeDouble(doublefield25);
  out.writeDouble(doublefield26);
  out.writeDouble(doublefield27);
  out.writeDouble(doublefield28);
  out.writeDouble(doublefield29);
  out.writeDouble(doublefield30);
  out.writeDouble(doublefield31);
  out.writeDouble(doublefield32);
  out.writeDouble(doublefield33);
  out.writeDouble(doublefield34);
  out.writeDouble(doublefield35);
  out.writeDouble(doublefield36);
  out.writeDouble(doublefield37);
  out.writeDouble(doublefield38);
  out.writeDouble(doublefield39);
  out.writeDouble(doublefield40);
  out.writeDouble(doublefield41);
  out.writeDouble(doublefield42);
  out.writeDouble(doublefield43);
  out.writeDouble(doublefield44);
  out.writeDouble(doublefield45);
  out.writeDouble(doublefield46);
  out.writeDouble(doublefield47);
  out.writeDouble(doublefield48);
  out.writeDouble(doublefield49);
  out.writeDouble(doublefield50);
  out.writeDouble(doublefield51);
  out.writeDouble(doublefield52);
  out.writeDouble(doublefield53);
  out.writeDouble(doublefield54);
  out.writeDouble(doublefield55);
  out.writeDouble(doublefield56);
  out.writeDouble(doublefield57);
  out.writeDouble(doublefield58);
  out.writeDouble(doublefield59);
  out.writeDouble(doublefield60);
  out.writeDouble(doublefield61);
  out.writeDouble(doublefield62);
  out.writeDouble(doublefield63);
  out.writeDouble(doublefield64);
  out.writeDouble(doublefield65);
  out.writeDouble(doublefield66);
  out.writeDouble(doublefield67);
  out.writeDouble(doublefield68);
  out.writeDouble(doublefield69);
  out.writeDouble(doublefield70);
  out.writeDouble(doublefield71);
  out.writeDouble(doublefield72);
  out.writeDouble(doublefield73);
  out.writeDouble(doublefield74);
  out.writeDouble(doublefield75);
  out.writeDouble(doublefield76);
  out.writeDouble(doublefield77);
  out.writeDouble(doublefield78);
  out.writeDouble(doublefield79);
  out.writeDouble(doublefield80);
  out.writeDouble(doublefield81);
  out.writeDouble(doublefield82);
  out.writeDouble(doublefield83);
  out.writeDouble(doublefield84);
  out.writeDouble(doublefield85);
  out.writeDouble(doublefield86);
  out.writeDouble(doublefield87);
  out.writeDouble(doublefield88);
  out.writeDouble(doublefield89);
  out.writeDouble(doublefield90);
  out.writeDouble(doublefield91);
  out.writeDouble(doublefield92);
  out.writeDouble(doublefield93);
  out.writeDouble(doublefield94);
  out.writeDouble(doublefield95);
  out.writeDouble(doublefield96);
  out.writeDouble(doublefield97);
  out.writeDouble(doublefield98);
  out.writeDouble(doublefield99);
  out.writeDouble(doublefield100);
  out.writeDouble(doublefield101);
  out.writeDouble(doublefield102);
  out.writeDouble(doublefield103);
  out.writeDouble(doublefield104);
  out.writeDouble(doublefield105);
  out.writeDouble(doublefield106);
  out.writeDouble(doublefield107);
  out.writeDouble(doublefield108);
  out.writeDouble(doublefield109);
  out.writeDouble(doublefield110);
  out.writeDouble(doublefield111);
  out.writeDouble(doublefield112);
  out.writeDouble(doublefield113);
  out.writeDouble(doublefield114);
  out.writeDouble(doublefield115);
  out.writeDouble(doublefield116);
  out.writeDouble(doublefield117);
  out.writeDouble(doublefield118);
  out.writeDouble(doublefield119);
  out.writeDouble(doublefield120);
  out.writeDouble(doublefield121);
  out.writeDouble(doublefield122);
  out.writeDouble(doublefield123);
  out.writeDouble(doublefield124);
  out.writeDouble(doublefield125);
  out.writeDouble(doublefield126);
  out.writeDouble(doublefield127);
  out.writeDouble(doublefield128);
  out.writeDouble(doublefield129);
  out.writeDouble(doublefield130);
  out.writeDouble(doublefield131);
  out.writeDouble(doublefield132);
  out.writeDouble(doublefield133);
  out.writeDouble(doublefield134);
  out.writeDouble(doublefield135);
  out.writeDouble(doublefield136);
  out.writeDouble(doublefield137);
  out.writeDouble(doublefield138);
  out.writeDouble(doublefield139);
  out.writeDouble(doublefield140);
  out.writeDouble(doublefield141);
  out.writeDouble(doublefield142);
  out.writeDouble(doublefield143);
  out.writeDouble(doublefield144);
  out.writeDouble(doublefield145);
  out.writeDouble(doublefield146);
  out.writeDouble(doublefield147);
  out.writeDouble(doublefield148);
  out.writeDouble(doublefield149);
  out.writeDouble(doublefield150);
  out.writeDouble(doublefield151);
  out.writeDouble(doublefield152);
  out.writeDouble(doublefield153);
  out.writeDouble(doublefield154);
  out.writeDouble(doublefield155);
  out.writeDouble(doublefield156);
  out.writeDouble(doublefield157);
  out.writeDouble(doublefield158);
  out.writeDouble(doublefield159);
  out.writeByte(bytefield0);
  out.writeByte(bytefield1);
  out.writeByte(bytefield2);
  out.writeByte(bytefield3);
  out.writeByte(bytefield4);
  out.writeByte(bytefield5);
  out.writeByte(bytefield6);
  out.writeByte(bytefield7);
  out.writeByte(bytefield8);
  out.writeByte(bytefield9);
  out.writeByte(bytefield10);
  out.writeByte(bytefield11);
  out.writeByte(bytefield12);
  out.writeByte(bytefield13);
  out.writeByte(bytefield14);
  out.writeByte(bytefield15);
  out.writeByte(bytefield16);
  out.writeByte(bytefield17);
  out.writeByte(bytefield18);
  out.writeByte(bytefield19);
  out.writeByte(bytefield20);
  out.writeByte(bytefield21);
  out.writeByte(bytefield22);
  out.writeByte(bytefield23);
  out.writeByte(bytefield24);
  out.writeByte(bytefield25);
  out.writeByte(bytefield26);
  out.writeByte(bytefield27);
  out.writeByte(bytefield28);
  out.writeByte(bytefield29);
  out.writeByte(bytefield30);
  out.writeByte(bytefield31);
  out.writeByte(bytefield32);
  out.writeByte(bytefield33);
  out.writeByte(bytefield34);
  out.writeByte(bytefield35);
  out.writeByte(bytefield36);
  out.writeByte(bytefield37);
  out.writeByte(bytefield38);
  out.writeByte(bytefield39);
  out.writeByte(bytefield40);
  out.writeByte(bytefield41);
  out.writeByte(bytefield42);
  out.writeByte(bytefield43);
  out.writeByte(bytefield44);
  out.writeByte(bytefield45);
  out.writeByte(bytefield46);
  out.writeByte(bytefield47);
  out.writeByte(bytefield48);
  out.writeByte(bytefield49);
  out.writeByte(bytefield50);
  out.writeByte(bytefield51);
  out.writeByte(bytefield52);
  out.writeByte(bytefield53);
  out.writeByte(bytefield54);
  out.writeByte(bytefield55);
  out.writeByte(bytefield56);
  out.writeByte(bytefield57);
  out.writeByte(bytefield58);
  out.writeByte(bytefield59);
  out.writeByte(bytefield60);
  out.writeByte(bytefield61);
  out.writeByte(bytefield62);
  out.writeByte(bytefield63);
  out.writeByte(bytefield64);
  out.writeByte(bytefield65);
  out.writeByte(bytefield66);
  out.writeByte(bytefield67);
  out.writeByte(bytefield68);
  out.writeByte(bytefield69);
  out.writeByte(bytefield70);
  out.writeByte(bytefield71);
  out.writeByte(bytefield72);
  out.writeByte(bytefield73);
  out.writeByte(bytefield74);
  out.writeByte(bytefield75);
  out.writeByte(bytefield76);
  out.writeByte(bytefield77);
  out.writeByte(bytefield78);
  out.writeByte(bytefield79);
  out.writeByte(bytefield80);
  out.writeByte(bytefield81);
  out.writeByte(bytefield82);
  out.writeByte(bytefield83);
  out.writeByte(bytefield84);
  out.writeByte(bytefield85);
  out.writeByte(bytefield86);
  out.writeByte(bytefield87);
  out.writeByte(bytefield88);
  out.writeByte(bytefield89);
  out.writeByte(bytefield90);
  out.writeByte(bytefield91);
  out.writeByte(bytefield92);
  out.writeByte(bytefield93);
  out.writeByte(bytefield94);
  out.writeByte(bytefield95);
  out.writeByte(bytefield96);
  out.writeByte(bytefield97);
  out.writeByte(bytefield98);
  out.writeByte(bytefield99);
  out.writeByte(bytefield100);
  out.writeByte(bytefield101);
  out.writeByte(bytefield102);
  out.writeByte(bytefield103);
  out.writeByte(bytefield104);
  out.writeByte(bytefield105);
  out.writeByte(bytefield106);
  out.writeByte(bytefield107);
  out.writeByte(bytefield108);
  out.writeByte(bytefield109);
  out.writeByte(bytefield110);
  out.writeByte(bytefield111);
  out.writeByte(bytefield112);
  out.writeByte(bytefield113);
  out.writeByte(bytefield114);
  out.writeByte(bytefield115);
  out.writeByte(bytefield116);
  out.writeByte(bytefield117);
  out.writeByte(bytefield118);
  out.writeByte(bytefield119);
  out.writeByte(bytefield120);
  out.writeByte(bytefield121);
  out.writeByte(bytefield122);
  out.writeByte(bytefield123);
  out.writeByte(bytefield124);
  out.writeByte(bytefield125);
  out.writeByte(bytefield126);
  out.writeByte(bytefield127);
  out.writeByte(bytefield128);
  out.writeByte(bytefield129);
  out.writeByte(bytefield130);
  out.writeByte(bytefield131);
  out.writeByte(bytefield132);
  out.writeByte(bytefield133);
  out.writeByte(bytefield134);
  out.writeByte(bytefield135);
  out.writeByte(bytefield136);
  out.writeByte(bytefield137);
  out.writeByte(bytefield138);
  out.writeByte(bytefield139);
  out.writeByte(bytefield140);
  out.writeByte(bytefield141);
  out.writeByte(bytefield142);
  out.writeByte(bytefield143);
  out.writeByte(bytefield144);
  out.writeByte(bytefield145);
  out.writeByte(bytefield146);
  out.writeByte(bytefield147);
  out.writeByte(bytefield148);
  out.writeByte(bytefield149);
  out.writeChar(charfield0);
  out.writeChar(charfield1);
  out.writeChar(charfield2);
  out.writeChar(charfield3);
  out.writeChar(charfield4);
  out.writeChar(charfield5);
  out.writeChar(charfield6);
  out.writeChar(charfield7);
  out.writeChar(charfield8);
  out.writeChar(charfield9);
  out.writeChar(charfield10);
  out.writeChar(charfield11);
  out.writeChar(charfield12);
  out.writeChar(charfield13);
  out.writeChar(charfield14);
  out.writeChar(charfield15);
  out.writeChar(charfield16);
  out.writeChar(charfield17);
  out.writeChar(charfield18);
  out.writeChar(charfield19);
  out.writeChar(charfield20);
  out.writeChar(charfield21);
  out.writeChar(charfield22);
  out.writeChar(charfield23);
  out.writeChar(charfield24);
  out.writeChar(charfield25);
  out.writeChar(charfield26);
  out.writeChar(charfield27);
  out.writeChar(charfield28);
  out.writeChar(charfield29);
  out.writeChar(charfield30);
  out.writeChar(charfield31);
  out.writeChar(charfield32);
  out.writeChar(charfield33);
  out.writeChar(charfield34);
  out.writeChar(charfield35);
  out.writeChar(charfield36);
  out.writeChar(charfield37);
  out.writeChar(charfield38);
  out.writeChar(charfield39);
  out.writeChar(charfield40);
  out.writeChar(charfield41);
  out.writeChar(charfield42);
  out.writeChar(charfield43);
  out.writeChar(charfield44);
  out.writeChar(charfield45);
  out.writeChar(charfield46);
  out.writeChar(charfield47);
  out.writeChar(charfield48);
  out.writeChar(charfield49);
  out.writeChar(charfield50);
  out.writeChar(charfield51);
  out.writeChar(charfield52);
  out.writeChar(charfield53);
  out.writeChar(charfield54);
  out.writeChar(charfield55);
  out.writeChar(charfield56);
  out.writeChar(charfield57);
  out.writeChar(charfield58);
  out.writeChar(charfield59);
  out.writeChar(charfield60);
  out.writeChar(charfield61);
  out.writeChar(charfield62);
  out.writeChar(charfield63);
  out.writeChar(charfield64);
  out.writeChar(charfield65);
  out.writeChar(charfield66);
  out.writeChar(charfield67);
  out.writeChar(charfield68);
  out.writeChar(charfield69);
  out.writeChar(charfield70);
  out.writeChar(charfield71);
  out.writeChar(charfield72);
  out.writeChar(charfield73);
  out.writeChar(charfield74);
  out.writeChar(charfield75);
  out.writeChar(charfield76);
  out.writeChar(charfield77);
  out.writeChar(charfield78);
  out.writeChar(charfield79);
  out.writeChar(charfield80);
  out.writeChar(charfield81);
  out.writeChar(charfield82);
  out.writeChar(charfield83);
  out.writeChar(charfield84);
  out.writeChar(charfield85);
  out.writeChar(charfield86);
  out.writeChar(charfield87);
  out.writeChar(charfield88);
  out.writeChar(charfield89);
  out.writeChar(charfield90);
  out.writeChar(charfield91);
  out.writeChar(charfield92);
  out.writeChar(charfield93);
  out.writeChar(charfield94);
  out.writeChar(charfield95);
  out.writeChar(charfield96);
  out.writeChar(charfield97);
  out.writeChar(charfield98);
  out.writeChar(charfield99);
  out.writeChar(charfield100);
  out.writeChar(charfield101);
  out.writeChar(charfield102);
  out.writeChar(charfield103);
  out.writeChar(charfield104);
  out.writeChar(charfield105);
  out.writeChar(charfield106);
  out.writeChar(charfield107);
  out.writeChar(charfield108);
  out.writeChar(charfield109);
  out.writeChar(charfield110);
  out.writeChar(charfield111);
  out.writeChar(charfield112);
  out.writeChar(charfield113);
  out.writeChar(charfield114);
  out.writeChar(charfield115);
  out.writeChar(charfield116);
  out.writeChar(charfield117);
  out.writeChar(charfield118);
  out.writeChar(charfield119);
  out.writeChar(charfield120);
  out.writeChar(charfield121);
  out.writeChar(charfield122);
  out.writeChar(charfield123);
  out.writeChar(charfield124);
  out.writeChar(charfield125);
  out.writeChar(charfield126);
  out.writeChar(charfield127);
  out.writeChar(charfield128);
  out.writeChar(charfield129);
  out.writeChar(charfield130);
  out.writeChar(charfield131);
  out.writeChar(charfield132);
  out.writeChar(charfield133);
  out.writeChar(charfield134);
  out.writeChar(charfield135);
  out.writeChar(charfield136);
  out.writeChar(charfield137);
  out.writeChar(charfield138);
  out.writeChar(charfield139);
  out.writeChar(charfield140);
  out.writeChar(charfield141);
  out.writeChar(charfield142);
  out.writeChar(charfield143);
  out.writeChar(charfield144);
  out.writeChar(charfield145);
  out.writeChar(charfield146);
  out.writeChar(charfield147);
  out.writeChar(charfield148);
  out.writeChar(charfield149);
  out.writeBoolean(booleanfield0);
  out.writeBoolean(booleanfield1);
  out.writeBoolean(booleanfield2);
  out.writeBoolean(booleanfield3);
  out.writeBoolean(booleanfield4);
  out.writeBoolean(booleanfield5);
  out.writeBoolean(booleanfield6);
  out.writeBoolean(booleanfield7);
  out.writeBoolean(booleanfield8);
  out.writeBoolean(booleanfield9);
  out.writeBoolean(booleanfield10);
  out.writeBoolean(booleanfield11);
  out.writeBoolean(booleanfield12);
  out.writeBoolean(booleanfield13);
  out.writeBoolean(booleanfield14);
  out.writeBoolean(booleanfield15);
  out.writeBoolean(booleanfield16);
  out.writeBoolean(booleanfield17);
  out.writeBoolean(booleanfield18);
  out.writeBoolean(booleanfield19);
  out.writeBoolean(booleanfield20);
  out.writeBoolean(booleanfield21);
  out.writeBoolean(booleanfield22);
  out.writeBoolean(booleanfield23);
  out.writeBoolean(booleanfield24);
  out.writeBoolean(booleanfield25);
  out.writeBoolean(booleanfield26);
  out.writeBoolean(booleanfield27);
  out.writeBoolean(booleanfield28);
  out.writeBoolean(booleanfield29);
  out.writeBoolean(booleanfield30);
  out.writeBoolean(booleanfield31);
  out.writeBoolean(booleanfield32);
  out.writeBoolean(booleanfield33);
  out.writeBoolean(booleanfield34);
  out.writeBoolean(booleanfield35);
  out.writeBoolean(booleanfield36);
  out.writeBoolean(booleanfield37);
  out.writeBoolean(booleanfield38);
  out.writeBoolean(booleanfield39);
  out.writeBoolean(booleanfield40);
  out.writeBoolean(booleanfield41);
  out.writeBoolean(booleanfield42);
  out.writeBoolean(booleanfield43);
  out.writeBoolean(booleanfield44);
  out.writeBoolean(booleanfield45);
  out.writeBoolean(booleanfield46);
  out.writeBoolean(booleanfield47);
  out.writeBoolean(booleanfield48);
  out.writeBoolean(booleanfield49);
  out.writeBoolean(booleanfield50);
  out.writeBoolean(booleanfield51);
  out.writeBoolean(booleanfield52);
  out.writeBoolean(booleanfield53);
  out.writeBoolean(booleanfield54);
  out.writeBoolean(booleanfield55);
  out.writeBoolean(booleanfield56);
  out.writeBoolean(booleanfield57);
  out.writeBoolean(booleanfield58);
  out.writeBoolean(booleanfield59);
  out.writeBoolean(booleanfield60);
  out.writeBoolean(booleanfield61);
  out.writeBoolean(booleanfield62);
  out.writeBoolean(booleanfield63);
  out.writeBoolean(booleanfield64);
  out.writeBoolean(booleanfield65);
  out.writeBoolean(booleanfield66);
  out.writeBoolean(booleanfield67);
  out.writeBoolean(booleanfield68);
  out.writeBoolean(booleanfield69);
  out.writeBoolean(booleanfield70);
  out.writeBoolean(booleanfield71);
  out.writeBoolean(booleanfield72);
  out.writeBoolean(booleanfield73);
  out.writeBoolean(booleanfield74);
  out.writeBoolean(booleanfield75);
  out.writeBoolean(booleanfield76);
  out.writeBoolean(booleanfield77);
  out.writeBoolean(booleanfield78);
  out.writeBoolean(booleanfield79);
  out.writeBoolean(booleanfield80);
  out.writeBoolean(booleanfield81);
  out.writeBoolean(booleanfield82);
  out.writeBoolean(booleanfield83);
  out.writeBoolean(booleanfield84);
  out.writeBoolean(booleanfield85);
  out.writeBoolean(booleanfield86);
  out.writeBoolean(booleanfield87);
  out.writeBoolean(booleanfield88);
  out.writeBoolean(booleanfield89);
  out.writeBoolean(booleanfield90);
  out.writeBoolean(booleanfield91);
  out.writeBoolean(booleanfield92);
  out.writeBoolean(booleanfield93);
  out.writeBoolean(booleanfield94);
  out.writeBoolean(booleanfield95);
  out.writeBoolean(booleanfield96);
  out.writeBoolean(booleanfield97);
  out.writeBoolean(booleanfield98);
  out.writeBoolean(booleanfield99);
  out.writeBoolean(booleanfield100);
  out.writeBoolean(booleanfield101);
  out.writeBoolean(booleanfield102);
  out.writeBoolean(booleanfield103);
  out.writeBoolean(booleanfield104);
  out.writeBoolean(booleanfield105);
  out.writeBoolean(booleanfield106);
  out.writeBoolean(booleanfield107);
  out.writeBoolean(booleanfield108);
  out.writeBoolean(booleanfield109);
  out.writeBoolean(booleanfield110);
  out.writeBoolean(booleanfield111);
  out.writeBoolean(booleanfield112);
  out.writeBoolean(booleanfield113);
  out.writeBoolean(booleanfield114);
  out.writeBoolean(booleanfield115);
  out.writeBoolean(booleanfield116);
  out.writeBoolean(booleanfield117);
  out.writeBoolean(booleanfield118);
  out.writeBoolean(booleanfield119);
  out.writeBoolean(booleanfield120);
  out.writeBoolean(booleanfield121);
  out.writeBoolean(booleanfield122);
  out.writeBoolean(booleanfield123);
  out.writeBoolean(booleanfield124);
  out.writeBoolean(booleanfield125);
  out.writeBoolean(booleanfield126);
  out.writeBoolean(booleanfield127);
  out.writeBoolean(booleanfield128);
  out.writeBoolean(booleanfield129);
  out.writeBoolean(booleanfield130);
  out.writeBoolean(booleanfield131);
  out.writeBoolean(booleanfield132);
  out.writeBoolean(booleanfield133);
  out.writeBoolean(booleanfield134);
  out.writeBoolean(booleanfield135);
  out.writeBoolean(booleanfield136);
  out.writeBoolean(booleanfield137);
  out.writeBoolean(booleanfield138);
  out.writeBoolean(booleanfield139);
  out.writeBoolean(booleanfield140);
  out.writeBoolean(booleanfield141);
  out.writeBoolean(booleanfield142);
  out.writeBoolean(booleanfield143);
  out.writeBoolean(booleanfield144);
  out.writeBoolean(booleanfield145);
  out.writeBoolean(booleanfield146);
  out.writeBoolean(booleanfield147);
  out.writeBoolean(booleanfield148);
  out.writeBoolean(booleanfield149);
  out.writeFloat(floatfield0);
  out.writeFloat(floatfield1);
  out.writeFloat(floatfield2);
  out.writeFloat(floatfield3);
  out.writeFloat(floatfield4);
  out.writeFloat(floatfield5);
  out.writeFloat(floatfield6);
  out.writeFloat(floatfield7);
  out.writeFloat(floatfield8);
  out.writeFloat(floatfield9);
  out.writeFloat(floatfield10);
  out.writeFloat(floatfield11);
  out.writeFloat(floatfield12);
  out.writeFloat(floatfield13);
  out.writeFloat(floatfield14);
  out.writeFloat(floatfield15);
  out.writeFloat(floatfield16);
  out.writeFloat(floatfield17);
  out.writeFloat(floatfield18);
  out.writeFloat(floatfield19);
  out.writeFloat(floatfield20);
  out.writeFloat(floatfield21);
  out.writeFloat(floatfield22);
  out.writeFloat(floatfield23);
  out.writeFloat(floatfield24);
  out.writeFloat(floatfield25);
  out.writeFloat(floatfield26);
  out.writeFloat(floatfield27);
  out.writeFloat(floatfield28);
  out.writeFloat(floatfield29);
  out.writeFloat(floatfield30);
  out.writeFloat(floatfield31);
  out.writeFloat(floatfield32);
  out.writeFloat(floatfield33);
  out.writeFloat(floatfield34);
  out.writeFloat(floatfield35);
  out.writeFloat(floatfield36);
  out.writeFloat(floatfield37);
  out.writeFloat(floatfield38);
  out.writeFloat(floatfield39);
  out.writeFloat(floatfield40);
  out.writeFloat(floatfield41);
  out.writeFloat(floatfield42);
  out.writeFloat(floatfield43);
  out.writeFloat(floatfield44);
  out.writeFloat(floatfield45);
  out.writeFloat(floatfield46);
  out.writeFloat(floatfield47);
  out.writeFloat(floatfield48);
  out.writeFloat(floatfield49);
  out.writeFloat(floatfield50);
  out.writeFloat(floatfield51);
  out.writeFloat(floatfield52);
  out.writeFloat(floatfield53);
  out.writeFloat(floatfield54);
  out.writeFloat(floatfield55);
  out.writeFloat(floatfield56);
  out.writeFloat(floatfield57);
  out.writeFloat(floatfield58);
  out.writeFloat(floatfield59);
  out.writeFloat(floatfield60);
  out.writeFloat(floatfield61);
  out.writeFloat(floatfield62);
  out.writeFloat(floatfield63);
  out.writeFloat(floatfield64);
  out.writeFloat(floatfield65);
  out.writeFloat(floatfield66);
  out.writeFloat(floatfield67);
  out.writeFloat(floatfield68);
  out.writeFloat(floatfield69);
  out.writeFloat(floatfield70);
  out.writeFloat(floatfield71);
  out.writeFloat(floatfield72);
  out.writeFloat(floatfield73);
  out.writeFloat(floatfield74);
  out.writeFloat(floatfield75);
  out.writeFloat(floatfield76);
  out.writeFloat(floatfield77);
  out.writeFloat(floatfield78);
  out.writeFloat(floatfield79);
  out.writeFloat(floatfield80);
  out.writeFloat(floatfield81);
  out.writeFloat(floatfield82);
  out.writeFloat(floatfield83);
  out.writeFloat(floatfield84);
  out.writeFloat(floatfield85);
  out.writeFloat(floatfield86);
  out.writeFloat(floatfield87);
  out.writeFloat(floatfield88);
  out.writeFloat(floatfield89);
  out.writeFloat(floatfield90);
  out.writeFloat(floatfield91);
  out.writeFloat(floatfield92);
  out.writeFloat(floatfield93);
  out.writeFloat(floatfield94);
  out.writeFloat(floatfield95);
  out.writeFloat(floatfield96);
  out.writeFloat(floatfield97);
  out.writeFloat(floatfield98);
  out.writeFloat(floatfield99);
  out.writeFloat(floatfield100);
  out.writeFloat(floatfield101);
  out.writeFloat(floatfield102);
  out.writeFloat(floatfield103);
  out.writeFloat(floatfield104);
  out.writeFloat(floatfield105);
  out.writeFloat(floatfield106);
  out.writeFloat(floatfield107);
  out.writeFloat(floatfield108);
  out.writeFloat(floatfield109);
  out.writeFloat(floatfield110);
  out.writeFloat(floatfield111);
  out.writeFloat(floatfield112);
  out.writeFloat(floatfield113);
  out.writeFloat(floatfield114);
  out.writeFloat(floatfield115);
  out.writeFloat(floatfield116);
  out.writeFloat(floatfield117);
  out.writeFloat(floatfield118);
  out.writeFloat(floatfield119);
  out.writeFloat(floatfield120);
  out.writeFloat(floatfield121);
  out.writeFloat(floatfield122);
  out.writeFloat(floatfield123);
  out.writeFloat(floatfield124);
  out.writeFloat(floatfield125);
  out.writeFloat(floatfield126);
  out.writeFloat(floatfield127);
  out.writeFloat(floatfield128);
  out.writeFloat(floatfield129);
  out.writeFloat(floatfield130);
  out.writeFloat(floatfield131);
  out.writeFloat(floatfield132);
  out.writeFloat(floatfield133);
  out.writeFloat(floatfield134);
  out.writeFloat(floatfield135);
  out.writeFloat(floatfield136);
  out.writeFloat(floatfield137);
  out.writeFloat(floatfield138);
  out.writeFloat(floatfield139);
  out.writeFloat(floatfield140);
  out.writeFloat(floatfield141);
  out.writeFloat(floatfield142);
  out.writeFloat(floatfield143);
  out.writeFloat(floatfield144);
  out.writeFloat(floatfield145);
  out.writeFloat(floatfield146);
  out.writeFloat(floatfield147);
  out.writeFloat(floatfield148);
  out.writeFloat(floatfield149);
  out.writeLong(longfield0);
  out.writeLong(longfield1);
  out.writeLong(longfield2);
  out.writeLong(longfield3);
  out.writeLong(longfield4);
  out.writeLong(longfield5);
  out.writeLong(longfield6);
  out.writeLong(longfield7);
  out.writeLong(longfield8);
  out.writeLong(longfield9);
  out.writeLong(longfield10);
  out.writeLong(longfield11);
  out.writeLong(longfield12);
  out.writeLong(longfield13);
  out.writeLong(longfield14);
  out.writeLong(longfield15);
  out.writeLong(longfield16);
  out.writeLong(longfield17);
  out.writeLong(longfield18);
  out.writeLong(longfield19);
  out.writeLong(longfield20);
  out.writeLong(longfield21);
  out.writeLong(longfield22);
  out.writeLong(longfield23);
  out.writeLong(longfield24);
  out.writeLong(longfield25);
  out.writeLong(longfield26);
  out.writeLong(longfield27);
  out.writeLong(longfield28);
  out.writeLong(longfield29);
  out.writeLong(longfield30);
  out.writeLong(longfield31);
  out.writeLong(longfield32);
  out.writeLong(longfield33);
  out.writeLong(longfield34);
  out.writeLong(longfield35);
  out.writeLong(longfield36);
  out.writeLong(longfield37);
  out.writeLong(longfield38);
  out.writeLong(longfield39);
  out.writeLong(longfield40);
  out.writeLong(longfield41);
  out.writeLong(longfield42);
  out.writeLong(longfield43);
  out.writeLong(longfield44);
  out.writeLong(longfield45);
  out.writeLong(longfield46);
  out.writeLong(longfield47);
  out.writeLong(longfield48);
  out.writeLong(longfield49);
  out.writeLong(longfield50);
  out.writeLong(longfield51);
  out.writeLong(longfield52);
  out.writeLong(longfield53);
  out.writeLong(longfield54);
  out.writeLong(longfield55);
  out.writeLong(longfield56);
  out.writeLong(longfield57);
  out.writeLong(longfield58);
  out.writeLong(longfield59);
  out.writeLong(longfield60);
  out.writeLong(longfield61);
  out.writeLong(longfield62);
  out.writeLong(longfield63);
  out.writeLong(longfield64);
  out.writeLong(longfield65);
  out.writeLong(longfield66);
  out.writeLong(longfield67);
  out.writeLong(longfield68);
  out.writeLong(longfield69);
  out.writeLong(longfield70);
  out.writeLong(longfield71);
  out.writeLong(longfield72);
  out.writeLong(longfield73);
  out.writeLong(longfield74);
  out.writeLong(longfield75);
  out.writeLong(longfield76);
  out.writeLong(longfield77);
  out.writeLong(longfield78);
  out.writeLong(longfield79);
  out.writeLong(longfield80);
  out.writeLong(longfield81);
  out.writeLong(longfield82);
  out.writeLong(longfield83);
  out.writeLong(longfield84);
  out.writeLong(longfield85);
  out.writeLong(longfield86);
  out.writeLong(longfield87);
  out.writeLong(longfield88);
  out.writeLong(longfield89);
  out.writeLong(longfield90);
  out.writeLong(longfield91);
  out.writeLong(longfield92);
  out.writeLong(longfield93);
  out.writeLong(longfield94);
  out.writeLong(longfield95);
  out.writeLong(longfield96);
  out.writeLong(longfield97);
  out.writeLong(longfield98);
  out.writeLong(longfield99);
  out.writeLong(longfield100);
  out.writeLong(longfield101);
  out.writeLong(longfield102);
  out.writeLong(longfield103);
  out.writeLong(longfield104);
  out.writeLong(longfield105);
  out.writeLong(longfield106);
  out.writeLong(longfield107);
  out.writeLong(longfield108);
  out.writeLong(longfield109);
  out.writeLong(longfield110);
  out.writeLong(longfield111);
  out.writeLong(longfield112);
  out.writeLong(longfield113);
  out.writeLong(longfield114);
  out.writeLong(longfield115);
  out.writeLong(longfield116);
  out.writeLong(longfield117);
  out.writeLong(longfield118);
  out.writeLong(longfield119);
  out.writeLong(longfield120);
  out.writeLong(longfield121);
  out.writeLong(longfield122);
  out.writeLong(longfield123);
  out.writeLong(longfield124);
  out.writeLong(longfield125);
  out.writeLong(longfield126);
  out.writeLong(longfield127);
  out.writeLong(longfield128);
  out.writeLong(longfield129);
  out.writeLong(longfield130);
  out.writeLong(longfield131);
  out.writeLong(longfield132);
  out.writeLong(longfield133);
  out.writeLong(longfield134);
  out.writeLong(longfield135);
  out.writeLong(longfield136);
  out.writeLong(longfield137);
  out.writeLong(longfield138);
  out.writeLong(longfield139);
  out.writeLong(longfield140);
  out.writeLong(longfield141);
  out.writeLong(longfield142);
  out.writeLong(longfield143);
  out.writeLong(longfield144);
  out.writeLong(longfield145);
  out.writeLong(longfield146);
  out.writeLong(longfield147);
  out.writeLong(longfield148);
  out.writeLong(longfield149);
  out.writeShort(shortfield0);
  out.writeShort(shortfield1);
  out.writeShort(shortfield2);
  out.writeShort(shortfield3);
  out.writeShort(shortfield4);
  out.writeShort(shortfield5);
  out.writeShort(shortfield6);
  out.writeShort(shortfield7);
  out.writeShort(shortfield8);
  out.writeShort(shortfield9);
  out.writeShort(shortfield10);
  out.writeShort(shortfield11);
  out.writeShort(shortfield12);
  out.writeShort(shortfield13);
  out.writeShort(shortfield14);
  out.writeShort(shortfield15);
  out.writeShort(shortfield16);
  out.writeShort(shortfield17);
  out.writeShort(shortfield18);
  out.writeShort(shortfield19);
  out.writeShort(shortfield20);
  out.writeShort(shortfield21);
  out.writeShort(shortfield22);
  out.writeShort(shortfield23);
  out.writeShort(shortfield24);
  out.writeShort(shortfield25);
  out.writeShort(shortfield26);
  out.writeShort(shortfield27);
  out.writeShort(shortfield28);
  out.writeShort(shortfield29);
  out.writeShort(shortfield30);
  out.writeShort(shortfield31);
  out.writeShort(shortfield32);
  out.writeShort(shortfield33);
  out.writeShort(shortfield34);
  out.writeShort(shortfield35);
  out.writeShort(shortfield36);
  out.writeShort(shortfield37);
  out.writeShort(shortfield38);
  out.writeShort(shortfield39);
  out.writeShort(shortfield40);
  out.writeShort(shortfield41);
  out.writeShort(shortfield42);
  out.writeShort(shortfield43);
  out.writeShort(shortfield44);
  out.writeShort(shortfield45);
  out.writeShort(shortfield46);
  out.writeShort(shortfield47);
  out.writeShort(shortfield48);
  out.writeShort(shortfield49);
  out.writeShort(shortfield50);
  out.writeShort(shortfield51);
  out.writeShort(shortfield52);
  out.writeShort(shortfield53);
  out.writeShort(shortfield54);
  out.writeShort(shortfield55);
  out.writeShort(shortfield56);
  out.writeShort(shortfield57);
  out.writeShort(shortfield58);
  out.writeShort(shortfield59);
  out.writeShort(shortfield60);
  out.writeShort(shortfield61);
  out.writeShort(shortfield62);
  out.writeShort(shortfield63);
  out.writeShort(shortfield64);
  out.writeShort(shortfield65);
  out.writeShort(shortfield66);
  out.writeShort(shortfield67);
  out.writeShort(shortfield68);
  out.writeShort(shortfield69);
  out.writeShort(shortfield70);
  out.writeShort(shortfield71);
  out.writeShort(shortfield72);
  out.writeShort(shortfield73);
  out.writeShort(shortfield74);
  out.writeShort(shortfield75);
  out.writeShort(shortfield76);
  out.writeShort(shortfield77);
  out.writeShort(shortfield78);
  out.writeShort(shortfield79);
  out.writeShort(shortfield80);
  out.writeShort(shortfield81);
  out.writeShort(shortfield82);
  out.writeShort(shortfield83);
  out.writeShort(shortfield84);
  out.writeShort(shortfield85);
  out.writeShort(shortfield86);
  out.writeShort(shortfield87);
  out.writeShort(shortfield88);
  out.writeShort(shortfield89);
  out.writeShort(shortfield90);
  out.writeShort(shortfield91);
  out.writeShort(shortfield92);
  out.writeShort(shortfield93);
  out.writeShort(shortfield94);
  out.writeShort(shortfield95);
  out.writeShort(shortfield96);
  out.writeShort(shortfield97);
  out.writeShort(shortfield98);
  out.writeShort(shortfield99);
  out.writeShort(shortfield100);
  out.writeShort(shortfield101);
  out.writeShort(shortfield102);
  out.writeShort(shortfield103);
  out.writeShort(shortfield104);
  out.writeShort(shortfield105);
  out.writeShort(shortfield106);
  out.writeShort(shortfield107);
  out.writeShort(shortfield108);
  out.writeShort(shortfield109);
  out.writeShort(shortfield110);
  out.writeShort(shortfield111);
  out.writeShort(shortfield112);
  out.writeShort(shortfield113);
  out.writeShort(shortfield114);
  out.writeShort(shortfield115);
  out.writeShort(shortfield116);
  out.writeShort(shortfield117);
  out.writeShort(shortfield118);
  out.writeShort(shortfield119);
  out.writeShort(shortfield120);
  out.writeShort(shortfield121);
  out.writeShort(shortfield122);
  out.writeShort(shortfield123);
  out.writeShort(shortfield124);
  out.writeShort(shortfield125);
  out.writeShort(shortfield126);
  out.writeShort(shortfield127);
  out.writeShort(shortfield128);
  out.writeShort(shortfield129);
  out.writeShort(shortfield130);
  out.writeShort(shortfield131);
  out.writeShort(shortfield132);
  out.writeShort(shortfield133);
  out.writeShort(shortfield134);
  out.writeShort(shortfield135);
  out.writeShort(shortfield136);
  out.writeShort(shortfield137);
  out.writeShort(shortfield138);
  out.writeShort(shortfield139);
  out.writeShort(shortfield140);
  out.writeShort(shortfield141);
  out.writeShort(shortfield142);
  out.writeShort(shortfield143);
  out.writeShort(shortfield144);
  out.writeShort(shortfield145);
  out.writeShort(shortfield146);
  out.writeShort(shortfield147);
  out.writeShort(shortfield148);
  out.writeShort(shortfield149);
  }
  public void fromData(DataInput in) throws IOException,ClassNotFoundException{
  strfield0 = DataSerializer.readString(in);
  strfield1 = DataSerializer.readString(in);
  strfield2 = DataSerializer.readString(in);
  strfield3 = DataSerializer.readString(in);
  strfield4 = DataSerializer.readString(in);
  strfield5 = DataSerializer.readString(in);
  strfield6 = DataSerializer.readString(in);
  strfield7 = DataSerializer.readString(in);
  strfield8 = DataSerializer.readString(in);
  strfield9 = DataSerializer.readString(in);
  strfield10 = DataSerializer.readString(in);
  strfield11 = DataSerializer.readString(in);
  strfield12 = DataSerializer.readString(in);
  strfield13 = DataSerializer.readString(in);
  strfield14 = DataSerializer.readString(in);
  strfield15 = DataSerializer.readString(in);
  strfield16 = DataSerializer.readString(in);
  strfield17 = DataSerializer.readString(in);
  strfield18 = DataSerializer.readString(in);
  strfield19 = DataSerializer.readString(in);
  strfield20 = DataSerializer.readString(in);
  strfield21 = DataSerializer.readString(in);
  strfield22 = DataSerializer.readString(in);
  strfield23 = DataSerializer.readString(in);
  strfield24 = DataSerializer.readString(in);
  strfield25 = DataSerializer.readString(in);
  strfield26 = DataSerializer.readString(in);
  strfield27 = DataSerializer.readString(in);
  strfield28 = DataSerializer.readString(in);
  strfield29 = DataSerializer.readString(in);
  strfield30 = DataSerializer.readString(in);
  strfield31 = DataSerializer.readString(in);
  strfield32 = DataSerializer.readString(in);
  strfield33 = DataSerializer.readString(in);
  strfield34 = DataSerializer.readString(in);
  strfield35 = DataSerializer.readString(in);
  strfield36 = DataSerializer.readString(in);
  strfield37 = DataSerializer.readString(in);
  strfield38 = DataSerializer.readString(in);
  strfield39 = DataSerializer.readString(in);
  strfield40 = DataSerializer.readString(in);
  strfield41 = DataSerializer.readString(in);
  strfield42 = DataSerializer.readString(in);
  strfield43 = DataSerializer.readString(in);
  strfield44 = DataSerializer.readString(in);
  strfield45 = DataSerializer.readString(in);
  strfield46 = DataSerializer.readString(in);
  strfield47 = DataSerializer.readString(in);
  strfield48 = DataSerializer.readString(in);
  strfield49 = DataSerializer.readString(in);
  strfield50 = DataSerializer.readString(in);
  strfield51 = DataSerializer.readString(in);
  strfield52 = DataSerializer.readString(in);
  strfield53 = DataSerializer.readString(in);
  strfield54 = DataSerializer.readString(in);
  strfield55 = DataSerializer.readString(in);
  strfield56 = DataSerializer.readString(in);
  strfield57 = DataSerializer.readString(in);
  strfield58 = DataSerializer.readString(in);
  strfield59 = DataSerializer.readString(in);
  strfield60 = DataSerializer.readString(in);
  strfield61 = DataSerializer.readString(in);
  strfield62 = DataSerializer.readString(in);
  strfield63 = DataSerializer.readString(in);
  strfield64 = DataSerializer.readString(in);
  strfield65 = DataSerializer.readString(in);
  strfield66 = DataSerializer.readString(in);
  strfield67 = DataSerializer.readString(in);
  strfield68 = DataSerializer.readString(in);
  strfield69 = DataSerializer.readString(in);
  strfield70 = DataSerializer.readString(in);
  strfield71 = DataSerializer.readString(in);
  strfield72 = DataSerializer.readString(in);
  strfield73 = DataSerializer.readString(in);
  strfield74 = DataSerializer.readString(in);
  strfield75 = DataSerializer.readString(in);
  strfield76 = DataSerializer.readString(in);
  strfield77 = DataSerializer.readString(in);
  strfield78 = DataSerializer.readString(in);
  strfield79 = DataSerializer.readString(in);
  strfield80 = DataSerializer.readString(in);
  strfield81 = DataSerializer.readString(in);
  strfield82 = DataSerializer.readString(in);
  strfield83 = DataSerializer.readString(in);
  strfield84 = DataSerializer.readString(in);
  strfield85 = DataSerializer.readString(in);
  strfield86 = DataSerializer.readString(in);
  strfield87 = DataSerializer.readString(in);
  strfield88 = DataSerializer.readString(in);
  strfield89 = DataSerializer.readString(in);
  strfield90 = DataSerializer.readString(in);
  strfield91 = DataSerializer.readString(in);
  strfield92 = DataSerializer.readString(in);
  strfield93 = DataSerializer.readString(in);
  strfield94 = DataSerializer.readString(in);
  strfield95 = DataSerializer.readString(in);
  strfield96 = DataSerializer.readString(in);
  strfield97 = DataSerializer.readString(in);
  strfield98 = DataSerializer.readString(in);
  strfield99 = DataSerializer.readString(in);
  strfield100 = DataSerializer.readString(in);
  strfield101 = DataSerializer.readString(in);
  strfield102 = DataSerializer.readString(in);
  strfield103 = DataSerializer.readString(in);
  strfield104 = DataSerializer.readString(in);
  strfield105 = DataSerializer.readString(in);
  strfield106 = DataSerializer.readString(in);
  strfield107 = DataSerializer.readString(in);
  strfield108 = DataSerializer.readString(in);
  strfield109 = DataSerializer.readString(in);
  strfield110 = DataSerializer.readString(in);
  strfield111 = DataSerializer.readString(in);
  strfield112 = DataSerializer.readString(in);
  strfield113 = DataSerializer.readString(in);
  strfield114 = DataSerializer.readString(in);
  strfield115 = DataSerializer.readString(in);
  strfield116 = DataSerializer.readString(in);
  strfield117 = DataSerializer.readString(in);
  strfield118 = DataSerializer.readString(in);
  strfield119 = DataSerializer.readString(in);
  strfield120 = DataSerializer.readString(in);
  strfield121 = DataSerializer.readString(in);
  strfield122 = DataSerializer.readString(in);
  strfield123 = DataSerializer.readString(in);
  strfield124 = DataSerializer.readString(in);
  strfield125 = DataSerializer.readString(in);
  strfield126 = DataSerializer.readString(in);
  strfield127 = DataSerializer.readString(in);
  strfield128 = DataSerializer.readString(in);
  strfield129 = DataSerializer.readString(in);
  strfield130 = DataSerializer.readString(in);
  strfield131 = DataSerializer.readString(in);
  strfield132 = DataSerializer.readString(in);
  strfield133 = DataSerializer.readString(in);
  strfield134 = DataSerializer.readString(in);
  strfield135 = DataSerializer.readString(in);
  strfield136 = DataSerializer.readString(in);
  strfield137 = DataSerializer.readString(in);
  strfield138 = DataSerializer.readString(in);
  strfield139 = DataSerializer.readString(in);
  strfield140 = DataSerializer.readString(in);
  strfield141 = DataSerializer.readString(in);
  strfield142 = DataSerializer.readString(in);
  strfield143 = DataSerializer.readString(in);
  strfield144 = DataSerializer.readString(in);
  strfield145 = DataSerializer.readString(in);
  strfield146 = DataSerializer.readString(in);
  strfield147 = DataSerializer.readString(in);
  strfield148 = DataSerializer.readString(in);
  strfield149 = DataSerializer.readString(in);
  strfield150 = DataSerializer.readString(in);
  strfield151 = DataSerializer.readString(in);
  strfield152 = DataSerializer.readString(in);
  strfield153 = DataSerializer.readString(in);
  strfield154 = DataSerializer.readString(in);
  strfield155 = DataSerializer.readString(in);
  strfield156 = DataSerializer.readString(in);
  strfield157 = DataSerializer.readString(in);
  strfield158 = DataSerializer.readString(in);
  strfield159 = DataSerializer.readString(in);
  strfield160 = DataSerializer.readString(in);
  strfield161 = DataSerializer.readString(in);
  strfield162 = DataSerializer.readString(in);
  strfield163 = DataSerializer.readString(in);
  strfield164 = DataSerializer.readString(in);
  strfield165 = DataSerializer.readString(in);
  strfield166 = DataSerializer.readString(in);
  strfield167 = DataSerializer.readString(in);
  strfield168 = DataSerializer.readString(in);
  strfield169 = DataSerializer.readString(in);
  strfield170 = DataSerializer.readString(in);
  strfield171 = DataSerializer.readString(in);
  strfield172 = DataSerializer.readString(in);
  strfield173 = DataSerializer.readString(in);
  strfield174 = DataSerializer.readString(in);
  strfield175 = DataSerializer.readString(in);
  strfield176 = DataSerializer.readString(in);
  strfield177 = DataSerializer.readString(in);
  strfield178 = DataSerializer.readString(in);
  strfield179 = DataSerializer.readString(in);
  id = in.readInt();
  intfield1 = in.readInt();
  intfield2 = in.readInt();
  intfield3 = in.readInt();
  intfield4 = in.readInt();
  intfield5 = in.readInt();
  intfield6 = in.readInt();
  intfield7 = in.readInt();
  intfield8 = in.readInt();
  intfield9 = in.readInt();
  intfield10 = in.readInt();
  intfield11 = in.readInt();
  intfield12 = in.readInt();
  intfield13 = in.readInt();
  intfield14 = in.readInt();
  intfield15 = in.readInt();
  intfield16 = in.readInt();
  intfield17 = in.readInt();
  intfield18 = in.readInt();
  intfield19 = in.readInt();
  intfield20 = in.readInt();
  intfield21 = in.readInt();
  intfield22 = in.readInt();
  intfield23 = in.readInt();
  intfield24 = in.readInt();
  intfield25 = in.readInt();
  intfield26 = in.readInt();
  intfield27 = in.readInt();
  intfield28 = in.readInt();
  intfield29 = in.readInt();
  intfield30 = in.readInt();
  intfield31 = in.readInt();
  intfield32 = in.readInt();
  intfield33 = in.readInt();
  intfield34 = in.readInt();
  intfield35 = in.readInt();
  intfield36 = in.readInt();
  intfield37 = in.readInt();
  intfield38 = in.readInt();
  intfield39 = in.readInt();
  intfield40 = in.readInt();
  intfield41 = in.readInt();
  intfield42 = in.readInt();
  intfield43 = in.readInt();
  intfield44 = in.readInt();
  intfield45 = in.readInt();
  intfield46 = in.readInt();
  intfield47 = in.readInt();
  intfield48 = in.readInt();
  intfield49 = in.readInt();
  intfield50 = in.readInt();
  intfield51 = in.readInt();
  intfield52 = in.readInt();
  intfield53 = in.readInt();
  intfield54 = in.readInt();
  intfield55 = in.readInt();
  intfield56 = in.readInt();
  intfield57 = in.readInt();
  intfield58 = in.readInt();
  intfield59 = in.readInt();
  intfield60 = in.readInt();
  intfield61 = in.readInt();
  intfield62 = in.readInt();
  intfield63 = in.readInt();
  intfield64 = in.readInt();
  intfield65 = in.readInt();
  intfield66 = in.readInt();
  intfield67 = in.readInt();
  intfield68 = in.readInt();
  intfield69 = in.readInt();
  intfield70 = in.readInt();
  intfield71 = in.readInt();
  intfield72 = in.readInt();
  intfield73 = in.readInt();
  intfield74 = in.readInt();
  intfield75 = in.readInt();
  intfield76 = in.readInt();
  intfield77 = in.readInt();
  intfield78 = in.readInt();
  intfield79 = in.readInt();
  intfield80 = in.readInt();
  intfield81 = in.readInt();
  intfield82 = in.readInt();
  intfield83 = in.readInt();
  intfield84 = in.readInt();
  intfield85 = in.readInt();
  intfield86 = in.readInt();
  intfield87 = in.readInt();
  intfield88 = in.readInt();
  intfield89 = in.readInt();
  intfield90 = in.readInt();
  intfield91 = in.readInt();
  intfield92 = in.readInt();
  intfield93 = in.readInt();
  intfield94 = in.readInt();
  intfield95 = in.readInt();
  intfield96 = in.readInt();
  intfield97 = in.readInt();
  intfield98 = in.readInt();
  intfield99 = in.readInt();
  intfield100 = in.readInt();
  intfield101 = in.readInt();
  intfield102 = in.readInt();
  intfield103 = in.readInt();
  intfield104 = in.readInt();
  intfield105 = in.readInt();
  intfield106 = in.readInt();
  intfield107 = in.readInt();
  intfield108 = in.readInt();
  intfield109 = in.readInt();
  intfield110 = in.readInt();
  intfield111 = in.readInt();
  intfield112 = in.readInt();
  intfield113 = in.readInt();
  intfield114 = in.readInt();
  intfield115 = in.readInt();
  intfield116 = in.readInt();
  intfield117 = in.readInt();
  intfield118 = in.readInt();
  intfield119 = in.readInt();
  intfield120 = in.readInt();
  intfield121 = in.readInt();
  intfield122 = in.readInt();
  intfield123 = in.readInt();
  intfield124 = in.readInt();
  intfield125 = in.readInt();
  intfield126 = in.readInt();
  intfield127 = in.readInt();
  intfield128 = in.readInt();
  intfield129 = in.readInt();
  intfield130 = in.readInt();
  intfield131 = in.readInt();
  intfield132 = in.readInt();
  intfield133 = in.readInt();
  intfield134 = in.readInt();
  intfield135 = in.readInt();
  intfield136 = in.readInt();
  intfield137 = in.readInt();
  intfield138 = in.readInt();
  intfield139 = in.readInt();
  intfield140 = in.readInt();
  intfield141 = in.readInt();
  intfield142 = in.readInt();
  intfield143 = in.readInt();
  intfield144 = in.readInt();
  intfield145 = in.readInt();
  intfield146 = in.readInt();
  intfield147 = in.readInt();
  intfield148 = in.readInt();
  intfield149 = in.readInt();
  intfield150 = in.readInt();
  intfield151 = in.readInt();
  intfield152 = in.readInt();
  intfield153 = in.readInt();
  intfield154 = in.readInt();
  intfield155 = in.readInt();
  intfield156 = in.readInt();
  intfield157 = in.readInt();
  intfield158 = in.readInt();
  intfield159 = in.readInt();
  intfield160 = in.readInt();
  intfield161 = in.readInt();
  intfield162 = in.readInt();
  intfield163 = in.readInt();
  intfield164 = in.readInt();
  intfield165 = in.readInt();
  intfield166 = in.readInt();
  intfield167 = in.readInt();
  intfield168 = in.readInt();
  intfield169 = in.readInt();
  intfield170 = in.readInt();
  intfield171 = in.readInt();
  intfield172 = in.readInt();
  intfield173 = in.readInt();
  intfield174 = in.readInt();
  intfield175 = in.readInt();
  intfield176 = in.readInt();
  doublefield0 = in.readDouble();
  doublefield1 = in.readDouble();
  doublefield2 = in.readDouble();
  doublefield3 = in.readDouble();
  doublefield4 = in.readDouble();
  doublefield5 = in.readDouble();
  doublefield6 = in.readDouble();
  doublefield7 = in.readDouble();
  doublefield8 = in.readDouble();
  doublefield9 = in.readDouble();
  doublefield10 = in.readDouble();
  doublefield11 = in.readDouble();
  doublefield12 = in.readDouble();
  doublefield13 = in.readDouble();
  doublefield14 = in.readDouble();
  doublefield15 = in.readDouble();
  doublefield16 = in.readDouble();
  doublefield17 = in.readDouble();
  doublefield18 = in.readDouble();
  doublefield19 = in.readDouble();
  doublefield20 = in.readDouble();
  doublefield21 = in.readDouble();
  doublefield22 = in.readDouble();
  doublefield23 = in.readDouble();
  doublefield24 = in.readDouble();
  doublefield25 = in.readDouble();
  doublefield26 = in.readDouble();
  doublefield27 = in.readDouble();
  doublefield28 = in.readDouble();
  doublefield29 = in.readDouble();
  doublefield30 = in.readDouble();
  doublefield31 = in.readDouble();
  doublefield32 = in.readDouble();
  doublefield33 = in.readDouble();
  doublefield34 = in.readDouble();
  doublefield35 = in.readDouble();
  doublefield36 = in.readDouble();
  doublefield37 = in.readDouble();
  doublefield38 = in.readDouble();
  doublefield39 = in.readDouble();
  doublefield40 = in.readDouble();
  doublefield41 = in.readDouble();
  doublefield42 = in.readDouble();
  doublefield43 = in.readDouble();
  doublefield44 = in.readDouble();
  doublefield45 = in.readDouble();
  doublefield46 = in.readDouble();
  doublefield47 = in.readDouble();
  doublefield48 = in.readDouble();
  doublefield49 = in.readDouble();
  doublefield50 = in.readDouble();
  doublefield51 = in.readDouble();
  doublefield52 = in.readDouble();
  doublefield53 = in.readDouble();
  doublefield54 = in.readDouble();
  doublefield55 = in.readDouble();
  doublefield56 = in.readDouble();
  doublefield57 = in.readDouble();
  doublefield58 = in.readDouble();
  doublefield59 = in.readDouble();
  doublefield60 = in.readDouble();
  doublefield61 = in.readDouble();
  doublefield62 = in.readDouble();
  doublefield63 = in.readDouble();
  doublefield64 = in.readDouble();
  doublefield65 = in.readDouble();
  doublefield66 = in.readDouble();
  doublefield67 = in.readDouble();
  doublefield68 = in.readDouble();
  doublefield69 = in.readDouble();
  doublefield70 = in.readDouble();
  doublefield71 = in.readDouble();
  doublefield72 = in.readDouble();
  doublefield73 = in.readDouble();
  doublefield74 = in.readDouble();
  doublefield75 = in.readDouble();
  doublefield76 = in.readDouble();
  doublefield77 = in.readDouble();
  doublefield78 = in.readDouble();
  doublefield79 = in.readDouble();
  doublefield80 = in.readDouble();
  doublefield81 = in.readDouble();
  doublefield82 = in.readDouble();
  doublefield83 = in.readDouble();
  doublefield84 = in.readDouble();
  doublefield85 = in.readDouble();
  doublefield86 = in.readDouble();
  doublefield87 = in.readDouble();
  doublefield88 = in.readDouble();
  doublefield89 = in.readDouble();
  doublefield90 = in.readDouble();
  doublefield91 = in.readDouble();
  doublefield92 = in.readDouble();
  doublefield93 = in.readDouble();
  doublefield94 = in.readDouble();
  doublefield95 = in.readDouble();
  doublefield96 = in.readDouble();
  doublefield97 = in.readDouble();
  doublefield98 = in.readDouble();
  doublefield99 = in.readDouble();
  doublefield100 = in.readDouble();
  doublefield101 = in.readDouble();
  doublefield102 = in.readDouble();
  doublefield103 = in.readDouble();
  doublefield104 = in.readDouble();
  doublefield105 = in.readDouble();
  doublefield106 = in.readDouble();
  doublefield107 = in.readDouble();
  doublefield108 = in.readDouble();
  doublefield109 = in.readDouble();
  doublefield110 = in.readDouble();
  doublefield111 = in.readDouble();
  doublefield112 = in.readDouble();
  doublefield113 = in.readDouble();
  doublefield114 = in.readDouble();
  doublefield115 = in.readDouble();
  doublefield116 = in.readDouble();
  doublefield117 = in.readDouble();
  doublefield118 = in.readDouble();
  doublefield119 = in.readDouble();
  doublefield120 = in.readDouble();
  doublefield121 = in.readDouble();
  doublefield122 = in.readDouble();
  doublefield123 = in.readDouble();
  doublefield124 = in.readDouble();
  doublefield125 = in.readDouble();
  doublefield126 = in.readDouble();
  doublefield127 = in.readDouble();
  doublefield128 = in.readDouble();
  doublefield129 = in.readDouble();
  doublefield130 = in.readDouble();
  doublefield131 = in.readDouble();
  doublefield132 = in.readDouble();
  doublefield133 = in.readDouble();
  doublefield134 = in.readDouble();
  doublefield135 = in.readDouble();
  doublefield136 = in.readDouble();
  doublefield137 = in.readDouble();
  doublefield138 = in.readDouble();
  doublefield139 = in.readDouble();
  doublefield140 = in.readDouble();
  doublefield141 = in.readDouble();
  doublefield142 = in.readDouble();
  doublefield143 = in.readDouble();
  doublefield144 = in.readDouble();
  doublefield145 = in.readDouble();
  doublefield146 = in.readDouble();
  doublefield147 = in.readDouble();
  doublefield148 = in.readDouble();
  doublefield149 = in.readDouble();
  doublefield150 = in.readDouble();
  doublefield151 = in.readDouble();
  doublefield152 = in.readDouble();
  doublefield153 = in.readDouble();
  doublefield154 = in.readDouble();
  doublefield155 = in.readDouble();
  doublefield156 = in.readDouble();
  doublefield157 = in.readDouble();
  doublefield158 = in.readDouble();
  doublefield159 = in.readDouble();
  bytefield0 = in.readByte();
  bytefield1 = in.readByte();
  bytefield2 = in.readByte();
  bytefield3 = in.readByte();
  bytefield4 = in.readByte();
  bytefield5 = in.readByte();
  bytefield6 = in.readByte();
  bytefield7 = in.readByte();
  bytefield8 = in.readByte();
  bytefield9 = in.readByte();
  bytefield10 = in.readByte();
  bytefield11 = in.readByte();
  bytefield12 = in.readByte();
  bytefield13 = in.readByte();
  bytefield14 = in.readByte();
  bytefield15 = in.readByte();
  bytefield16 = in.readByte();
  bytefield17 = in.readByte();
  bytefield18 = in.readByte();
  bytefield19 = in.readByte();
  bytefield20 = in.readByte();
  bytefield21 = in.readByte();
  bytefield22 = in.readByte();
  bytefield23 = in.readByte();
  bytefield24 = in.readByte();
  bytefield25 = in.readByte();
  bytefield26 = in.readByte();
  bytefield27 = in.readByte();
  bytefield28 = in.readByte();
  bytefield29 = in.readByte();
  bytefield30 = in.readByte();
  bytefield31 = in.readByte();
  bytefield32 = in.readByte();
  bytefield33 = in.readByte();
  bytefield34 = in.readByte();
  bytefield35 = in.readByte();
  bytefield36 = in.readByte();
  bytefield37 = in.readByte();
  bytefield38 = in.readByte();
  bytefield39 = in.readByte();
  bytefield40 = in.readByte();
  bytefield41 = in.readByte();
  bytefield42 = in.readByte();
  bytefield43 = in.readByte();
  bytefield44 = in.readByte();
  bytefield45 = in.readByte();
  bytefield46 = in.readByte();
  bytefield47 = in.readByte();
  bytefield48 = in.readByte();
  bytefield49 = in.readByte();
  bytefield50 = in.readByte();
  bytefield51 = in.readByte();
  bytefield52 = in.readByte();
  bytefield53 = in.readByte();
  bytefield54 = in.readByte();
  bytefield55 = in.readByte();
  bytefield56 = in.readByte();
  bytefield57 = in.readByte();
  bytefield58 = in.readByte();
  bytefield59 = in.readByte();
  bytefield60 = in.readByte();
  bytefield61 = in.readByte();
  bytefield62 = in.readByte();
  bytefield63 = in.readByte();
  bytefield64 = in.readByte();
  bytefield65 = in.readByte();
  bytefield66 = in.readByte();
  bytefield67 = in.readByte();
  bytefield68 = in.readByte();
  bytefield69 = in.readByte();
  bytefield70 = in.readByte();
  bytefield71 = in.readByte();
  bytefield72 = in.readByte();
  bytefield73 = in.readByte();
  bytefield74 = in.readByte();
  bytefield75 = in.readByte();
  bytefield76 = in.readByte();
  bytefield77 = in.readByte();
  bytefield78 = in.readByte();
  bytefield79 = in.readByte();
  bytefield80 = in.readByte();
  bytefield81 = in.readByte();
  bytefield82 = in.readByte();
  bytefield83 = in.readByte();
  bytefield84 = in.readByte();
  bytefield85 = in.readByte();
  bytefield86 = in.readByte();
  bytefield87 = in.readByte();
  bytefield88 = in.readByte();
  bytefield89 = in.readByte();
  bytefield90 = in.readByte();
  bytefield91 = in.readByte();
  bytefield92 = in.readByte();
  bytefield93 = in.readByte();
  bytefield94 = in.readByte();
  bytefield95 = in.readByte();
  bytefield96 = in.readByte();
  bytefield97 = in.readByte();
  bytefield98 = in.readByte();
  bytefield99 = in.readByte();
  bytefield100 = in.readByte();
  bytefield101 = in.readByte();
  bytefield102 = in.readByte();
  bytefield103 = in.readByte();
  bytefield104 = in.readByte();
  bytefield105 = in.readByte();
  bytefield106 = in.readByte();
  bytefield107 = in.readByte();
  bytefield108 = in.readByte();
  bytefield109 = in.readByte();
  bytefield110 = in.readByte();
  bytefield111 = in.readByte();
  bytefield112 = in.readByte();
  bytefield113 = in.readByte();
  bytefield114 = in.readByte();
  bytefield115 = in.readByte();
  bytefield116 = in.readByte();
  bytefield117 = in.readByte();
  bytefield118 = in.readByte();
  bytefield119 = in.readByte();
  bytefield120 = in.readByte();
  bytefield121 = in.readByte();
  bytefield122 = in.readByte();
  bytefield123 = in.readByte();
  bytefield124 = in.readByte();
  bytefield125 = in.readByte();
  bytefield126 = in.readByte();
  bytefield127 = in.readByte();
  bytefield128 = in.readByte();
  bytefield129 = in.readByte();
  bytefield130 = in.readByte();
  bytefield131 = in.readByte();
  bytefield132 = in.readByte();
  bytefield133 = in.readByte();
  bytefield134 = in.readByte();
  bytefield135 = in.readByte();
  bytefield136 = in.readByte();
  bytefield137 = in.readByte();
  bytefield138 = in.readByte();
  bytefield139 = in.readByte();
  bytefield140 = in.readByte();
  bytefield141 = in.readByte();
  bytefield142 = in.readByte();
  bytefield143 = in.readByte();
  bytefield144 = in.readByte();
  bytefield145 = in.readByte();
  bytefield146 = in.readByte();
  bytefield147 = in.readByte();
  bytefield148 = in.readByte();
  bytefield149 = in.readByte();
  charfield0 = in.readChar();
  charfield1 = in.readChar();
  charfield2 = in.readChar();
  charfield3 = in.readChar();
  charfield4 = in.readChar();
  charfield5 = in.readChar();
  charfield6 = in.readChar();
  charfield7 = in.readChar();
  charfield8 = in.readChar();
  charfield9 = in.readChar();
  charfield10 = in.readChar();
  charfield11 = in.readChar();
  charfield12 = in.readChar();
  charfield13 = in.readChar();
  charfield14 = in.readChar();
  charfield15 = in.readChar();
  charfield16 = in.readChar();
  charfield17 = in.readChar();
  charfield18 = in.readChar();
  charfield19 = in.readChar();
  charfield20 = in.readChar();
  charfield21 = in.readChar();
  charfield22 = in.readChar();
  charfield23 = in.readChar();
  charfield24 = in.readChar();
  charfield25 = in.readChar();
  charfield26 = in.readChar();
  charfield27 = in.readChar();
  charfield28 = in.readChar();
  charfield29 = in.readChar();
  charfield30 = in.readChar();
  charfield31 = in.readChar();
  charfield32 = in.readChar();
  charfield33 = in.readChar();
  charfield34 = in.readChar();
  charfield35 = in.readChar();
  charfield36 = in.readChar();
  charfield37 = in.readChar();
  charfield38 = in.readChar();
  charfield39 = in.readChar();
  charfield40 = in.readChar();
  charfield41 = in.readChar();
  charfield42 = in.readChar();
  charfield43 = in.readChar();
  charfield44 = in.readChar();
  charfield45 = in.readChar();
  charfield46 = in.readChar();
  charfield47 = in.readChar();
  charfield48 = in.readChar();
  charfield49 = in.readChar();
  charfield50 = in.readChar();
  charfield51 = in.readChar();
  charfield52 = in.readChar();
  charfield53 = in.readChar();
  charfield54 = in.readChar();
  charfield55 = in.readChar();
  charfield56 = in.readChar();
  charfield57 = in.readChar();
  charfield58 = in.readChar();
  charfield59 = in.readChar();
  charfield60 = in.readChar();
  charfield61 = in.readChar();
  charfield62 = in.readChar();
  charfield63 = in.readChar();
  charfield64 = in.readChar();
  charfield65 = in.readChar();
  charfield66 = in.readChar();
  charfield67 = in.readChar();
  charfield68 = in.readChar();
  charfield69 = in.readChar();
  charfield70 = in.readChar();
  charfield71 = in.readChar();
  charfield72 = in.readChar();
  charfield73 = in.readChar();
  charfield74 = in.readChar();
  charfield75 = in.readChar();
  charfield76 = in.readChar();
  charfield77 = in.readChar();
  charfield78 = in.readChar();
  charfield79 = in.readChar();
  charfield80 = in.readChar();
  charfield81 = in.readChar();
  charfield82 = in.readChar();
  charfield83 = in.readChar();
  charfield84 = in.readChar();
  charfield85 = in.readChar();
  charfield86 = in.readChar();
  charfield87 = in.readChar();
  charfield88 = in.readChar();
  charfield89 = in.readChar();
  charfield90 = in.readChar();
  charfield91 = in.readChar();
  charfield92 = in.readChar();
  charfield93 = in.readChar();
  charfield94 = in.readChar();
  charfield95 = in.readChar();
  charfield96 = in.readChar();
  charfield97 = in.readChar();
  charfield98 = in.readChar();
  charfield99 = in.readChar();
  charfield100 = in.readChar();
  charfield101 = in.readChar();
  charfield102 = in.readChar();
  charfield103 = in.readChar();
  charfield104 = in.readChar();
  charfield105 = in.readChar();
  charfield106 = in.readChar();
  charfield107 = in.readChar();
  charfield108 = in.readChar();
  charfield109 = in.readChar();
  charfield110 = in.readChar();
  charfield111 = in.readChar();
  charfield112 = in.readChar();
  charfield113 = in.readChar();
  charfield114 = in.readChar();
  charfield115 = in.readChar();
  charfield116 = in.readChar();
  charfield117 = in.readChar();
  charfield118 = in.readChar();
  charfield119 = in.readChar();
  charfield120 = in.readChar();
  charfield121 = in.readChar();
  charfield122 = in.readChar();
  charfield123 = in.readChar();
  charfield124 = in.readChar();
  charfield125 = in.readChar();
  charfield126 = in.readChar();
  charfield127 = in.readChar();
  charfield128 = in.readChar();
  charfield129 = in.readChar();
  charfield130 = in.readChar();
  charfield131 = in.readChar();
  charfield132 = in.readChar();
  charfield133 = in.readChar();
  charfield134 = in.readChar();
  charfield135 = in.readChar();
  charfield136 = in.readChar();
  charfield137 = in.readChar();
  charfield138 = in.readChar();
  charfield139 = in.readChar();
  charfield140 = in.readChar();
  charfield141 = in.readChar();
  charfield142 = in.readChar();
  charfield143 = in.readChar();
  charfield144 = in.readChar();
  charfield145 = in.readChar();
  charfield146 = in.readChar();
  charfield147 = in.readChar();
  charfield148 = in.readChar();
  charfield149 = in.readChar();
  booleanfield0 = in.readBoolean();
  booleanfield1 = in.readBoolean();
  booleanfield2 = in.readBoolean();
  booleanfield3 = in.readBoolean();
  booleanfield4 = in.readBoolean();
  booleanfield5 = in.readBoolean();
  booleanfield6 = in.readBoolean();
  booleanfield7 = in.readBoolean();
  booleanfield8 = in.readBoolean();
  booleanfield9 = in.readBoolean();
  booleanfield10 = in.readBoolean();
  booleanfield11 = in.readBoolean();
  booleanfield12 = in.readBoolean();
  booleanfield13 = in.readBoolean();
  booleanfield14 = in.readBoolean();
  booleanfield15 = in.readBoolean();
  booleanfield16 = in.readBoolean();
  booleanfield17 = in.readBoolean();
  booleanfield18 = in.readBoolean();
  booleanfield19 = in.readBoolean();
  booleanfield20 = in.readBoolean();
  booleanfield21 = in.readBoolean();
  booleanfield22 = in.readBoolean();
  booleanfield23 = in.readBoolean();
  booleanfield24 = in.readBoolean();
  booleanfield25 = in.readBoolean();
  booleanfield26 = in.readBoolean();
  booleanfield27 = in.readBoolean();
  booleanfield28 = in.readBoolean();
  booleanfield29 = in.readBoolean();
  booleanfield30 = in.readBoolean();
  booleanfield31 = in.readBoolean();
  booleanfield32 = in.readBoolean();
  booleanfield33 = in.readBoolean();
  booleanfield34 = in.readBoolean();
  booleanfield35 = in.readBoolean();
  booleanfield36 = in.readBoolean();
  booleanfield37 = in.readBoolean();
  booleanfield38 = in.readBoolean();
  booleanfield39 = in.readBoolean();
  booleanfield40 = in.readBoolean();
  booleanfield41 = in.readBoolean();
  booleanfield42 = in.readBoolean();
  booleanfield43 = in.readBoolean();
  booleanfield44 = in.readBoolean();
  booleanfield45 = in.readBoolean();
  booleanfield46 = in.readBoolean();
  booleanfield47 = in.readBoolean();
  booleanfield48 = in.readBoolean();
  booleanfield49 = in.readBoolean();
  booleanfield50 = in.readBoolean();
  booleanfield51 = in.readBoolean();
  booleanfield52 = in.readBoolean();
  booleanfield53 = in.readBoolean();
  booleanfield54 = in.readBoolean();
  booleanfield55 = in.readBoolean();
  booleanfield56 = in.readBoolean();
  booleanfield57 = in.readBoolean();
  booleanfield58 = in.readBoolean();
  booleanfield59 = in.readBoolean();
  booleanfield60 = in.readBoolean();
  booleanfield61 = in.readBoolean();
  booleanfield62 = in.readBoolean();
  booleanfield63 = in.readBoolean();
  booleanfield64 = in.readBoolean();
  booleanfield65 = in.readBoolean();
  booleanfield66 = in.readBoolean();
  booleanfield67 = in.readBoolean();
  booleanfield68 = in.readBoolean();
  booleanfield69 = in.readBoolean();
  booleanfield70 = in.readBoolean();
  booleanfield71 = in.readBoolean();
  booleanfield72 = in.readBoolean();
  booleanfield73 = in.readBoolean();
  booleanfield74 = in.readBoolean();
  booleanfield75 = in.readBoolean();
  booleanfield76 = in.readBoolean();
  booleanfield77 = in.readBoolean();
  booleanfield78 = in.readBoolean();
  booleanfield79 = in.readBoolean();
  booleanfield80 = in.readBoolean();
  booleanfield81 = in.readBoolean();
  booleanfield82 = in.readBoolean();
  booleanfield83 = in.readBoolean();
  booleanfield84 = in.readBoolean();
  booleanfield85 = in.readBoolean();
  booleanfield86 = in.readBoolean();
  booleanfield87 = in.readBoolean();
  booleanfield88 = in.readBoolean();
  booleanfield89 = in.readBoolean();
  booleanfield90 = in.readBoolean();
  booleanfield91 = in.readBoolean();
  booleanfield92 = in.readBoolean();
  booleanfield93 = in.readBoolean();
  booleanfield94 = in.readBoolean();
  booleanfield95 = in.readBoolean();
  booleanfield96 = in.readBoolean();
  booleanfield97 = in.readBoolean();
  booleanfield98 = in.readBoolean();
  booleanfield99 = in.readBoolean();
  booleanfield100 = in.readBoolean();
  booleanfield101 = in.readBoolean();
  booleanfield102 = in.readBoolean();
  booleanfield103 = in.readBoolean();
  booleanfield104 = in.readBoolean();
  booleanfield105 = in.readBoolean();
  booleanfield106 = in.readBoolean();
  booleanfield107 = in.readBoolean();
  booleanfield108 = in.readBoolean();
  booleanfield109 = in.readBoolean();
  booleanfield110 = in.readBoolean();
  booleanfield111 = in.readBoolean();
  booleanfield112 = in.readBoolean();
  booleanfield113 = in.readBoolean();
  booleanfield114 = in.readBoolean();
  booleanfield115 = in.readBoolean();
  booleanfield116 = in.readBoolean();
  booleanfield117 = in.readBoolean();
  booleanfield118 = in.readBoolean();
  booleanfield119 = in.readBoolean();
  booleanfield120 = in.readBoolean();
  booleanfield121 = in.readBoolean();
  booleanfield122 = in.readBoolean();
  booleanfield123 = in.readBoolean();
  booleanfield124 = in.readBoolean();
  booleanfield125 = in.readBoolean();
  booleanfield126 = in.readBoolean();
  booleanfield127 = in.readBoolean();
  booleanfield128 = in.readBoolean();
  booleanfield129 = in.readBoolean();
  booleanfield130 = in.readBoolean();
  booleanfield131 = in.readBoolean();
  booleanfield132 = in.readBoolean();
  booleanfield133 = in.readBoolean();
  booleanfield134 = in.readBoolean();
  booleanfield135 = in.readBoolean();
  booleanfield136 = in.readBoolean();
  booleanfield137 = in.readBoolean();
  booleanfield138 = in.readBoolean();
  booleanfield139 = in.readBoolean();
  booleanfield140 = in.readBoolean();
  booleanfield141 = in.readBoolean();
  booleanfield142 = in.readBoolean();
  booleanfield143 = in.readBoolean();
  booleanfield144 = in.readBoolean();
  booleanfield145 = in.readBoolean();
  booleanfield146 = in.readBoolean();
  booleanfield147 = in.readBoolean();
  booleanfield148 = in.readBoolean();
  booleanfield149 = in.readBoolean();
  floatfield0 = in.readFloat();
  floatfield1 = in.readFloat();
  floatfield2 = in.readFloat();
  floatfield3 = in.readFloat();
  floatfield4 = in.readFloat();
  floatfield5 = in.readFloat();
  floatfield6 = in.readFloat();
  floatfield7 = in.readFloat();
  floatfield8 = in.readFloat();
  floatfield9 = in.readFloat();
  floatfield10 = in.readFloat();
  floatfield11 = in.readFloat();
  floatfield12 = in.readFloat();
  floatfield13 = in.readFloat();
  floatfield14 = in.readFloat();
  floatfield15 = in.readFloat();
  floatfield16 = in.readFloat();
  floatfield17 = in.readFloat();
  floatfield18 = in.readFloat();
  floatfield19 = in.readFloat();
  floatfield20 = in.readFloat();
  floatfield21 = in.readFloat();
  floatfield22 = in.readFloat();
  floatfield23 = in.readFloat();
  floatfield24 = in.readFloat();
  floatfield25 = in.readFloat();
  floatfield26 = in.readFloat();
  floatfield27 = in.readFloat();
  floatfield28 = in.readFloat();
  floatfield29 = in.readFloat();
  floatfield30 = in.readFloat();
  floatfield31 = in.readFloat();
  floatfield32 = in.readFloat();
  floatfield33 = in.readFloat();
  floatfield34 = in.readFloat();
  floatfield35 = in.readFloat();
  floatfield36 = in.readFloat();
  floatfield37 = in.readFloat();
  floatfield38 = in.readFloat();
  floatfield39 = in.readFloat();
  floatfield40 = in.readFloat();
  floatfield41 = in.readFloat();
  floatfield42 = in.readFloat();
  floatfield43 = in.readFloat();
  floatfield44 = in.readFloat();
  floatfield45 = in.readFloat();
  floatfield46 = in.readFloat();
  floatfield47 = in.readFloat();
  floatfield48 = in.readFloat();
  floatfield49 = in.readFloat();
  floatfield50 = in.readFloat();
  floatfield51 = in.readFloat();
  floatfield52 = in.readFloat();
  floatfield53 = in.readFloat();
  floatfield54 = in.readFloat();
  floatfield55 = in.readFloat();
  floatfield56 = in.readFloat();
  floatfield57 = in.readFloat();
  floatfield58 = in.readFloat();
  floatfield59 = in.readFloat();
  floatfield60 = in.readFloat();
  floatfield61 = in.readFloat();
  floatfield62 = in.readFloat();
  floatfield63 = in.readFloat();
  floatfield64 = in.readFloat();
  floatfield65 = in.readFloat();
  floatfield66 = in.readFloat();
  floatfield67 = in.readFloat();
  floatfield68 = in.readFloat();
  floatfield69 = in.readFloat();
  floatfield70 = in.readFloat();
  floatfield71 = in.readFloat();
  floatfield72 = in.readFloat();
  floatfield73 = in.readFloat();
  floatfield74 = in.readFloat();
  floatfield75 = in.readFloat();
  floatfield76 = in.readFloat();
  floatfield77 = in.readFloat();
  floatfield78 = in.readFloat();
  floatfield79 = in.readFloat();
  floatfield80 = in.readFloat();
  floatfield81 = in.readFloat();
  floatfield82 = in.readFloat();
  floatfield83 = in.readFloat();
  floatfield84 = in.readFloat();
  floatfield85 = in.readFloat();
  floatfield86 = in.readFloat();
  floatfield87 = in.readFloat();
  floatfield88 = in.readFloat();
  floatfield89 = in.readFloat();
  floatfield90 = in.readFloat();
  floatfield91 = in.readFloat();
  floatfield92 = in.readFloat();
  floatfield93 = in.readFloat();
  floatfield94 = in.readFloat();
  floatfield95 = in.readFloat();
  floatfield96 = in.readFloat();
  floatfield97 = in.readFloat();
  floatfield98 = in.readFloat();
  floatfield99 = in.readFloat();
  floatfield100 = in.readFloat();
  floatfield101 = in.readFloat();
  floatfield102 = in.readFloat();
  floatfield103 = in.readFloat();
  floatfield104 = in.readFloat();
  floatfield105 = in.readFloat();
  floatfield106 = in.readFloat();
  floatfield107 = in.readFloat();
  floatfield108 = in.readFloat();
  floatfield109 = in.readFloat();
  floatfield110 = in.readFloat();
  floatfield111 = in.readFloat();
  floatfield112 = in.readFloat();
  floatfield113 = in.readFloat();
  floatfield114 = in.readFloat();
  floatfield115 = in.readFloat();
  floatfield116 = in.readFloat();
  floatfield117 = in.readFloat();
  floatfield118 = in.readFloat();
  floatfield119 = in.readFloat();
  floatfield120 = in.readFloat();
  floatfield121 = in.readFloat();
  floatfield122 = in.readFloat();
  floatfield123 = in.readFloat();
  floatfield124 = in.readFloat();
  floatfield125 = in.readFloat();
  floatfield126 = in.readFloat();
  floatfield127 = in.readFloat();
  floatfield128 = in.readFloat();
  floatfield129 = in.readFloat();
  floatfield130 = in.readFloat();
  floatfield131 = in.readFloat();
  floatfield132 = in.readFloat();
  floatfield133 = in.readFloat();
  floatfield134 = in.readFloat();
  floatfield135 = in.readFloat();
  floatfield136 = in.readFloat();
  floatfield137 = in.readFloat();
  floatfield138 = in.readFloat();
  floatfield139 = in.readFloat();
  floatfield140 = in.readFloat();
  floatfield141 = in.readFloat();
  floatfield142 = in.readFloat();
  floatfield143 = in.readFloat();
  floatfield144 = in.readFloat();
  floatfield145 = in.readFloat();
  floatfield146 = in.readFloat();
  floatfield147 = in.readFloat();
  floatfield148 = in.readFloat();
  floatfield149 = in.readFloat();
  longfield0 = in.readLong();
  longfield1 = in.readLong();
  longfield2 = in.readLong();
  longfield3 = in.readLong();
  longfield4 = in.readLong();
  longfield5 = in.readLong();
  longfield6 = in.readLong();
  longfield7 = in.readLong();
  longfield8 = in.readLong();
  longfield9 = in.readLong();
  longfield10 = in.readLong();
  longfield11 = in.readLong();
  longfield12 = in.readLong();
  longfield13 = in.readLong();
  longfield14 = in.readLong();
  longfield15 = in.readLong();
  longfield16 = in.readLong();
  longfield17 = in.readLong();
  longfield18 = in.readLong();
  longfield19 = in.readLong();
  longfield20 = in.readLong();
  longfield21 = in.readLong();
  longfield22 = in.readLong();
  longfield23 = in.readLong();
  longfield24 = in.readLong();
  longfield25 = in.readLong();
  longfield26 = in.readLong();
  longfield27 = in.readLong();
  longfield28 = in.readLong();
  longfield29 = in.readLong();
  longfield30 = in.readLong();
  longfield31 = in.readLong();
  longfield32 = in.readLong();
  longfield33 = in.readLong();
  longfield34 = in.readLong();
  longfield35 = in.readLong();
  longfield36 = in.readLong();
  longfield37 = in.readLong();
  longfield38 = in.readLong();
  longfield39 = in.readLong();
  longfield40 = in.readLong();
  longfield41 = in.readLong();
  longfield42 = in.readLong();
  longfield43 = in.readLong();
  longfield44 = in.readLong();
  longfield45 = in.readLong();
  longfield46 = in.readLong();
  longfield47 = in.readLong();
  longfield48 = in.readLong();
  longfield49 = in.readLong();
  longfield50 = in.readLong();
  longfield51 = in.readLong();
  longfield52 = in.readLong();
  longfield53 = in.readLong();
  longfield54 = in.readLong();
  longfield55 = in.readLong();
  longfield56 = in.readLong();
  longfield57 = in.readLong();
  longfield58 = in.readLong();
  longfield59 = in.readLong();
  longfield60 = in.readLong();
  longfield61 = in.readLong();
  longfield62 = in.readLong();
  longfield63 = in.readLong();
  longfield64 = in.readLong();
  longfield65 = in.readLong();
  longfield66 = in.readLong();
  longfield67 = in.readLong();
  longfield68 = in.readLong();
  longfield69 = in.readLong();
  longfield70 = in.readLong();
  longfield71 = in.readLong();
  longfield72 = in.readLong();
  longfield73 = in.readLong();
  longfield74 = in.readLong();
  longfield75 = in.readLong();
  longfield76 = in.readLong();
  longfield77 = in.readLong();
  longfield78 = in.readLong();
  longfield79 = in.readLong();
  longfield80 = in.readLong();
  longfield81 = in.readLong();
  longfield82 = in.readLong();
  longfield83 = in.readLong();
  longfield84 = in.readLong();
  longfield85 = in.readLong();
  longfield86 = in.readLong();
  longfield87 = in.readLong();
  longfield88 = in.readLong();
  longfield89 = in.readLong();
  longfield90 = in.readLong();
  longfield91 = in.readLong();
  longfield92 = in.readLong();
  longfield93 = in.readLong();
  longfield94 = in.readLong();
  longfield95 = in.readLong();
  longfield96 = in.readLong();
  longfield97 = in.readLong();
  longfield98 = in.readLong();
  longfield99 = in.readLong();
  longfield100 = in.readLong();
  longfield101 = in.readLong();
  longfield102 = in.readLong();
  longfield103 = in.readLong();
  longfield104 = in.readLong();
  longfield105 = in.readLong();
  longfield106 = in.readLong();
  longfield107 = in.readLong();
  longfield108 = in.readLong();
  longfield109 = in.readLong();
  longfield110 = in.readLong();
  longfield111 = in.readLong();
  longfield112 = in.readLong();
  longfield113 = in.readLong();
  longfield114 = in.readLong();
  longfield115 = in.readLong();
  longfield116 = in.readLong();
  longfield117 = in.readLong();
  longfield118 = in.readLong();
  longfield119 = in.readLong();
  longfield120 = in.readLong();
  longfield121 = in.readLong();
  longfield122 = in.readLong();
  longfield123 = in.readLong();
  longfield124 = in.readLong();
  longfield125 = in.readLong();
  longfield126 = in.readLong();
  longfield127 = in.readLong();
  longfield128 = in.readLong();
  longfield129 = in.readLong();
  longfield130 = in.readLong();
  longfield131 = in.readLong();
  longfield132 = in.readLong();
  longfield133 = in.readLong();
  longfield134 = in.readLong();
  longfield135 = in.readLong();
  longfield136 = in.readLong();
  longfield137 = in.readLong();
  longfield138 = in.readLong();
  longfield139 = in.readLong();
  longfield140 = in.readLong();
  longfield141 = in.readLong();
  longfield142 = in.readLong();
  longfield143 = in.readLong();
  longfield144 = in.readLong();
  longfield145 = in.readLong();
  longfield146 = in.readLong();
  longfield147 = in.readLong();
  longfield148 = in.readLong();
  longfield149 = in.readLong();
  shortfield0 = in.readShort();
  shortfield1 = in.readShort();
  shortfield2 = in.readShort();
  shortfield3 = in.readShort();
  shortfield4 = in.readShort();
  shortfield5 = in.readShort();
  shortfield6 = in.readShort();
  shortfield7 = in.readShort();
  shortfield8 = in.readShort();
  shortfield9 = in.readShort();
  shortfield10 = in.readShort();
  shortfield11 = in.readShort();
  shortfield12 = in.readShort();
  shortfield13 = in.readShort();
  shortfield14 = in.readShort();
  shortfield15 = in.readShort();
  shortfield16 = in.readShort();
  shortfield17 = in.readShort();
  shortfield18 = in.readShort();
  shortfield19 = in.readShort();
  shortfield20 = in.readShort();
  shortfield21 = in.readShort();
  shortfield22 = in.readShort();
  shortfield23 = in.readShort();
  shortfield24 = in.readShort();
  shortfield25 = in.readShort();
  shortfield26 = in.readShort();
  shortfield27 = in.readShort();
  shortfield28 = in.readShort();
  shortfield29 = in.readShort();
  shortfield30 = in.readShort();
  shortfield31 = in.readShort();
  shortfield32 = in.readShort();
  shortfield33 = in.readShort();
  shortfield34 = in.readShort();
  shortfield35 = in.readShort();
  shortfield36 = in.readShort();
  shortfield37 = in.readShort();
  shortfield38 = in.readShort();
  shortfield39 = in.readShort();
  shortfield40 = in.readShort();
  shortfield41 = in.readShort();
  shortfield42 = in.readShort();
  shortfield43 = in.readShort();
  shortfield44 = in.readShort();
  shortfield45 = in.readShort();
  shortfield46 = in.readShort();
  shortfield47 = in.readShort();
  shortfield48 = in.readShort();
  shortfield49 = in.readShort();
  shortfield50 = in.readShort();
  shortfield51 = in.readShort();
  shortfield52 = in.readShort();
  shortfield53 = in.readShort();
  shortfield54 = in.readShort();
  shortfield55 = in.readShort();
  shortfield56 = in.readShort();
  shortfield57 = in.readShort();
  shortfield58 = in.readShort();
  shortfield59 = in.readShort();
  shortfield60 = in.readShort();
  shortfield61 = in.readShort();
  shortfield62 = in.readShort();
  shortfield63 = in.readShort();
  shortfield64 = in.readShort();
  shortfield65 = in.readShort();
  shortfield66 = in.readShort();
  shortfield67 = in.readShort();
  shortfield68 = in.readShort();
  shortfield69 = in.readShort();
  shortfield70 = in.readShort();
  shortfield71 = in.readShort();
  shortfield72 = in.readShort();
  shortfield73 = in.readShort();
  shortfield74 = in.readShort();
  shortfield75 = in.readShort();
  shortfield76 = in.readShort();
  shortfield77 = in.readShort();
  shortfield78 = in.readShort();
  shortfield79 = in.readShort();
  shortfield80 = in.readShort();
  shortfield81 = in.readShort();
  shortfield82 = in.readShort();
  shortfield83 = in.readShort();
  shortfield84 = in.readShort();
  shortfield85 = in.readShort();
  shortfield86 = in.readShort();
  shortfield87 = in.readShort();
  shortfield88 = in.readShort();
  shortfield89 = in.readShort();
  shortfield90 = in.readShort();
  shortfield91 = in.readShort();
  shortfield92 = in.readShort();
  shortfield93 = in.readShort();
  shortfield94 = in.readShort();
  shortfield95 = in.readShort();
  shortfield96 = in.readShort();
  shortfield97 = in.readShort();
  shortfield98 = in.readShort();
  shortfield99 = in.readShort();
  shortfield100 = in.readShort();
  shortfield101 = in.readShort();
  shortfield102 = in.readShort();
  shortfield103 = in.readShort();
  shortfield104 = in.readShort();
  shortfield105 = in.readShort();
  shortfield106 = in.readShort();
  shortfield107 = in.readShort();
  shortfield108 = in.readShort();
  shortfield109 = in.readShort();
  shortfield110 = in.readShort();
  shortfield111 = in.readShort();
  shortfield112 = in.readShort();
  shortfield113 = in.readShort();
  shortfield114 = in.readShort();
  shortfield115 = in.readShort();
  shortfield116 = in.readShort();
  shortfield117 = in.readShort();
  shortfield118 = in.readShort();
  shortfield119 = in.readShort();
  shortfield120 = in.readShort();
  shortfield121 = in.readShort();
  shortfield122 = in.readShort();
  shortfield123 = in.readShort();
  shortfield124 = in.readShort();
  shortfield125 = in.readShort();
  shortfield126 = in.readShort();
  shortfield127 = in.readShort();
  shortfield128 = in.readShort();
  shortfield129 = in.readShort();
  shortfield130 = in.readShort();
  shortfield131 = in.readShort();
  shortfield132 = in.readShort();
  shortfield133 = in.readShort();
  shortfield134 = in.readShort();
  shortfield135 = in.readShort();
  shortfield136 = in.readShort();
  shortfield137 = in.readShort();
  shortfield138 = in.readShort();
  shortfield139 = in.readShort();
  shortfield140 = in.readShort();
  shortfield141 = in.readShort();
  shortfield142 = in.readShort();
  shortfield143 = in.readShort();
  shortfield144 = in.readShort();
  shortfield145 = in.readShort();
  shortfield146 = in.readShort();
  shortfield147 = in.readShort();
  shortfield148 = in.readShort();
  shortfield149 = in.readShort();
  }
  public static FlatObject[] genObjects(int msgNum)  throws IOException{
  NumberFormat format=new DecimalFormat("#####");
  format.setMinimumIntegerDigits(5);
  FlatObject[] result=new FlatObject[msgNum];
  for(int i=0;i<msgNum;i++){
  result[i]=new FlatObject();
  result[i].strfield0=format.format(i);
  result[i].strfield1=format.format(i);
  result[i].strfield2=format.format(i);
  result[i].strfield3=format.format(i);
  result[i].strfield4=format.format(i);
  result[i].strfield5=format.format(i);
  result[i].strfield6=format.format(i);
  result[i].strfield7=format.format(i);
  result[i].strfield8=format.format(i);
  result[i].strfield9=format.format(i);
  result[i].strfield10=format.format(i);
  result[i].strfield11=format.format(i);
  result[i].strfield12=format.format(i);
  result[i].strfield13=format.format(i);
  result[i].strfield14=format.format(i);
  result[i].strfield15=format.format(i);
  result[i].strfield16=format.format(i);
  result[i].strfield17=format.format(i);
  result[i].strfield18=format.format(i);
  result[i].strfield19=format.format(i);
  result[i].strfield20=format.format(i);
  result[i].strfield21=format.format(i);
  result[i].strfield22=format.format(i);
  result[i].strfield23=format.format(i);
  result[i].strfield24=format.format(i);
  result[i].strfield25=format.format(i);
  result[i].strfield26=format.format(i);
  result[i].strfield27=format.format(i);
  result[i].strfield28=format.format(i);
  result[i].strfield29=format.format(i);
  result[i].strfield30=format.format(i);
  result[i].strfield31=format.format(i);
  result[i].strfield32=format.format(i);
  result[i].strfield33=format.format(i);
  result[i].strfield34=format.format(i);
  result[i].strfield35=format.format(i);
  result[i].strfield36=format.format(i);
  result[i].strfield37=format.format(i);
  result[i].strfield38=format.format(i);
  result[i].strfield39=format.format(i);
  result[i].strfield40=format.format(i);
  result[i].strfield41=format.format(i);
  result[i].strfield42=format.format(i);
  result[i].strfield43=format.format(i);
  result[i].strfield44=format.format(i);
  result[i].strfield45=format.format(i);
  result[i].strfield46=format.format(i);
  result[i].strfield47=format.format(i);
  result[i].strfield48=format.format(i);
  result[i].strfield49=format.format(i);
  result[i].strfield50=format.format(i);
  result[i].strfield51=format.format(i);
  result[i].strfield52=format.format(i);
  result[i].strfield53=format.format(i);
  result[i].strfield54=format.format(i);
  result[i].strfield55=format.format(i);
  result[i].strfield56=format.format(i);
  result[i].strfield57=format.format(i);
  result[i].strfield58=format.format(i);
  result[i].strfield59=format.format(i);
  result[i].strfield60=format.format(i);
  result[i].strfield61=format.format(i);
  result[i].strfield62=format.format(i);
  result[i].strfield63=format.format(i);
  result[i].strfield64=format.format(i);
  result[i].strfield65=format.format(i);
  result[i].strfield66=format.format(i);
  result[i].strfield67=format.format(i);
  result[i].strfield68=format.format(i);
  result[i].strfield69=format.format(i);
  result[i].strfield70=format.format(i);
  result[i].strfield71=format.format(i);
  result[i].strfield72=format.format(i);
  result[i].strfield73=format.format(i);
  result[i].strfield74=format.format(i);
  result[i].strfield75=format.format(i);
  result[i].strfield76=format.format(i);
  result[i].strfield77=format.format(i);
  result[i].strfield78=format.format(i);
  result[i].strfield79=format.format(i);
  result[i].strfield80=format.format(i);
  result[i].strfield81=format.format(i);
  result[i].strfield82=format.format(i);
  result[i].strfield83=format.format(i);
  result[i].strfield84=format.format(i);
  result[i].strfield85=format.format(i);
  result[i].strfield86=format.format(i);
  result[i].strfield87=format.format(i);
  result[i].strfield88=format.format(i);
  result[i].strfield89=format.format(i);
  result[i].strfield90=format.format(i);
  result[i].strfield91=format.format(i);
  result[i].strfield92=format.format(i);
  result[i].strfield93=format.format(i);
  result[i].strfield94=format.format(i);
  result[i].strfield95=format.format(i);
  result[i].strfield96=format.format(i);
  result[i].strfield97=format.format(i);
  result[i].strfield98=format.format(i);
  result[i].strfield99=format.format(i);
  result[i].strfield100=format.format(i);
  result[i].strfield101=format.format(i);
  result[i].strfield102=format.format(i);
  result[i].strfield103=format.format(i);
  result[i].strfield104=format.format(i);
  result[i].strfield105=format.format(i);
  result[i].strfield106=format.format(i);
  result[i].strfield107=format.format(i);
  result[i].strfield108=format.format(i);
  result[i].strfield109=format.format(i);
  result[i].strfield110=format.format(i);
  result[i].strfield111=format.format(i);
  result[i].strfield112=format.format(i);
  result[i].strfield113=format.format(i);
  result[i].strfield114=format.format(i);
  result[i].strfield115=format.format(i);
  result[i].strfield116=format.format(i);
  result[i].strfield117=format.format(i);
  result[i].strfield118=format.format(i);
  result[i].strfield119=format.format(i);
  result[i].strfield120=format.format(i);
  result[i].strfield121=format.format(i);
  result[i].strfield122=format.format(i);
  result[i].strfield123=format.format(i);
  result[i].strfield124=format.format(i);
  result[i].strfield125=format.format(i);
  result[i].strfield126=format.format(i);
  result[i].strfield127=format.format(i);
  result[i].strfield128=format.format(i);
  result[i].strfield129=format.format(i);
  result[i].strfield130=format.format(i);
  result[i].strfield131=format.format(i);
  result[i].strfield132=format.format(i);
  result[i].strfield133=format.format(i);
  result[i].strfield134=format.format(i);
  result[i].strfield135=format.format(i);
  result[i].strfield136=format.format(i);
  result[i].strfield137=format.format(i);
  result[i].strfield138=format.format(i);
  result[i].strfield139=format.format(i);
  result[i].strfield140=format.format(i);
  result[i].strfield141=format.format(i);
  result[i].strfield142=format.format(i);
  result[i].strfield143=format.format(i);
  result[i].strfield144=format.format(i);
  result[i].strfield145=format.format(i);
  result[i].strfield146=format.format(i);
  result[i].strfield147=format.format(i);
  result[i].strfield148=format.format(i);
  result[i].strfield149=format.format(i);
  result[i].strfield150=format.format(i);
  result[i].strfield151=format.format(i);
  result[i].strfield152=format.format(i);
  result[i].strfield153=format.format(i);
  result[i].strfield154=format.format(i);
  result[i].strfield155=format.format(i);
  result[i].strfield156=format.format(i);
  result[i].strfield157=format.format(i);
  result[i].strfield158=format.format(i);
  result[i].strfield159=format.format(i);
  result[i].strfield160=format.format(i);
  result[i].strfield161=format.format(i);
  result[i].strfield162=format.format(i);
  result[i].strfield163=format.format(i);
  result[i].strfield164=format.format(i);
  result[i].strfield165=format.format(i);
  result[i].strfield166=format.format(i);
  result[i].strfield167=format.format(i);
  result[i].strfield168=format.format(i);
  result[i].strfield169=format.format(i);
  result[i].strfield170=format.format(i);
  result[i].strfield171=format.format(i);
  result[i].strfield172=format.format(i);
  result[i].strfield173=format.format(i);
  result[i].strfield174=format.format(i);
  result[i].strfield175=format.format(i);
  result[i].strfield176=format.format(i);
  result[i].strfield177=format.format(i);
  result[i].strfield178=format.format(i);
  result[i].strfield179=format.format(i);
  result[i].id=i;
  result[i].intfield1=i;
  result[i].intfield2=i;
  result[i].intfield3=i;
  result[i].intfield4=i;
  result[i].intfield5=i;
  result[i].intfield6=i;
  result[i].intfield7=i;
  result[i].intfield8=i;
  result[i].intfield9=i;
  result[i].intfield10=i;
  result[i].intfield11=i;
  result[i].intfield12=i;
  result[i].intfield13=i;
  result[i].intfield14=i;
  result[i].intfield15=i;
  result[i].intfield16=i;
  result[i].intfield17=i;
  result[i].intfield18=i;
  result[i].intfield19=i;
  result[i].intfield20=i;
  result[i].intfield21=i;
  result[i].intfield22=i;
  result[i].intfield23=i;
  result[i].intfield24=i;
  result[i].intfield25=i;
  result[i].intfield26=i;
  result[i].intfield27=i;
  result[i].intfield28=i;
  result[i].intfield29=i;
  result[i].intfield30=i;
  result[i].intfield31=i;
  result[i].intfield32=i;
  result[i].intfield33=i;
  result[i].intfield34=i;
  result[i].intfield35=i;
  result[i].intfield36=i;
  result[i].intfield37=i;
  result[i].intfield38=i;
  result[i].intfield39=i;
  result[i].intfield40=i;
  result[i].intfield41=i;
  result[i].intfield42=i;
  result[i].intfield43=i;
  result[i].intfield44=i;
  result[i].intfield45=i;
  result[i].intfield46=i;
  result[i].intfield47=i;
  result[i].intfield48=i;
  result[i].intfield49=i;
  result[i].intfield50=i;
  result[i].intfield51=i;
  result[i].intfield52=i;
  result[i].intfield53=i;
  result[i].intfield54=i;
  result[i].intfield55=i;
  result[i].intfield56=i;
  result[i].intfield57=i;
  result[i].intfield58=i;
  result[i].intfield59=i;
  result[i].intfield60=i;
  result[i].intfield61=i;
  result[i].intfield62=i;
  result[i].intfield63=i;
  result[i].intfield64=i;
  result[i].intfield65=i;
  result[i].intfield66=i;
  result[i].intfield67=i;
  result[i].intfield68=i;
  result[i].intfield69=i;
  result[i].intfield70=i;
  result[i].intfield71=i;
  result[i].intfield72=i;
  result[i].intfield73=i;
  result[i].intfield74=i;
  result[i].intfield75=i;
  result[i].intfield76=i;
  result[i].intfield77=i;
  result[i].intfield78=i;
  result[i].intfield79=i;
  result[i].intfield80=i;
  result[i].intfield81=i;
  result[i].intfield82=i;
  result[i].intfield83=i;
  result[i].intfield84=i;
  result[i].intfield85=i;
  result[i].intfield86=i;
  result[i].intfield87=i;
  result[i].intfield88=i;
  result[i].intfield89=i;
  result[i].intfield90=i;
  result[i].intfield91=i;
  result[i].intfield92=i;
  result[i].intfield93=i;
  result[i].intfield94=i;
  result[i].intfield95=i;
  result[i].intfield96=i;
  result[i].intfield97=i;
  result[i].intfield98=i;
  result[i].intfield99=i;
  result[i].intfield100=i;
  result[i].intfield101=i;
  result[i].intfield102=i;
  result[i].intfield103=i;
  result[i].intfield104=i;
  result[i].intfield105=i;
  result[i].intfield106=i;
  result[i].intfield107=i;
  result[i].intfield108=i;
  result[i].intfield109=i;
  result[i].intfield110=i;
  result[i].intfield111=i;
  result[i].intfield112=i;
  result[i].intfield113=i;
  result[i].intfield114=i;
  result[i].intfield115=i;
  result[i].intfield116=i;
  result[i].intfield117=i;
  result[i].intfield118=i;
  result[i].intfield119=i;
  result[i].intfield120=i;
  result[i].intfield121=i;
  result[i].intfield122=i;
  result[i].intfield123=i;
  result[i].intfield124=i;
  result[i].intfield125=i;
  result[i].intfield126=i;
  result[i].intfield127=i;
  result[i].intfield128=i;
  result[i].intfield129=i;
  result[i].intfield130=i;
  result[i].intfield131=i;
  result[i].intfield132=i;
  result[i].intfield133=i;
  result[i].intfield134=i;
  result[i].intfield135=i;
  result[i].intfield136=i;
  result[i].intfield137=i;
  result[i].intfield138=i;
  result[i].intfield139=i;
  result[i].intfield140=i;
  result[i].intfield141=i;
  result[i].intfield142=i;
  result[i].intfield143=i;
  result[i].intfield144=i;
  result[i].intfield145=i;
  result[i].intfield146=i;
  result[i].intfield147=i;
  result[i].intfield148=i;
  result[i].intfield149=i;
  result[i].intfield150=i;
  result[i].intfield151=i;
  result[i].intfield152=i;
  result[i].intfield153=i;
  result[i].intfield154=i;
  result[i].intfield155=i;
  result[i].intfield156=i;
  result[i].intfield157=i;
  result[i].intfield158=i;
  result[i].intfield159=i;
  result[i].intfield160=i;
  result[i].intfield161=i;
  result[i].intfield162=i;
  result[i].intfield163=i;
  result[i].intfield164=i;
  result[i].intfield165=i;
  result[i].intfield166=i;
  result[i].intfield167=i;
  result[i].intfield168=i;
  result[i].intfield169=i;
  result[i].intfield170=i;
  result[i].intfield171=i;
  result[i].intfield172=i;
  result[i].intfield173=i;
  result[i].intfield174=i;
  result[i].intfield175=i;
  result[i].intfield176=i;
  result[i].doublefield0=i*1.01;
  result[i].doublefield1=i*1.01;
  result[i].doublefield2=i*1.01;
  result[i].doublefield3=i*1.01;
  result[i].doublefield4=i*1.01;
  result[i].doublefield5=i*1.01;
  result[i].doublefield6=i*1.01;
  result[i].doublefield7=i*1.01;
  result[i].doublefield8=i*1.01;
  result[i].doublefield9=i*1.01;
  result[i].doublefield10=i*1.01;
  result[i].doublefield11=i*1.01;
  result[i].doublefield12=i*1.01;
  result[i].doublefield13=i*1.01;
  result[i].doublefield14=i*1.01;
  result[i].doublefield15=i*1.01;
  result[i].doublefield16=i*1.01;
  result[i].doublefield17=i*1.01;
  result[i].doublefield18=i*1.01;
  result[i].doublefield19=i*1.01;
  result[i].doublefield20=i*1.01;
  result[i].doublefield21=i*1.01;
  result[i].doublefield22=i*1.01;
  result[i].doublefield23=i*1.01;
  result[i].doublefield24=i*1.01;
  result[i].doublefield25=i*1.01;
  result[i].doublefield26=i*1.01;
  result[i].doublefield27=i*1.01;
  result[i].doublefield28=i*1.01;
  result[i].doublefield29=i*1.01;
  result[i].doublefield30=i*1.01;
  result[i].doublefield31=i*1.01;
  result[i].doublefield32=i*1.01;
  result[i].doublefield33=i*1.01;
  result[i].doublefield34=i*1.01;
  result[i].doublefield35=i*1.01;
  result[i].doublefield36=i*1.01;
  result[i].doublefield37=i*1.01;
  result[i].doublefield38=i*1.01;
  result[i].doublefield39=i*1.01;
  result[i].doublefield40=i*1.01;
  result[i].doublefield41=i*1.01;
  result[i].doublefield42=i*1.01;
  result[i].doublefield43=i*1.01;
  result[i].doublefield44=i*1.01;
  result[i].doublefield45=i*1.01;
  result[i].doublefield46=i*1.01;
  result[i].doublefield47=i*1.01;
  result[i].doublefield48=i*1.01;
  result[i].doublefield49=i*1.01;
  result[i].doublefield50=i*1.01;
  result[i].doublefield51=i*1.01;
  result[i].doublefield52=i*1.01;
  result[i].doublefield53=i*1.01;
  result[i].doublefield54=i*1.01;
  result[i].doublefield55=i*1.01;
  result[i].doublefield56=i*1.01;
  result[i].doublefield57=i*1.01;
  result[i].doublefield58=i*1.01;
  result[i].doublefield59=i*1.01;
  result[i].doublefield60=i*1.01;
  result[i].doublefield61=i*1.01;
  result[i].doublefield62=i*1.01;
  result[i].doublefield63=i*1.01;
  result[i].doublefield64=i*1.01;
  result[i].doublefield65=i*1.01;
  result[i].doublefield66=i*1.01;
  result[i].doublefield67=i*1.01;
  result[i].doublefield68=i*1.01;
  result[i].doublefield69=i*1.01;
  result[i].doublefield70=i*1.01;
  result[i].doublefield71=i*1.01;
  result[i].doublefield72=i*1.01;
  result[i].doublefield73=i*1.01;
  result[i].doublefield74=i*1.01;
  result[i].doublefield75=i*1.01;
  result[i].doublefield76=i*1.01;
  result[i].doublefield77=i*1.01;
  result[i].doublefield78=i*1.01;
  result[i].doublefield79=i*1.01;
  result[i].doublefield80=i*1.01;
  result[i].doublefield81=i*1.01;
  result[i].doublefield82=i*1.01;
  result[i].doublefield83=i*1.01;
  result[i].doublefield84=i*1.01;
  result[i].doublefield85=i*1.01;
  result[i].doublefield86=i*1.01;
  result[i].doublefield87=i*1.01;
  result[i].doublefield88=i*1.01;
  result[i].doublefield89=i*1.01;
  result[i].doublefield90=i*1.01;
  result[i].doublefield91=i*1.01;
  result[i].doublefield92=i*1.01;
  result[i].doublefield93=i*1.01;
  result[i].doublefield94=i*1.01;
  result[i].doublefield95=i*1.01;
  result[i].doublefield96=i*1.01;
  result[i].doublefield97=i*1.01;
  result[i].doublefield98=i*1.01;
  result[i].doublefield99=i*1.01;
  result[i].doublefield100=i*1.01;
  result[i].doublefield101=i*1.01;
  result[i].doublefield102=i*1.01;
  result[i].doublefield103=i*1.01;
  result[i].doublefield104=i*1.01;
  result[i].doublefield105=i*1.01;
  result[i].doublefield106=i*1.01;
  result[i].doublefield107=i*1.01;
  result[i].doublefield108=i*1.01;
  result[i].doublefield109=i*1.01;
  result[i].doublefield110=i*1.01;
  result[i].doublefield111=i*1.01;
  result[i].doublefield112=i*1.01;
  result[i].doublefield113=i*1.01;
  result[i].doublefield114=i*1.01;
  result[i].doublefield115=i*1.01;
  result[i].doublefield116=i*1.01;
  result[i].doublefield117=i*1.01;
  result[i].doublefield118=i*1.01;
  result[i].doublefield119=i*1.01;
  result[i].doublefield120=i*1.01;
  result[i].doublefield121=i*1.01;
  result[i].doublefield122=i*1.01;
  result[i].doublefield123=i*1.01;
  result[i].doublefield124=i*1.01;
  result[i].doublefield125=i*1.01;
  result[i].doublefield126=i*1.01;
  result[i].doublefield127=i*1.01;
  result[i].doublefield128=i*1.01;
  result[i].doublefield129=i*1.01;
  result[i].doublefield130=i*1.01;
  result[i].doublefield131=i*1.01;
  result[i].doublefield132=i*1.01;
  result[i].doublefield133=i*1.01;
  result[i].doublefield134=i*1.01;
  result[i].doublefield135=i*1.01;
  result[i].doublefield136=i*1.01;
  result[i].doublefield137=i*1.01;
  result[i].doublefield138=i*1.01;
  result[i].doublefield139=i*1.01;
  result[i].doublefield140=i*1.01;
  result[i].doublefield141=i*1.01;
  result[i].doublefield142=i*1.01;
  result[i].doublefield143=i*1.01;
  result[i].doublefield144=i*1.01;
  result[i].doublefield145=i*1.01;
  result[i].doublefield146=i*1.01;
  result[i].doublefield147=i*1.01;
  result[i].doublefield148=i*1.01;
  result[i].doublefield149=i*1.01;
  result[i].doublefield150=i*1.01;
  result[i].doublefield151=i*1.01;
  result[i].doublefield152=i*1.01;
  result[i].doublefield153=i*1.01;
  result[i].doublefield154=i*1.01;
  result[i].doublefield155=i*1.01;
  result[i].doublefield156=i*1.01;
  result[i].doublefield157=i*1.01;
  result[i].doublefield158=i*1.01;
  result[i].doublefield159=i*1.01;
  result[i].bytefield0= 'a';
  result[i].bytefield1= 'a';
  result[i].bytefield2= 'a';
  result[i].bytefield3= 'a';
  result[i].bytefield4= 'a';
  result[i].bytefield5= 'a';
  result[i].bytefield6= 'a';
  result[i].bytefield7= 'a';
  result[i].bytefield8= 'a';
  result[i].bytefield9= 'a';
  result[i].bytefield10= 'a';
  result[i].bytefield11= 'a';
  result[i].bytefield12= 'a';
  result[i].bytefield13= 'a';
  result[i].bytefield14= 'a';
  result[i].bytefield15= 'a';
  result[i].bytefield16= 'a';
  result[i].bytefield17= 'a';
  result[i].bytefield18= 'a';
  result[i].bytefield19= 'a';
  result[i].bytefield20= 'a';
  result[i].bytefield21= 'a';
  result[i].bytefield22= 'a';
  result[i].bytefield23= 'a';
  result[i].bytefield24= 'a';
  result[i].bytefield25= 'a';
  result[i].bytefield26= 'a';
  result[i].bytefield27= 'a';
  result[i].bytefield28= 'a';
  result[i].bytefield29= 'a';
  result[i].bytefield30= 'a';
  result[i].bytefield31= 'a';
  result[i].bytefield32= 'a';
  result[i].bytefield33= 'a';
  result[i].bytefield34= 'a';
  result[i].bytefield35= 'a';
  result[i].bytefield36= 'a';
  result[i].bytefield37= 'a';
  result[i].bytefield38= 'a';
  result[i].bytefield39= 'a';
  result[i].bytefield40= 'a';
  result[i].bytefield41= 'a';
  result[i].bytefield42= 'a';
  result[i].bytefield43= 'a';
  result[i].bytefield44= 'a';
  result[i].bytefield45= 'a';
  result[i].bytefield46= 'a';
  result[i].bytefield47= 'a';
  result[i].bytefield48= 'a';
  result[i].bytefield49= 'a';
  result[i].bytefield50= 'a';
  result[i].bytefield51= 'a';
  result[i].bytefield52= 'a';
  result[i].bytefield53= 'a';
  result[i].bytefield54= 'a';
  result[i].bytefield55= 'a';
  result[i].bytefield56= 'a';
  result[i].bytefield57= 'a';
  result[i].bytefield58= 'a';
  result[i].bytefield59= 'a';
  result[i].bytefield60= 'a';
  result[i].bytefield61= 'a';
  result[i].bytefield62= 'a';
  result[i].bytefield63= 'a';
  result[i].bytefield64= 'a';
  result[i].bytefield65= 'a';
  result[i].bytefield66= 'a';
  result[i].bytefield67= 'a';
  result[i].bytefield68= 'a';
  result[i].bytefield69= 'a';
  result[i].bytefield70= 'a';
  result[i].bytefield71= 'a';
  result[i].bytefield72= 'a';
  result[i].bytefield73= 'a';
  result[i].bytefield74= 'a';
  result[i].bytefield75= 'a';
  result[i].bytefield76= 'a';
  result[i].bytefield77= 'a';
  result[i].bytefield78= 'a';
  result[i].bytefield79= 'a';
  result[i].bytefield80= 'a';
  result[i].bytefield81= 'a';
  result[i].bytefield82= 'a';
  result[i].bytefield83= 'a';
  result[i].bytefield84= 'a';
  result[i].bytefield85= 'a';
  result[i].bytefield86= 'a';
  result[i].bytefield87= 'a';
  result[i].bytefield88= 'a';
  result[i].bytefield89= 'a';
  result[i].bytefield90= 'a';
  result[i].bytefield91= 'a';
  result[i].bytefield92= 'a';
  result[i].bytefield93= 'a';
  result[i].bytefield94= 'a';
  result[i].bytefield95= 'a';
  result[i].bytefield96= 'a';
  result[i].bytefield97= 'a';
  result[i].bytefield98= 'a';
  result[i].bytefield99= 'a';
  result[i].bytefield100= 'a';
  result[i].bytefield101= 'a';
  result[i].bytefield102= 'a';
  result[i].bytefield103= 'a';
  result[i].bytefield104= 'a';
  result[i].bytefield105= 'a';
  result[i].bytefield106= 'a';
  result[i].bytefield107= 'a';
  result[i].bytefield108= 'a';
  result[i].bytefield109= 'a';
  result[i].bytefield110= 'a';
  result[i].bytefield111= 'a';
  result[i].bytefield112= 'a';
  result[i].bytefield113= 'a';
  result[i].bytefield114= 'a';
  result[i].bytefield115= 'a';
  result[i].bytefield116= 'a';
  result[i].bytefield117= 'a';
  result[i].bytefield118= 'a';
  result[i].bytefield119= 'a';
  result[i].bytefield120= 'a';
  result[i].bytefield121= 'a';
  result[i].bytefield122= 'a';
  result[i].bytefield123= 'a';
  result[i].bytefield124= 'a';
  result[i].bytefield125= 'a';
  result[i].bytefield126= 'a';
  result[i].bytefield127= 'a';
  result[i].bytefield128= 'a';
  result[i].bytefield129= 'a';
  result[i].bytefield130= 'a';
  result[i].bytefield131= 'a';
  result[i].bytefield132= 'a';
  result[i].bytefield133= 'a';
  result[i].bytefield134= 'a';
  result[i].bytefield135= 'a';
  result[i].bytefield136= 'a';
  result[i].bytefield137= 'a';
  result[i].bytefield138= 'a';
  result[i].bytefield139= 'a';
  result[i].bytefield140= 'a';
  result[i].bytefield141= 'a';
  result[i].bytefield142= 'a';
  result[i].bytefield143= 'a';
  result[i].bytefield144= 'a';
  result[i].bytefield145= 'a';
  result[i].bytefield146= 'a';
  result[i].bytefield147= 'a';
  result[i].bytefield148= 'a';
  result[i].bytefield149= 'a';
  result[i].charfield0= 'b';
  result[i].charfield1= 'b';
  result[i].charfield2= 'b';
  result[i].charfield3= 'b';
  result[i].charfield4= 'b';
  result[i].charfield5= 'b';
  result[i].charfield6= 'b';
  result[i].charfield7= 'b';
  result[i].charfield8= 'b';
  result[i].charfield9= 'b';
  result[i].charfield10= 'b';
  result[i].charfield11= 'b';
  result[i].charfield12= 'b';
  result[i].charfield13= 'b';
  result[i].charfield14= 'b';
  result[i].charfield15= 'b';
  result[i].charfield16= 'b';
  result[i].charfield17= 'b';
  result[i].charfield18= 'b';
  result[i].charfield19= 'b';
  result[i].charfield20= 'b';
  result[i].charfield21= 'b';
  result[i].charfield22= 'b';
  result[i].charfield23= 'b';
  result[i].charfield24= 'b';
  result[i].charfield25= 'b';
  result[i].charfield26= 'b';
  result[i].charfield27= 'b';
  result[i].charfield28= 'b';
  result[i].charfield29= 'b';
  result[i].charfield30= 'b';
  result[i].charfield31= 'b';
  result[i].charfield32= 'b';
  result[i].charfield33= 'b';
  result[i].charfield34= 'b';
  result[i].charfield35= 'b';
  result[i].charfield36= 'b';
  result[i].charfield37= 'b';
  result[i].charfield38= 'b';
  result[i].charfield39= 'b';
  result[i].charfield40= 'b';
  result[i].charfield41= 'b';
  result[i].charfield42= 'b';
  result[i].charfield43= 'b';
  result[i].charfield44= 'b';
  result[i].charfield45= 'b';
  result[i].charfield46= 'b';
  result[i].charfield47= 'b';
  result[i].charfield48= 'b';
  result[i].charfield49= 'b';
  result[i].charfield50= 'b';
  result[i].charfield51= 'b';
  result[i].charfield52= 'b';
  result[i].charfield53= 'b';
  result[i].charfield54= 'b';
  result[i].charfield55= 'b';
  result[i].charfield56= 'b';
  result[i].charfield57= 'b';
  result[i].charfield58= 'b';
  result[i].charfield59= 'b';
  result[i].charfield60= 'b';
  result[i].charfield61= 'b';
  result[i].charfield62= 'b';
  result[i].charfield63= 'b';
  result[i].charfield64= 'b';
  result[i].charfield65= 'b';
  result[i].charfield66= 'b';
  result[i].charfield67= 'b';
  result[i].charfield68= 'b';
  result[i].charfield69= 'b';
  result[i].charfield70= 'b';
  result[i].charfield71= 'b';
  result[i].charfield72= 'b';
  result[i].charfield73= 'b';
  result[i].charfield74= 'b';
  result[i].charfield75= 'b';
  result[i].charfield76= 'b';
  result[i].charfield77= 'b';
  result[i].charfield78= 'b';
  result[i].charfield79= 'b';
  result[i].charfield80= 'b';
  result[i].charfield81= 'b';
  result[i].charfield82= 'b';
  result[i].charfield83= 'b';
  result[i].charfield84= 'b';
  result[i].charfield85= 'b';
  result[i].charfield86= 'b';
  result[i].charfield87= 'b';
  result[i].charfield88= 'b';
  result[i].charfield89= 'b';
  result[i].charfield90= 'b';
  result[i].charfield91= 'b';
  result[i].charfield92= 'b';
  result[i].charfield93= 'b';
  result[i].charfield94= 'b';
  result[i].charfield95= 'b';
  result[i].charfield96= 'b';
  result[i].charfield97= 'b';
  result[i].charfield98= 'b';
  result[i].charfield99= 'b';
  result[i].charfield100= 'b';
  result[i].charfield101= 'b';
  result[i].charfield102= 'b';
  result[i].charfield103= 'b';
  result[i].charfield104= 'b';
  result[i].charfield105= 'b';
  result[i].charfield106= 'b';
  result[i].charfield107= 'b';
  result[i].charfield108= 'b';
  result[i].charfield109= 'b';
  result[i].charfield110= 'b';
  result[i].charfield111= 'b';
  result[i].charfield112= 'b';
  result[i].charfield113= 'b';
  result[i].charfield114= 'b';
  result[i].charfield115= 'b';
  result[i].charfield116= 'b';
  result[i].charfield117= 'b';
  result[i].charfield118= 'b';
  result[i].charfield119= 'b';
  result[i].charfield120= 'b';
  result[i].charfield121= 'b';
  result[i].charfield122= 'b';
  result[i].charfield123= 'b';
  result[i].charfield124= 'b';
  result[i].charfield125= 'b';
  result[i].charfield126= 'b';
  result[i].charfield127= 'b';
  result[i].charfield128= 'b';
  result[i].charfield129= 'b';
  result[i].charfield130= 'b';
  result[i].charfield131= 'b';
  result[i].charfield132= 'b';
  result[i].charfield133= 'b';
  result[i].charfield134= 'b';
  result[i].charfield135= 'b';
  result[i].charfield136= 'b';
  result[i].charfield137= 'b';
  result[i].charfield138= 'b';
  result[i].charfield139= 'b';
  result[i].charfield140= 'b';
  result[i].charfield141= 'b';
  result[i].charfield142= 'b';
  result[i].charfield143= 'b';
  result[i].charfield144= 'b';
  result[i].charfield145= 'b';
  result[i].charfield146= 'b';
  result[i].charfield147= 'b';
  result[i].charfield148= 'b';
  result[i].charfield149= 'b';
  result[i].booleanfield0= false;
  result[i].booleanfield1= false;
  result[i].booleanfield2= false;
  result[i].booleanfield3= false;
  result[i].booleanfield4= false;
  result[i].booleanfield5= false;
  result[i].booleanfield6= false;
  result[i].booleanfield7= false;
  result[i].booleanfield8= false;
  result[i].booleanfield9= false;
  result[i].booleanfield10= false;
  result[i].booleanfield11= false;
  result[i].booleanfield12= false;
  result[i].booleanfield13= false;
  result[i].booleanfield14= false;
  result[i].booleanfield15= false;
  result[i].booleanfield16= false;
  result[i].booleanfield17= false;
  result[i].booleanfield18= false;
  result[i].booleanfield19= false;
  result[i].booleanfield20= false;
  result[i].booleanfield21= false;
  result[i].booleanfield22= false;
  result[i].booleanfield23= false;
  result[i].booleanfield24= false;
  result[i].booleanfield25= false;
  result[i].booleanfield26= false;
  result[i].booleanfield27= false;
  result[i].booleanfield28= false;
  result[i].booleanfield29= false;
  result[i].booleanfield30= false;
  result[i].booleanfield31= false;
  result[i].booleanfield32= false;
  result[i].booleanfield33= false;
  result[i].booleanfield34= false;
  result[i].booleanfield35= false;
  result[i].booleanfield36= false;
  result[i].booleanfield37= false;
  result[i].booleanfield38= false;
  result[i].booleanfield39= false;
  result[i].booleanfield40= false;
  result[i].booleanfield41= false;
  result[i].booleanfield42= false;
  result[i].booleanfield43= false;
  result[i].booleanfield44= false;
  result[i].booleanfield45= false;
  result[i].booleanfield46= false;
  result[i].booleanfield47= false;
  result[i].booleanfield48= false;
  result[i].booleanfield49= false;
  result[i].booleanfield50= false;
  result[i].booleanfield51= false;
  result[i].booleanfield52= false;
  result[i].booleanfield53= false;
  result[i].booleanfield54= false;
  result[i].booleanfield55= false;
  result[i].booleanfield56= false;
  result[i].booleanfield57= false;
  result[i].booleanfield58= false;
  result[i].booleanfield59= false;
  result[i].booleanfield60= false;
  result[i].booleanfield61= false;
  result[i].booleanfield62= false;
  result[i].booleanfield63= false;
  result[i].booleanfield64= false;
  result[i].booleanfield65= false;
  result[i].booleanfield66= false;
  result[i].booleanfield67= false;
  result[i].booleanfield68= false;
  result[i].booleanfield69= false;
  result[i].booleanfield70= false;
  result[i].booleanfield71= false;
  result[i].booleanfield72= false;
  result[i].booleanfield73= false;
  result[i].booleanfield74= false;
  result[i].booleanfield75= false;
  result[i].booleanfield76= false;
  result[i].booleanfield77= false;
  result[i].booleanfield78= false;
  result[i].booleanfield79= false;
  result[i].booleanfield80= false;
  result[i].booleanfield81= false;
  result[i].booleanfield82= false;
  result[i].booleanfield83= false;
  result[i].booleanfield84= false;
  result[i].booleanfield85= false;
  result[i].booleanfield86= false;
  result[i].booleanfield87= false;
  result[i].booleanfield88= false;
  result[i].booleanfield89= false;
  result[i].booleanfield90= false;
  result[i].booleanfield91= false;
  result[i].booleanfield92= false;
  result[i].booleanfield93= false;
  result[i].booleanfield94= false;
  result[i].booleanfield95= false;
  result[i].booleanfield96= false;
  result[i].booleanfield97= false;
  result[i].booleanfield98= false;
  result[i].booleanfield99= false;
  result[i].booleanfield100= false;
  result[i].booleanfield101= false;
  result[i].booleanfield102= false;
  result[i].booleanfield103= false;
  result[i].booleanfield104= false;
  result[i].booleanfield105= false;
  result[i].booleanfield106= false;
  result[i].booleanfield107= false;
  result[i].booleanfield108= false;
  result[i].booleanfield109= false;
  result[i].booleanfield110= false;
  result[i].booleanfield111= false;
  result[i].booleanfield112= false;
  result[i].booleanfield113= false;
  result[i].booleanfield114= false;
  result[i].booleanfield115= false;
  result[i].booleanfield116= false;
  result[i].booleanfield117= false;
  result[i].booleanfield118= false;
  result[i].booleanfield119= false;
  result[i].booleanfield120= false;
  result[i].booleanfield121= false;
  result[i].booleanfield122= false;
  result[i].booleanfield123= false;
  result[i].booleanfield124= false;
  result[i].booleanfield125= false;
  result[i].booleanfield126= false;
  result[i].booleanfield127= false;
  result[i].booleanfield128= false;
  result[i].booleanfield129= false;
  result[i].booleanfield130= false;
  result[i].booleanfield131= false;
  result[i].booleanfield132= false;
  result[i].booleanfield133= false;
  result[i].booleanfield134= false;
  result[i].booleanfield135= false;
  result[i].booleanfield136= false;
  result[i].booleanfield137= false;
  result[i].booleanfield138= false;
  result[i].booleanfield139= false;
  result[i].booleanfield140= false;
  result[i].booleanfield141= false;
  result[i].booleanfield142= false;
  result[i].booleanfield143= false;
  result[i].booleanfield144= false;
  result[i].booleanfield145= false;
  result[i].booleanfield146= false;
  result[i].booleanfield147= false;
  result[i].booleanfield148= false;
  result[i].booleanfield149= false;
  result[i].floatfield0= 1.101f;
  result[i].floatfield1= 1.101f;
  result[i].floatfield2= 1.101f;
  result[i].floatfield3= 1.101f;
  result[i].floatfield4= 1.101f;
  result[i].floatfield5= 1.101f;
  result[i].floatfield6= 1.101f;
  result[i].floatfield7= 1.101f;
  result[i].floatfield8= 1.101f;
  result[i].floatfield9= 1.101f;
  result[i].floatfield10= 1.101f;
  result[i].floatfield11= 1.101f;
  result[i].floatfield12= 1.101f;
  result[i].floatfield13= 1.101f;
  result[i].floatfield14= 1.101f;
  result[i].floatfield15= 1.101f;
  result[i].floatfield16= 1.101f;
  result[i].floatfield17= 1.101f;
  result[i].floatfield18= 1.101f;
  result[i].floatfield19= 1.101f;
  result[i].floatfield20= 1.101f;
  result[i].floatfield21= 1.101f;
  result[i].floatfield22= 1.101f;
  result[i].floatfield23= 1.101f;
  result[i].floatfield24= 1.101f;
  result[i].floatfield25= 1.101f;
  result[i].floatfield26= 1.101f;
  result[i].floatfield27= 1.101f;
  result[i].floatfield28= 1.101f;
  result[i].floatfield29= 1.101f;
  result[i].floatfield30= 1.101f;
  result[i].floatfield31= 1.101f;
  result[i].floatfield32= 1.101f;
  result[i].floatfield33= 1.101f;
  result[i].floatfield34= 1.101f;
  result[i].floatfield35= 1.101f;
  result[i].floatfield36= 1.101f;
  result[i].floatfield37= 1.101f;
  result[i].floatfield38= 1.101f;
  result[i].floatfield39= 1.101f;
  result[i].floatfield40= 1.101f;
  result[i].floatfield41= 1.101f;
  result[i].floatfield42= 1.101f;
  result[i].floatfield43= 1.101f;
  result[i].floatfield44= 1.101f;
  result[i].floatfield45= 1.101f;
  result[i].floatfield46= 1.101f;
  result[i].floatfield47= 1.101f;
  result[i].floatfield48= 1.101f;
  result[i].floatfield49= 1.101f;
  result[i].floatfield50= 1.101f;
  result[i].floatfield51= 1.101f;
  result[i].floatfield52= 1.101f;
  result[i].floatfield53= 1.101f;
  result[i].floatfield54= 1.101f;
  result[i].floatfield55= 1.101f;
  result[i].floatfield56= 1.101f;
  result[i].floatfield57= 1.101f;
  result[i].floatfield58= 1.101f;
  result[i].floatfield59= 1.101f;
  result[i].floatfield60= 1.101f;
  result[i].floatfield61= 1.101f;
  result[i].floatfield62= 1.101f;
  result[i].floatfield63= 1.101f;
  result[i].floatfield64= 1.101f;
  result[i].floatfield65= 1.101f;
  result[i].floatfield66= 1.101f;
  result[i].floatfield67= 1.101f;
  result[i].floatfield68= 1.101f;
  result[i].floatfield69= 1.101f;
  result[i].floatfield70= 1.101f;
  result[i].floatfield71= 1.101f;
  result[i].floatfield72= 1.101f;
  result[i].floatfield73= 1.101f;
  result[i].floatfield74= 1.101f;
  result[i].floatfield75= 1.101f;
  result[i].floatfield76= 1.101f;
  result[i].floatfield77= 1.101f;
  result[i].floatfield78= 1.101f;
  result[i].floatfield79= 1.101f;
  result[i].floatfield80= 1.101f;
  result[i].floatfield81= 1.101f;
  result[i].floatfield82= 1.101f;
  result[i].floatfield83= 1.101f;
  result[i].floatfield84= 1.101f;
  result[i].floatfield85= 1.101f;
  result[i].floatfield86= 1.101f;
  result[i].floatfield87= 1.101f;
  result[i].floatfield88= 1.101f;
  result[i].floatfield89= 1.101f;
  result[i].floatfield90= 1.101f;
  result[i].floatfield91= 1.101f;
  result[i].floatfield92= 1.101f;
  result[i].floatfield93= 1.101f;
  result[i].floatfield94= 1.101f;
  result[i].floatfield95= 1.101f;
  result[i].floatfield96= 1.101f;
  result[i].floatfield97= 1.101f;
  result[i].floatfield98= 1.101f;
  result[i].floatfield99= 1.101f;
  result[i].floatfield100= 1.101f;
  result[i].floatfield101= 1.101f;
  result[i].floatfield102= 1.101f;
  result[i].floatfield103= 1.101f;
  result[i].floatfield104= 1.101f;
  result[i].floatfield105= 1.101f;
  result[i].floatfield106= 1.101f;
  result[i].floatfield107= 1.101f;
  result[i].floatfield108= 1.101f;
  result[i].floatfield109= 1.101f;
  result[i].floatfield110= 1.101f;
  result[i].floatfield111= 1.101f;
  result[i].floatfield112= 1.101f;
  result[i].floatfield113= 1.101f;
  result[i].floatfield114= 1.101f;
  result[i].floatfield115= 1.101f;
  result[i].floatfield116= 1.101f;
  result[i].floatfield117= 1.101f;
  result[i].floatfield118= 1.101f;
  result[i].floatfield119= 1.101f;
  result[i].floatfield120= 1.101f;
  result[i].floatfield121= 1.101f;
  result[i].floatfield122= 1.101f;
  result[i].floatfield123= 1.101f;
  result[i].floatfield124= 1.101f;
  result[i].floatfield125= 1.101f;
  result[i].floatfield126= 1.101f;
  result[i].floatfield127= 1.101f;
  result[i].floatfield128= 1.101f;
  result[i].floatfield129= 1.101f;
  result[i].floatfield130= 1.101f;
  result[i].floatfield131= 1.101f;
  result[i].floatfield132= 1.101f;
  result[i].floatfield133= 1.101f;
  result[i].floatfield134= 1.101f;
  result[i].floatfield135= 1.101f;
  result[i].floatfield136= 1.101f;
  result[i].floatfield137= 1.101f;
  result[i].floatfield138= 1.101f;
  result[i].floatfield139= 1.101f;
  result[i].floatfield140= 1.101f;
  result[i].floatfield141= 1.101f;
  result[i].floatfield142= 1.101f;
  result[i].floatfield143= 1.101f;
  result[i].floatfield144= 1.101f;
  result[i].floatfield145= 1.101f;
  result[i].floatfield146= 1.101f;
  result[i].floatfield147= 1.101f;
  result[i].floatfield148= 1.101f;
  result[i].floatfield149= 1.101f;
  result[i].longfield0= 123465677l;
  result[i].longfield1= 123465677l;
  result[i].longfield2= 123465677l;
  result[i].longfield3= 123465677l;
  result[i].longfield4= 123465677l;
  result[i].longfield5= 123465677l;
  result[i].longfield6= 123465677l;
  result[i].longfield7= 123465677l;
  result[i].longfield8= 123465677l;
  result[i].longfield9= 123465677l;
  result[i].longfield10= 123465677l;
  result[i].longfield11= 123465677l;
  result[i].longfield12= 123465677l;
  result[i].longfield13= 123465677l;
  result[i].longfield14= 123465677l;
  result[i].longfield15= 123465677l;
  result[i].longfield16= 123465677l;
  result[i].longfield17= 123465677l;
  result[i].longfield18= 123465677l;
  result[i].longfield19= 123465677l;
  result[i].longfield20= 123465677l;
  result[i].longfield21= 123465677l;
  result[i].longfield22= 123465677l;
  result[i].longfield23= 123465677l;
  result[i].longfield24= 123465677l;
  result[i].longfield25= 123465677l;
  result[i].longfield26= 123465677l;
  result[i].longfield27= 123465677l;
  result[i].longfield28= 123465677l;
  result[i].longfield29= 123465677l;
  result[i].longfield30= 123465677l;
  result[i].longfield31= 123465677l;
  result[i].longfield32= 123465677l;
  result[i].longfield33= 123465677l;
  result[i].longfield34= 123465677l;
  result[i].longfield35= 123465677l;
  result[i].longfield36= 123465677l;
  result[i].longfield37= 123465677l;
  result[i].longfield38= 123465677l;
  result[i].longfield39= 123465677l;
  result[i].longfield40= 123465677l;
  result[i].longfield41= 123465677l;
  result[i].longfield42= 123465677l;
  result[i].longfield43= 123465677l;
  result[i].longfield44= 123465677l;
  result[i].longfield45= 123465677l;
  result[i].longfield46= 123465677l;
  result[i].longfield47= 123465677l;
  result[i].longfield48= 123465677l;
  result[i].longfield49= 123465677l;
  result[i].longfield50= 123465677l;
  result[i].longfield51= 123465677l;
  result[i].longfield52= 123465677l;
  result[i].longfield53= 123465677l;
  result[i].longfield54= 123465677l;
  result[i].longfield55= 123465677l;
  result[i].longfield56= 123465677l;
  result[i].longfield57= 123465677l;
  result[i].longfield58= 123465677l;
  result[i].longfield59= 123465677l;
  result[i].longfield60= 123465677l;
  result[i].longfield61= 123465677l;
  result[i].longfield62= 123465677l;
  result[i].longfield63= 123465677l;
  result[i].longfield64= 123465677l;
  result[i].longfield65= 123465677l;
  result[i].longfield66= 123465677l;
  result[i].longfield67= 123465677l;
  result[i].longfield68= 123465677l;
  result[i].longfield69= 123465677l;
  result[i].longfield70= 123465677l;
  result[i].longfield71= 123465677l;
  result[i].longfield72= 123465677l;
  result[i].longfield73= 123465677l;
  result[i].longfield74= 123465677l;
  result[i].longfield75= 123465677l;
  result[i].longfield76= 123465677l;
  result[i].longfield77= 123465677l;
  result[i].longfield78= 123465677l;
  result[i].longfield79= 123465677l;
  result[i].longfield80= 123465677l;
  result[i].longfield81= 123465677l;
  result[i].longfield82= 123465677l;
  result[i].longfield83= 123465677l;
  result[i].longfield84= 123465677l;
  result[i].longfield85= 123465677l;
  result[i].longfield86= 123465677l;
  result[i].longfield87= 123465677l;
  result[i].longfield88= 123465677l;
  result[i].longfield89= 123465677l;
  result[i].longfield90= 123465677l;
  result[i].longfield91= 123465677l;
  result[i].longfield92= 123465677l;
  result[i].longfield93= 123465677l;
  result[i].longfield94= 123465677l;
  result[i].longfield95= 123465677l;
  result[i].longfield96= 123465677l;
  result[i].longfield97= 123465677l;
  result[i].longfield98= 123465677l;
  result[i].longfield99= 123465677l;
  result[i].longfield100= 123465677l;
  result[i].longfield101= 123465677l;
  result[i].longfield102= 123465677l;
  result[i].longfield103= 123465677l;
  result[i].longfield104= 123465677l;
  result[i].longfield105= 123465677l;
  result[i].longfield106= 123465677l;
  result[i].longfield107= 123465677l;
  result[i].longfield108= 123465677l;
  result[i].longfield109= 123465677l;
  result[i].longfield110= 123465677l;
  result[i].longfield111= 123465677l;
  result[i].longfield112= 123465677l;
  result[i].longfield113= 123465677l;
  result[i].longfield114= 123465677l;
  result[i].longfield115= 123465677l;
  result[i].longfield116= 123465677l;
  result[i].longfield117= 123465677l;
  result[i].longfield118= 123465677l;
  result[i].longfield119= 123465677l;
  result[i].longfield120= 123465677l;
  result[i].longfield121= 123465677l;
  result[i].longfield122= 123465677l;
  result[i].longfield123= 123465677l;
  result[i].longfield124= 123465677l;
  result[i].longfield125= 123465677l;
  result[i].longfield126= 123465677l;
  result[i].longfield127= 123465677l;
  result[i].longfield128= 123465677l;
  result[i].longfield129= 123465677l;
  result[i].longfield130= 123465677l;
  result[i].longfield131= 123465677l;
  result[i].longfield132= 123465677l;
  result[i].longfield133= 123465677l;
  result[i].longfield134= 123465677l;
  result[i].longfield135= 123465677l;
  result[i].longfield136= 123465677l;
  result[i].longfield137= 123465677l;
  result[i].longfield138= 123465677l;
  result[i].longfield139= 123465677l;
  result[i].longfield140= 123465677l;
  result[i].longfield141= 123465677l;
  result[i].longfield142= 123465677l;
  result[i].longfield143= 123465677l;
  result[i].longfield144= 123465677l;
  result[i].longfield145= 123465677l;
  result[i].longfield146= 123465677l;
  result[i].longfield147= 123465677l;
  result[i].longfield148= 123465677l;
  result[i].longfield149= 123465677l;
  result[i].shortfield0= 12;
  result[i].shortfield1= 12;
  result[i].shortfield2= 12;
  result[i].shortfield3= 12;
  result[i].shortfield4= 12;
  result[i].shortfield5= 12;
  result[i].shortfield6= 12;
  result[i].shortfield7= 12;
  result[i].shortfield8= 12;
  result[i].shortfield9= 12;
  result[i].shortfield10= 12;
  result[i].shortfield11= 12;
  result[i].shortfield12= 12;
  result[i].shortfield13= 12;
  result[i].shortfield14= 12;
  result[i].shortfield15= 12;
  result[i].shortfield16= 12;
  result[i].shortfield17= 12;
  result[i].shortfield18= 12;
  result[i].shortfield19= 12;
  result[i].shortfield20= 12;
  result[i].shortfield21= 12;
  result[i].shortfield22= 12;
  result[i].shortfield23= 12;
  result[i].shortfield24= 12;
  result[i].shortfield25= 12;
  result[i].shortfield26= 12;
  result[i].shortfield27= 12;
  result[i].shortfield28= 12;
  result[i].shortfield29= 12;
  result[i].shortfield30= 12;
  result[i].shortfield31= 12;
  result[i].shortfield32= 12;
  result[i].shortfield33= 12;
  result[i].shortfield34= 12;
  result[i].shortfield35= 12;
  result[i].shortfield36= 12;
  result[i].shortfield37= 12;
  result[i].shortfield38= 12;
  result[i].shortfield39= 12;
  result[i].shortfield40= 12;
  result[i].shortfield41= 12;
  result[i].shortfield42= 12;
  result[i].shortfield43= 12;
  result[i].shortfield44= 12;
  result[i].shortfield45= 12;
  result[i].shortfield46= 12;
  result[i].shortfield47= 12;
  result[i].shortfield48= 12;
  result[i].shortfield49= 12;
  result[i].shortfield50= 12;
  result[i].shortfield51= 12;
  result[i].shortfield52= 12;
  result[i].shortfield53= 12;
  result[i].shortfield54= 12;
  result[i].shortfield55= 12;
  result[i].shortfield56= 12;
  result[i].shortfield57= 12;
  result[i].shortfield58= 12;
  result[i].shortfield59= 12;
  result[i].shortfield60= 12;
  result[i].shortfield61= 12;
  result[i].shortfield62= 12;
  result[i].shortfield63= 12;
  result[i].shortfield64= 12;
  result[i].shortfield65= 12;
  result[i].shortfield66= 12;
  result[i].shortfield67= 12;
  result[i].shortfield68= 12;
  result[i].shortfield69= 12;
  result[i].shortfield70= 12;
  result[i].shortfield71= 12;
  result[i].shortfield72= 12;
  result[i].shortfield73= 12;
  result[i].shortfield74= 12;
  result[i].shortfield75= 12;
  result[i].shortfield76= 12;
  result[i].shortfield77= 12;
  result[i].shortfield78= 12;
  result[i].shortfield79= 12;
  result[i].shortfield80= 12;
  result[i].shortfield81= 12;
  result[i].shortfield82= 12;
  result[i].shortfield83= 12;
  result[i].shortfield84= 12;
  result[i].shortfield85= 12;
  result[i].shortfield86= 12;
  result[i].shortfield87= 12;
  result[i].shortfield88= 12;
  result[i].shortfield89= 12;
  result[i].shortfield90= 12;
  result[i].shortfield91= 12;
  result[i].shortfield92= 12;
  result[i].shortfield93= 12;
  result[i].shortfield94= 12;
  result[i].shortfield95= 12;
  result[i].shortfield96= 12;
  result[i].shortfield97= 12;
  result[i].shortfield98= 12;
  result[i].shortfield99= 12;
  result[i].shortfield100= 12;
  result[i].shortfield101= 12;
  result[i].shortfield102= 12;
  result[i].shortfield103= 12;
  result[i].shortfield104= 12;
  result[i].shortfield105= 12;
  result[i].shortfield106= 12;
  result[i].shortfield107= 12;
  result[i].shortfield108= 12;
  result[i].shortfield109= 12;
  result[i].shortfield110= 12;
  result[i].shortfield111= 12;
  result[i].shortfield112= 12;
  result[i].shortfield113= 12;
  result[i].shortfield114= 12;
  result[i].shortfield115= 12;
  result[i].shortfield116= 12;
  result[i].shortfield117= 12;
  result[i].shortfield118= 12;
  result[i].shortfield119= 12;
  result[i].shortfield120= 12;
  result[i].shortfield121= 12;
  result[i].shortfield122= 12;
  result[i].shortfield123= 12;
  result[i].shortfield124= 12;
  result[i].shortfield125= 12;
  result[i].shortfield126= 12;
  result[i].shortfield127= 12;
  result[i].shortfield128= 12;
  result[i].shortfield129= 12;
  result[i].shortfield130= 12;
  result[i].shortfield131= 12;
  result[i].shortfield132= 12;
  result[i].shortfield133= 12;
  result[i].shortfield134= 12;
  result[i].shortfield135= 12;
  result[i].shortfield136= 12;
  result[i].shortfield137= 12;
  result[i].shortfield138= 12;
  result[i].shortfield139= 12;
  result[i].shortfield140= 12;
  result[i].shortfield141= 12;
  result[i].shortfield142= 12;
  result[i].shortfield143= 12;
  result[i].shortfield144= 12;
  result[i].shortfield145= 12;
  result[i].shortfield146= 12;
  result[i].shortfield147= 12;
  result[i].shortfield148= 12;
  result[i].shortfield149= 12;
  }
  return result;
  }
  public int getId() {
    return id;
  }
  public void setId(int id) {
    this.id = id;
  }

  public FlatObject() {
  }
  public void init(int index) {
    throw new UnsupportedOperationException("No init here");
  }
  public int getIndex() {
    return this.id;
  }
  public void validate( int index ) {
    throw new UnsupportedOperationException("No validation here");
  }
}
