#!/bin/bash
#This script assumes WIX, VERSION, BITS, and OSBUILDDIR are set
OSBUILDDIR_CYG=`cygpath $OSBUILDDIR`

numParts=`echo ${VERSION} | sed 's/\./ /g' | wc -w`
VERSION4=${VERSION}
while [ ${numParts} -lt 4 ]; do
  VERSION4=${VERSION4}.0
  numParts=`expr ${numParts} + 1`
done

export PATH=`cygpath $WIX`/bin:$PATH
VERSION4_NODOT=`echo "$VERSION4" | sed 's/\.//g'`
NAME=Pivotal_GemFire_XD_ODBC_Driver_${BITS}
FULL_NAME=Pivotal_GemFire_XD_ODBC_Driver_${BITS}_${VERSION4_NODOT}
MSI_NAME=${NAME}_${VERSION4_NODOT}_b${BNUMBER}

if [ "x${BITS}" = "x64bit" ]; then
  MERGEMODULE_Id="Microsoft_VC100_CRT_x64"
  MERGEMODULE_SourceFile="${PROGRAMFILES}\Common Files\Merge Modules\Microsoft_VC100_CRT_x64.msm"
  is64=1  
  PLATFORM="Platforms=\"x64\""  
else  
  MERGEMODULE_Id="Microsoft_VC100_CRT_x86"
  MERGEMODULE_SourceFile="${PROGRAMFILES}\Common Files\Merge Modules\Microsoft_VC100_CRT_x86.msm"
  is64=0    
  PLATFORM=
fi

cp -r --preserve=all $OSBUILDDIR_CYG/artifacts $OSBUILDDIR_CYG/$FULL_NAME
mkdir -p $OSBUILDDIR_CYG/installer

chmod a+rwx -R "$OSBUILDDIR/$FULL_NAME"
chmod a+rwx -R "$OSBUILDDIR_CYG/installer"
echo "VERSION4_NODOT = $VERSION4_NODOT"

#The perl code gives each "component" a unique GID by incrementing
#the last 5 digits. Without a unique ID the uninstaller is broken
tallow.exe -nologo -d $OSBUILDDIR/$FULL_NAME > $OSBUILDDIR_CYG/installer/fragment.temp
cat $OSBUILDDIR_CYG/installer/fragment.temp | \
perl -ne 'chomp; $GID = sprintf("%.5d", $GIDNUM++); \
s/<Component Id="component(\d+)" DiskId="(\d+)" Guid="PUT-GUID-HERE">/<Component Id="component\1" DiskId="\2" Guid="12345678-'$VERSION4_NODOT'-1234-1234-1234567$GID">/;\
s/TARGETDIR/DEFAULT_INSTALLDIR/;\
print "$_\n"' > $OSBUILDDIR_CYG/installer/fragment.wxs

packageid="6E3F15E${is64}-${VERSION4_NODOT}-1234-1234-12345678ABCD"
cat<<__HEADER__ > $OSBUILDDIR_CYG/installer/installer_gen.wxs
<?xml version="1.0" encoding="UTF-8"?>
<Wix xmlns="http://schemas.microsoft.com/wix/2003/01/wi">
  <Product Id="$packageid" Name="Pivotal GemFire XD ODBC Driver $BITS $VERSION" Language="1033" Version="$VERSION4" Manufacturer="Pivotal Inc">
    <Package Id="????????-????-????-????-????????????" Description="Pivotal GemFire XD ODBC Driver" Comments="Installer for Pivotal GemFire XD ODBC Driver." InstallerVersion="300" Compressed="yes" $PLATFORM/>
    <Property Id="WixUI_InstallDir" Value="INSTALLLOCATION"/>
    <Condition Message="You need to be an administrator to install this product.
">
      Privileged
    </Condition>
    <Condition Message='This product is intended to only run on Windows 2000 or later'>
      VersionNT >= 500
    </Condition>

    <!--
    <Condition Message='This setup requires the .NET Framework 2.0 or higher.'>
      <![CDATA[MsiNetAssemblySupport >= "2.0.50727"]]>
    </Condition>
    --> 

    <Media Id="1" Cabinet="Product.cab" EmbedCab="yes" />
    <!-- Variables -->
    <Property Id="DEFAULT_INSTALLDIR"><![CDATA[C:\]]></Property>
    <Property Id="WIXUI_INSTALLDIR" Value="DEFAULT_INSTALLDIR"/>

    <!-- GUI code goes here -->
    <FragmentRef Id="Installer.UI" />
    <!-- end of GUI code-->

    <Directory Id="TARGETDIR" Name="SourceDir">
      <Directory Id="ProgramFilesFolder">
        <Directory Id="DEFAULT_INSTALLDIR" Name=".">
        <Merge Id="$MERGEMODULE_Id" Language="1033" SourceFile="$MERGEMODULE_SourceFile" DiskId="1"/>
        <Component Id="environment0" DiskId="1" Guid="12345678-$VERSION4_NODOT-1234-1234-EEEEEEEEEEEE">
          <CreateFolder/>		  
        </Component>
        </Directory>
      </Directory>
    </Directory>
	
    <Feature Id="OdbcDriverBaseFeature" Title="ODBC Driver" Level="1" ConfigurableDirectory="DEFAULT_INSTALLDIR">
          <ComponentRef Id="environment0" />
__HEADER__
#<Component Id="component1004" DiskId="1" Guid="PUT-GUID-HERE">
grep '<Component' $OSBUILDDIR_CYG/installer/fragment.wxs | sed \
-e 's/ *<Component /\t<ComponentRef /' \
-e 's/DiskId.*/\/>/g' >> $OSBUILDDIR_CYG/installer/installer_gen.wxs
cat<<__FOOTER__ >> $OSBUILDDIR_CYG/installer/installer_gen.wxs
        <MergeRef Id="$MERGEMODULE_Id" />
    </Feature>
	
   <Binary Id="CustomActionOdbcDriver.CA.dll"
	 src="../CustomActionOdbcDriver.CA.dll" />
 
   <CustomAction Id="InstallWithProperty"
      Property="Install"
      Value="location=[DEFAULT_INSTALLDIR]$FULL_NAME;name=gemfirexdodbcSetup.dll"
	  Execute="immediate"/>
		
   <CustomAction Id="Install"
      Execute="deferred"
      BinaryKey="CustomActionOdbcDriver.CA.dll"
      DllEntry="Install" />

    <CustomAction Id="UnInstall"
       Execute="immediate"
       BinaryKey="CustomActionOdbcDriver.CA.dll"
       DllEntry="UnInstall" />

	<InstallExecuteSequence>
    <Custom Action="InstallWithProperty" After="CostFinalize" />
    <Custom Action="Install" Before="InstallFinalize">
      NOT Installed or REINSTALL
    </Custom>
    <Custom Action="UnInstall" Before="InstallFinalize">
      Installed AND NOT UPGRADINGPRODUCTCODE
    </Custom>
    </InstallExecuteSequence>

	</Product>
</Wix>
__FOOTER__
candle.exe -nologo $OSBUILDDIR/installer/fragment.wxs -out $OSBUILDDIR/installer/fragment.wixobj
candle.exe -nologo $OSBUILDDIR/installer/installer_gen.wxs -out $OSBUILDDIR/installer/installer_gen.wixobj
candle.exe -nologo gemfirexd/odbc/GemFireXDODBCSetupInstaller/ui.wxs -out $OSBUILDDIR/installer/ui.wixobj
light.exe -nologo $OSBUILDDIR/installer/installer_gen.wixobj $OSBUILDDIR/installer/fragment.wixobj $OSBUILDDIR/installer/ui.wixobj -o $OSBUILDDIR/installer/${MSI_NAME}.msi

#clean up extra copy to reduce snapshot
rm -rf $OSBUILDDIR_CYG/$FULL_NAME
rm -rf $OSBUILDDIR_CYG/artifacts
rm -f $OSBUILDDIR_CYG/CustomActionOdbcDriver*.dll