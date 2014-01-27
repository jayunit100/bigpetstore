BigPetStore with maven profiles for dependency management
==========================================================

Build Instructions

  - Install pig jar see Pig instructions
  -  mvn clean package
  - Run Intergration tests with
  - Pig profile clean verify -P pig
  - Hive profile clean verify -P hive

> Install the Pig Jar

pig 0.12.0 from mvn central does not work out of the box

so do the following  
1) get the pig jar  
wget http://apache.claz.org/pig/pig-0.12.0/pig-0.12.0.tar.gz  
2) set up for pig build
sudo tar -xzf pig-0.12.0.tar.gz
sudo chmod -R 477 pig-0.12.0

3) in the unpacked pig directory do the build as
ant clean jar -Dhadoopversion=23

4) in the unpacked pig directory install the jar with  

mvn install:install-file -Dfile=pig.jar -DgroupId=org.bigpetstore.pigmodule -DartifactId=bigpetstore -Dversion=1.0 -Dpackaging=jar

'dependency'  
'groupId org.bigpetstore.pigmodule /groupId'  
'artifactId bigpetstore /artifactId'  
'version 1.0 /version'  
'/dependency'

    