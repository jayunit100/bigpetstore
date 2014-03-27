### THIS SCRIPT SETS UP HIVE AND HADOOP TARBALLS FOR YOU ###
mydir=`pwd`
mkdir -p /opt/bigpetstore
cd /opt/bigpetstore
wget  https://archive.apache.org/dist/hadoop/core/hadoop-1.2.1/hadoop-1.2.1.tar.gz
tar -xvf hadoop-1.2.1.tar.gz
export HADOOP_HOME=`pwd`/hadoop-1.2.1
wget  http://apache.cbox.biz/hive/hive-0.12.0.tar.gz  
tar -xvf hive-0.12.0.tar.gz  
cp /opt/hive-0.12.0/lib/hive-contrib-0.12.0.jar /HADOOP_HOME/lib  
cp /opt/hive-0.12.0/lib/hive-serde-0.12.0.jar /HADOOP_HOME/lib
cd $mydir
