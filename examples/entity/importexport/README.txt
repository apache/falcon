####### Using MySQL as Datasource #######
yum install mysql mysql-server mysql-libs
/usr/share/java/mysql-connector-java.jar
cp /home/ambari-qa/falcon_ingestion/backup/mysql-connector-java-5.1.31.jar /usr/share/java/
unlink /usr/share/java/mysql-connector-java.jar
ln -s /home/ambari-qa/falcon_ingestion/backup/mysql-connector-java-5.1.31.jar /usr/share/java/mysql-connector-java.jar
service mysqld start

####### oozie sharelib change for sqoop ######################
# Copy /usr/share/java/mysql-connector-java.jar (version 5.1.31) into oozie share lib of sqoop dir
hadoop dfs -put  /usr/share/java/mysql-connector-java-5.1.31.jar /user/oozie/share/lib/lib_XXXXXXXXXXX/sqoop

####### Setup MySQL user and create source table ##############
# Setup - Create MySQL source table - db_raw_data
###############################################################
use test;
create user sqoop_user@localhost identified by 'sqoop';
grant all privileges on *.* to 'sqoop_user'@'localhost' with grant option;
create user 'sqoop_user'@'%' identified by 'sqoop';
grant all privileges on *.* to 'sqoop_user'@'%' with grant option;
flush privileges;

## Import: following is the data source tables from which data will be pulled into HADOOP

create table db_raw_data(id int not null, name varchar(20), value int, modified_ts timestamp);
insert into db_raw_data values (1,  'Apple', 700, now() );
insert into db_raw_data values (2,  'Blackberry', 1, now() );
insert into db_raw_data values (3,  'Cisco', 100, now() );
insert into db_raw_data values (4,  'Delta', 10, now() );
insert into db_raw_data values (5,  'Eagle', 55, now() );
insert into db_raw_data values (6,  'Falcon', 99, now() );

## submit the following falcon entities and schedule

falcon entity -type cluster -file primary_cluster.xml -submit
falcon entity -type datasource -file mysql-datasource.xml -submit
falcon entity -type feed -file mysql-feed-import-filesystem.xml -submitAndSchedule

## Export: create a target table in the MySQL database 

create table db_export_fs(id int not null, name varchar(20), value int, modified_ts timestamp);

## submit the following falcon entities and schedule 
falcon entity -type feed -file mysql-feed-export-filesystem.xml -submitAndSchedule

### Note
# For other supported database types like Oracle, Postgres, Teradata, Netezza, DB2, Generic JDB
# please use the corresponding feed entity that starts with the database name in the xml entity
# definition. 

# Also, please make sure to copy the jdbc driver onto the Oozie Sqoop shared lib on HDFS and 
# restart Oozie. The datasource entity definition will also refer to the JDBC drivers in order
# for falcon to validate the connection when submitting the datasource entity.



