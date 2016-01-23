
# Apache Falcon

Falcon is a feed processing and feed management system aimed at making it
easier for end consumers to onboard their feed processing and feed
management on hadoop clusters.

## Why Apache Falcon?

* Dependencies across various data processing pipelines are not easy to
  establish. Gaps here typically leads to either incorrect/partial
  processing or expensive reprocessing. Repeated duplicate definition of
  a single feed multiple times can lead to inconsistencies / issues.

* Input data may not arrive always on time and it is required to kick off
  the processing without waiting for all data to arrive and accommodate
  late data separately

* Feed management services such as feed retention, replications across
  clusters, archival etc are tasks that are burdensome on individual
  pipeline owners and better offered as a service for all customers.

* It should be easy to onboard new workflows/pipelines

* Smoother integration with metastore/catalog

* Provide notification to end customer based on availability of feed
  groups (logical group of related feeds, which are likely to be used
  together)

## Usage

a. Setup cluster definition
   $FALCON_HOME/bin/falcon entity -submit -type cluster -file /cluster/definition.xml -url http://falcon-server:falcon-port

b. Setup feed definition
   $FALCON_HOME/bin/falcon entity -submit -type feed -file /feed1/definition.xml -url http://falcon-server:falcon-port
   $FALCON_HOME/bin/falcon entity -submit -type feed -file /feed2/definition.xml -url http://falcon-server:falcon-port

c. Setup process definition
   $FALCON_HOME/bin/falcon entity -submit -type process -file /process/definition.xml -url http://falcon-server:falcon-port

d. Once submitted, entity definition, status and dependency can be queried.
   $FALCON_HOME/bin/falcon entity -type [cluster|feed|process] -name <<name>> [-definition|-status|-dependency] -url http://falcon-server:falcon-port

   or entities for a particular type can be listed through
   $FALCON_HOME/bin/falcon entity -type [cluster|feed|process] -list

e. Schedule process
   $FALCON_HOME/bin/falcon entity  -type process -name process -schedule -url http://falcon-server:falcon-port

f. Once scheduled entities can be suspended, resumed or deleted (post submit)
   $FALCON_HOME/bin/falcon entity  -type [cluster|feed|process] -name <<name>> [-suspend|-delete|-resume] -url http://falcon-server:falcon-port

g. Once scheduled process instances can be managed through irovy CLI
   $FALCON_HOME/bin/falcon instance -processName <<name>> [-kill|-suspend|-resume|-re-run] -start "yyyy-MM-dd'T'HH:mm'Z'" -url http://falcon-server:falcon-port

## Example configurations

### Cluster:
<?xml version="1.0" encoding="UTF-8"?>
<cluster colo="local" description="" name="local" xmlns="uri:falcon:cluster:0.1">
    <interfaces>
        <interface type="readonly" endpoint="hftp://localhost:41110"
                   version="0.20.2"/>
        <interface type="write" endpoint="hdfs://localhost:41020"
                   version="0.20.2"/>
        <interface type="execute" endpoint="localhost:41021" version="0.20.2"/>
        <interface type="workflow" endpoint="http://localhost:41000/oozie/"
                   version="4.0"/>
        <interface type="messaging" endpoint="tcp://localhost:61616?daemon=true"
                   version="5.1.6"/>
        <interface type="registry" endpoint="Hcat" version="1"/>
    </interfaces>
    <locations>
        <location name="staging" path="/projects/falcon/staging"/>
        <location name="temp" path="/tmp"/>
        <location name="working" path="/project/falcon/working"/>
    </locations>
</cluster>

### Feed:
<?xml version="1.0" encoding="UTF-8"?>
<feed description="in" name="in" xmlns="uri:ivory:feed:0.1">
    <partitions>
        <partition name="type"/>
    </partitions>
    <groups>in</groups>

    <frequency>hours(1)</frequency>
    <timezone>UTC</timezone>
    <late-arrival cut-off="hours(6)"/>

    <clusters>
        <cluster name="local">
            <validity start="2013-01-01T00:00Z" end="2020-01-01T12:00Z"/>
            <retention limit="hours(24)" action="delete"/>
        </cluster>
    </clusters>

    <locations>
        <location type="data" path="/data/in/${YEAR}/${MONTH}/${DAY}/${HOUR}"/>
    </locations>

    <ACL owner="testuser" group="group" permission="0x644"/>
    <schema location="/schema/in/in.format.csv" provider="csv"/>
</feed>

### Process:
<?xml version="1.0" encoding="UTF-8"?>
<process name="wf-process" xmlns="uri:falcon:process:0.1">
    <clusters>
        <cluster name="local">
            <validity start="2013-01-01T00:00Z" end="2013-01-01T02:00Z"/>
        </cluster>
    </clusters>

    <parallel>1</parallel>
    <order>LIFO</order>
    <frequency>hours(1)</frequency>
    <timezone>UTC</timezone>

    <inputs>
        <input name="input" feed="in" start="now(0,0)" end="now(0,0)"/>
    </inputs>

    <outputs>
        <output name="output" feed="out" instance="today(0,0)"/>
    </outputs>

    <workflow engine="oozie" path="/app/mapred-wf.xml"/>
</process>
