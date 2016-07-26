# Apache Falcon Changelog

## Release 0.10 - 2016-07-26

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [FALCON-1926](https://issues.apache.org/jira/browse/FALCON-1926) | Filter out effectively non-falcon related JMS messages from Oozie |  Major | messaging | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1858](https://issues.apache.org/jira/browse/FALCON-1858) | Support HBase as a storage backend for Falcon Titan graphDB |  Major | . | Ying Zheng | Venkat Ranganathan |
| [FALCON-1852](https://issues.apache.org/jira/browse/FALCON-1852) | Optional Input for a process not truly optional |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-1844](https://issues.apache.org/jira/browse/FALCON-1844) | Falcon feed replication leaves behind old files when a feed instance is re-run |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-1835](https://issues.apache.org/jira/browse/FALCON-1835) | Falcon should do coord rerun rather than workflow rerun to ensure concurrency |  Major | . | Pallavi Rao | Pallavi Rao |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [FALCON-1956](https://issues.apache.org/jira/browse/FALCON-1956) | Graphite Plugin for monitoring |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1919](https://issues.apache.org/jira/browse/FALCON-1919) | Provide user the option to store sensitive information with Hadoop credential provider |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1865](https://issues.apache.org/jira/browse/FALCON-1865) | Persist Feed sla data to database |  Major | . | Ajay Yadava | Praveen Adlakha |
| [FALCON-1861](https://issues.apache.org/jira/browse/FALCON-1861) | Support HDFS Snapshot based replication in Falcon |  Major | replication | Balu Vellanki | Balu Vellanki |
| [FALCON-1763](https://issues.apache.org/jira/browse/FALCON-1763) | Create a spark execution engine for Falcon |  Major | . | Venkat Ranganathan | Peeyush Bishnoi |
| [FALCON-1627](https://issues.apache.org/jira/browse/FALCON-1627) | Provider integration with Azure Data Factory pipelines |  Major | . | Venkat Ranganathan | Ying Zheng |
| [FALCON-1623](https://issues.apache.org/jira/browse/FALCON-1623) | Implement Safe Mode in Falcon |  Major | . | sandeep samudrala | Balu Vellanki |
| [FALCON-1333](https://issues.apache.org/jira/browse/FALCON-1333) | Support instance search of a group of entities |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-634](https://issues.apache.org/jira/browse/FALCON-634) | Add server side extensions in Falcon |  Major | . | Venkatesh Seetharam | Sowmya Ramesh |
| [FALCON-141](https://issues.apache.org/jira/browse/FALCON-141) | Support cluster updates |  Major | . | Shwetha G S | Balu Vellanki |
| [FALCON-36](https://issues.apache.org/jira/browse/FALCON-36) | Ability to ingest data from databases |  Major | acquisition | Venkatesh Seetharam | Venkatesan Ramachandran |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [FALCON-2020](https://issues.apache.org/jira/browse/FALCON-2020) | Changes in Graphite Notification Plugin |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1981](https://issues.apache.org/jira/browse/FALCON-1981) | Remove runtime superfluous jar dependencies - pom.xml cleanup |  Major | build-tools | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1979](https://issues.apache.org/jira/browse/FALCON-1979) | Update HttpClient versions to close security vulnerabilities |  Major | . | Balu Vellanki | Balu Vellanki |
| [FALCON-1963](https://issues.apache.org/jira/browse/FALCON-1963) | Falcon CLI should provide detailed hints if the user's command is invalid |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1942](https://issues.apache.org/jira/browse/FALCON-1942) | Allow Falcon server and client classpath to be customizable |  Major | . | Venkat Ranganathan | Venkat Ranganathan |
| [FALCON-1916](https://issues.apache.org/jira/browse/FALCON-1916) | Allow RM principal to be specified in Cluster entity |  Major | common | Venkat Ranganathan | Venkat Ranganathan |
| [FALCON-1895](https://issues.apache.org/jira/browse/FALCON-1895) | Refactoring of FalconCLI and FalconClient |  Major | client | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1841](https://issues.apache.org/jira/browse/FALCON-1841) | Grouping test in falcon for running nightly regression |  Major | regression | Pragya Mittal | Pragya Mittal |
| [FALCON-1836](https://issues.apache.org/jira/browse/FALCON-1836) | Ingest to Hive |  Major | . | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1802](https://issues.apache.org/jira/browse/FALCON-1802) | Workflow Builder for scheduling based on Data for Process in case of Native Scheduler |  Major | . | pavan kumar kolamuri | pavan kumar kolamuri |
| [FALCON-1774](https://issues.apache.org/jira/browse/FALCON-1774) | Better message for api not allowed on server |  Major | . | Sanjeev T | Praveen Adlakha |
| [FALCON-1751](https://issues.apache.org/jira/browse/FALCON-1751) | Support assembly:single mojo |  Minor | . | ruoyu wang | ruoyu wang |
| [FALCON-887](https://issues.apache.org/jira/browse/FALCON-887) | Support for multiple lib paths in falcon process |  Minor | process | Akshay Goyal | Sowmya Ramesh |
| [FALCON-625](https://issues.apache.org/jira/browse/FALCON-625) | Documentation improvements |  Major | . | Paul Isaychuk | Ajay Yadava |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [FALCON-2100](https://issues.apache.org/jira/browse/FALCON-2100) | Remove dependency on com.vividsolutions.jts |  Major | . | Balu Vellanki | Balu Vellanki |
| [FALCON-2090](https://issues.apache.org/jira/browse/FALCON-2090) | HDFS Snapshot failed with UnknownHostException when scheduling in HA Mode |  Critical | replication | Murali Ramasami | Balu Vellanki |
| [FALCON-2088](https://issues.apache.org/jira/browse/FALCON-2088) | Entity submission fails with EntityNotRegisteredException in distributed mode |  Blocker | feed, prism, process | Pragya Mittal | Praveen Adlakha |
| [FALCON-2084](https://issues.apache.org/jira/browse/FALCON-2084) | HCatReplicationTest are failing in secure mode |  Critical | replication | Murali Ramasami | Venkat Ranganathan |
| [FALCON-2081](https://issues.apache.org/jira/browse/FALCON-2081) | ExtensionManagerIT fails occassionally |  Blocker | tests | Balu Vellanki | Balu Vellanki |
| [FALCON-2076](https://issues.apache.org/jira/browse/FALCON-2076) | Server fails to start since extension.store.uri is not defined in startup.properties |  Major | prism | Pragya Mittal | Balu Vellanki |
| [FALCON-2075](https://issues.apache.org/jira/browse/FALCON-2075) | Falcon HiveDR tasks do not report progress and can get killed |  Critical | . | Venkat Ranganathan | Venkat Ranganathan |
| [FALCON-2071](https://issues.apache.org/jira/browse/FALCON-2071) | Falcon Spark SQL failing with Yarn Client Mode |  Critical | process | Murali Ramasami | Peeyush Bishnoi |
| [FALCON-2061](https://issues.apache.org/jira/browse/FALCON-2061) | Falcon CLI shows hadoop classpath loading info in the console |  Major | client | Murali Ramasami | Balu Vellanki |
| [FALCON-2060](https://issues.apache.org/jira/browse/FALCON-2060) | Retry does not happen if instance timedout |  Major | . | Pragya Mittal | Pallavi Rao |
| [FALCON-2058](https://issues.apache.org/jira/browse/FALCON-2058) | s3 tests with dummy url no longer compatible with latest HDFS |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-2057](https://issues.apache.org/jira/browse/FALCON-2057) | HiveDR not working with multiple users and same DB |  Major | replication | Murali Ramasami | Balu Vellanki |
| [FALCON-2056](https://issues.apache.org/jira/browse/FALCON-2056) | HiveDR doesn't work with multiple users |  Major | replication | Murali Ramasami | Sowmya Ramesh |
| [FALCON-2051](https://issues.apache.org/jira/browse/FALCON-2051) | Falcon post-processing services are not getting invoked |  Blocker | general | Peeyush Bishnoi | Venkatesan Ramachandran |
| [FALCON-2050](https://issues.apache.org/jira/browse/FALCON-2050) | Configure jetty parent classloader to be prioritized over webapp classloader |  Major | common | Venkat Ranganathan | Venkat Ranganathan |
| [FALCON-2049](https://issues.apache.org/jira/browse/FALCON-2049) | Feed Replication with Empty Directories are failing |  Blocker | feed | Murali Ramasami | Balu Vellanki |
| [FALCON-2048](https://issues.apache.org/jira/browse/FALCON-2048) | Cluster submission failed in yarn-cluster mode |  Critical | general | Murali Ramasami | Peeyush Bishnoi |
| [FALCON-2046](https://issues.apache.org/jira/browse/FALCON-2046) | HDFS Replication failing in secure Mode |  Critical | replication | Murali Ramasami | Sowmya Ramesh |
| [FALCON-2045](https://issues.apache.org/jira/browse/FALCON-2045) | Enhance document on registry point in cluster specifiction for Hive HA mode |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-2038](https://issues.apache.org/jira/browse/FALCON-2038) | When all Optional input instances are missing, we should not suffix partition |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-2037](https://issues.apache.org/jira/browse/FALCON-2037) | HiveDR Extension tests are failed in Secure mode with clusterForJobNNKerberosPrincipal not found |  Critical | replication | Murali Ramasami | Sowmya Ramesh |
| [FALCON-2036](https://issues.apache.org/jira/browse/FALCON-2036) | Update twiki on entity list operation with up-to-date REST API path |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-2035](https://issues.apache.org/jira/browse/FALCON-2035) | Entity list operation without type parameter doesn't work when authorization is enabled |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-2034](https://issues.apache.org/jira/browse/FALCON-2034) | Make numThreads and timeOut configurable In ConfigurationStore init |  Critical | . | Pallavi Rao | sandeep samudrala |
| [FALCON-2032](https://issues.apache.org/jira/browse/FALCON-2032) | Update the extension documentation to add ExtensionService before ConfigurationStore in startup properties |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-2031](https://issues.apache.org/jira/browse/FALCON-2031) | Hcat Retention test cases are failing with NoClassDefFoundError |  Blocker | retention | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-2027](https://issues.apache.org/jira/browse/FALCON-2027) | Enhance documentation on data replication from HDP to Azure |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-2025](https://issues.apache.org/jira/browse/FALCON-2025) | Periodic revalidation of kerberos credentials should be done on loginUser |  Major | . | Balu Vellanki | Balu Vellanki |
| [FALCON-2023](https://issues.apache.org/jira/browse/FALCON-2023) | Feed eviction fails when feed locations "stats" and "meta" does not have time pattern. |  Blocker | feed | Balu Vellanki | Venkatesan Ramachandran |
| [FALCON-2018](https://issues.apache.org/jira/browse/FALCON-2018) | WorkflowJobNotification sends incorrect message for killed instances |  Major | . | Pragya Mittal | Praveen Adlakha |
| [FALCON-2017](https://issues.apache.org/jira/browse/FALCON-2017) | Fix HiveDR extension issues |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-2016](https://issues.apache.org/jira/browse/FALCON-2016) | maven assembly:single fails on MacOS |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-2010](https://issues.apache.org/jira/browse/FALCON-2010) | Fix UT errors due to ActiveMQ upgrade |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-2007](https://issues.apache.org/jira/browse/FALCON-2007) | Hive DR Replication failing with "Can not create a Path from a null string" |  Critical | . | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-1984](https://issues.apache.org/jira/browse/FALCON-1984) | Provide proper hint and documentation if required titan storage backend is not configured in startup.properties |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1983](https://issues.apache.org/jira/browse/FALCON-1983) | Upgrade jackson core and databind versions to fix dependency incompatibility with higher-version Hive |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1982](https://issues.apache.org/jira/browse/FALCON-1982) | Document use of  HBase in standalone mode for GraphDB |  Major | docs | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1978](https://issues.apache.org/jira/browse/FALCON-1978) | Fix flaky unit test - MetadataMappingServiceTest |  Major | tests | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1976](https://issues.apache.org/jira/browse/FALCON-1976) | Remove hadoop-2 profile |  Major | . | Venkat Ranganathan | Venkat Ranganathan |
| [FALCON-1975](https://issues.apache.org/jira/browse/FALCON-1975) | Getting NoSuchMethodError when calling isNoneEmpty |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1974](https://issues.apache.org/jira/browse/FALCON-1974) | Cluster update : Allow superuser to update bundle/coord of dependent entities |  Major | . | Balu Vellanki | Balu Vellanki |
| [FALCON-1973](https://issues.apache.org/jira/browse/FALCON-1973) | Falcon build failure due checkstyle issue |  Major | . | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-1972](https://issues.apache.org/jira/browse/FALCON-1972) | Handling cases when Extension service or "extension.store.uri" is not present in startup proeprties |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1969](https://issues.apache.org/jira/browse/FALCON-1969) | Provide server-side error details on CLI, if any |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1965](https://issues.apache.org/jira/browse/FALCON-1965) | Update ActiveMQ version to 5.13.3 to avoid Falcon start error after rolling upgrade |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1964](https://issues.apache.org/jira/browse/FALCON-1964) | Should delete temporary JKS file after IT tests for credential provider alias |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1962](https://issues.apache.org/jira/browse/FALCON-1962) | Extension related bugs |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1961](https://issues.apache.org/jira/browse/FALCON-1961) | Should return error if an extension job doesn't exist for delete/suspend/resume/schedule operations |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1957](https://issues.apache.org/jira/browse/FALCON-1957) | Documentation on using Hadoop credential provider for sensitive properties |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1953](https://issues.apache.org/jira/browse/FALCON-1953) | Build fails when profiles hivedr and test-patch is used together |  Major | . | Balu Vellanki | Balu Vellanki |
| [FALCON-1943](https://issues.apache.org/jira/browse/FALCON-1943) | Extension API/CLI fails when authorization is enabled |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1941](https://issues.apache.org/jira/browse/FALCON-1941) | HiveDR fails with NN-HA enabled on both the source and target clusters |  Critical | . | Venkat Ranganathan | Venkat Ranganathan |
| [FALCON-1939](https://issues.apache.org/jira/browse/FALCON-1939) | Avoid creating multiple falcon\*.tar.gz during falcon build |  Major | build-tools | Balu Vellanki | Balu Vellanki |
| [FALCON-1936](https://issues.apache.org/jira/browse/FALCON-1936) | Extensions related files are not available in $FALCON\_HOM/extensions/ directory |  Critical | . | Peeyush Bishnoi | Sowmya Ramesh |
| [FALCON-1935](https://issues.apache.org/jira/browse/FALCON-1935) | Falcon fails to start with default startup.properties |  Blocker | . | Ying Zheng | Praveen Adlakha |
| [FALCON-1934](https://issues.apache.org/jira/browse/FALCON-1934) | Document safemode in Falcon Server |  Major | docs | Balu Vellanki | Balu Vellanki |
| [FALCON-1932](https://issues.apache.org/jira/browse/FALCON-1932) | Extension CLI should support common options |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1931](https://issues.apache.org/jira/browse/FALCON-1931) | multiCluster tag is missing for Multiple Cluster scenarios |  Major | regression | Murali Ramasami | Murali Ramasami |
| [FALCON-1928](https://issues.apache.org/jira/browse/FALCON-1928) | FalconJPAService missing in default startup.properties |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-1924](https://issues.apache.org/jira/browse/FALCON-1924) | Falcon Coordinator rerun return old workflow id |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1909](https://issues.apache.org/jira/browse/FALCON-1909) | Catalog instance triage action fails with null pointer exception. |  Major | feed | Balu Vellanki | Balu Vellanki |
| [FALCON-1908](https://issues.apache.org/jira/browse/FALCON-1908) | Document HDFS snapshot based mirroring extension |  Major | . | Balu Vellanki | Balu Vellanki |
| [FALCON-1907](https://issues.apache.org/jira/browse/FALCON-1907) | Package new CLI module added |  Major | client | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1896](https://issues.apache.org/jira/browse/FALCON-1896) | Failure in Falcon build in distro module |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1894](https://issues.apache.org/jira/browse/FALCON-1894) | HDFS Data replication cannot be initiated independent of Oozie server location |  Minor | general | Alex Bush | Sowmya Ramesh |
| [FALCON-1886](https://issues.apache.org/jira/browse/FALCON-1886) | Feed sla monitoring does not work across restarts |  Major | . | Ajay Yadava | Ajay Yadava |
| [FALCON-1885](https://issues.apache.org/jira/browse/FALCON-1885) | SLA monitoring API throws ResultNotFoundException |  Major | feed | Pragya Mittal | Praveen Adlakha |
| [FALCON-1883](https://issues.apache.org/jira/browse/FALCON-1883) | Falcon regression build fails with minor checkstyle issues |  Major | regression | Murali Ramasami | Murali Ramasami |
| [FALCON-1882](https://issues.apache.org/jira/browse/FALCON-1882) | Instance status api not working via prism |  Major | prism | Pragya Mittal | Praveen Adlakha |
| [FALCON-1881](https://issues.apache.org/jira/browse/FALCON-1881) | Database Export should not expect fields list in the feed entity specification |  Major | acquisition | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1880](https://issues.apache.org/jira/browse/FALCON-1880) | To support TDE encryption : Add --skipcrccheck to distcp options for HiveDR |  Major | replication | Balu Vellanki | Balu Vellanki |
| [FALCON-1877](https://issues.apache.org/jira/browse/FALCON-1877) | Falcon webUI returns 413 (Full head - Request entity too large) error when TLS is enabled in a secure cluster with AD integration |  Major | . | Venkat Ranganathan | Venkat Ranganathan |
| [FALCON-1874](https://issues.apache.org/jira/browse/FALCON-1874) | Import and Export fails with HDFS as src/dest |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-1867](https://issues.apache.org/jira/browse/FALCON-1867) | hardcoded query names in JDBCStateStore |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1866](https://issues.apache.org/jira/browse/FALCON-1866) | Bug in JDBCStateStore |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1864](https://issues.apache.org/jira/browse/FALCON-1864) | Retry event does not get removed from delay queue even after the instance succeeds |  Major | rerun | Pallavi Rao | Pallavi Rao |
| [FALCON-1859](https://issues.apache.org/jira/browse/FALCON-1859) | Database Export instances are not added graph db for lineage tracking |  Major | general | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1855](https://issues.apache.org/jira/browse/FALCON-1855) | Falcon regression build fails with checkstyle issues |  Major | regression | Pragya Mittal | Murali Ramasami |
| [FALCON-1854](https://issues.apache.org/jira/browse/FALCON-1854) | Fixing PrismProcessScheduleTest and NoOutputProcessTest |  Major | regression | Murali Ramasami | Murali Ramasami |
| [FALCON-1848](https://issues.apache.org/jira/browse/FALCON-1848) | Late rerun is not working due to failnodes set to true |  Major | rerun | Pragya Mittal | Pallavi Rao |
| [FALCON-1847](https://issues.apache.org/jira/browse/FALCON-1847) | Execution order not honored when instances are suspended/resumed |  Major | scheduler | Pallavi Rao | Pallavi Rao |
| [FALCON-1846](https://issues.apache.org/jira/browse/FALCON-1846) | Fixing EntityDryRunTest |  Major | regression | Pragya Mittal | Pragya Mittal |
| [FALCON-1845](https://issues.apache.org/jira/browse/FALCON-1845) | Retries Stopped happening  for all entities when one entity was deleted during rerun of instance |  Major | rerun | pavan kumar kolamuri | pavan kumar kolamuri |
| [FALCON-1842](https://issues.apache.org/jira/browse/FALCON-1842) | Falcon build failed in Jenkins at org.apache.falcon.oozie.feed.OozieFeedWorkflowBuilderTest |  Major | falcon-unit | Balu Vellanki | Balu Vellanki |
| [FALCON-1840](https://issues.apache.org/jira/browse/FALCON-1840) | Archive older definition in case of update |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1838](https://issues.apache.org/jira/browse/FALCON-1838) | Export instances are not added graph db for lineage tracking |  Major | . | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1826](https://issues.apache.org/jira/browse/FALCON-1826) | Execution order not honoured when instances are KILLED |  Major | scheduler | Pragya Mittal | Pallavi Rao |
| [FALCON-1825](https://issues.apache.org/jira/browse/FALCON-1825) | Process end time inclusive in case of Native Scheduler |  Major | scheduler | pavan kumar kolamuri | pavan kumar kolamuri |
| [FALCON-1823](https://issues.apache.org/jira/browse/FALCON-1823) |  wrong permissions on hadoolibs and conf folder in distributed mode deb |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1819](https://issues.apache.org/jira/browse/FALCON-1819) | Improve test class entity cleanup logic |  Major | merlin | Paul Isaychuk | Paul Isaychuk |
| [FALCON-1816](https://issues.apache.org/jira/browse/FALCON-1816) | Fix findbugs-exclude.xml path and hadoop version in falcon-regression pom |  Major | merlin | Paul Isaychuk | Paul Isaychuk |
| [FALCON-1811](https://issues.apache.org/jira/browse/FALCON-1811) | Status API does not honour start option |  Major | client | Pragya Mittal | Praveen Adlakha |
| [FALCON-1796](https://issues.apache.org/jira/browse/FALCON-1796) | [HOTFIX] Incorrect parent pom in distro module |  Major | . | Ajay Yadava | Ajay Yadava |
| [FALCON-1795](https://issues.apache.org/jira/browse/FALCON-1795) | Kill api does not kill waiting/ready instances |  Major | oozie | Pragya Mittal | sandeep samudrala |
| [FALCON-1793](https://issues.apache.org/jira/browse/FALCON-1793) | feed element action="archive" is submittable via command line tool falcon |  Major | feed | Margus Roo | Deepak Barr |
| [FALCON-1792](https://issues.apache.org/jira/browse/FALCON-1792) | Upgrade hadoop.version to 2.6.2 |  Major | hadoop | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1787](https://issues.apache.org/jira/browse/FALCON-1787) | Ooozie pig-action.xml requires hive sharelib for HCatalog use |  Major | oozie | Mark Greene | Sowmya Ramesh |
| [FALCON-1784](https://issues.apache.org/jira/browse/FALCON-1784) | Add regression test for for FALCON-1647 |  Major | merlin | Paul Isaychuk | Paul Isaychuk |
| [FALCON-1783](https://issues.apache.org/jira/browse/FALCON-1783) | Fix ProcessUpdateTest and SearchApiTest to use prism |  Major | merlin | Paul Isaychuk | Paul Isaychuk |
| [FALCON-1766](https://issues.apache.org/jira/browse/FALCON-1766) | Add CLI metrics check for HiveDR, HDFS and feed replication |  Major | merlin | Paul Isaychuk | Paul Isaychuk |
| [FALCON-1743](https://issues.apache.org/jira/browse/FALCON-1743) | Entity summary does not work via prism |  Major | client | Pragya Mittal | Ajay Yadava |
| [FALCON-1724](https://issues.apache.org/jira/browse/FALCON-1724) | Falcon CLI.twiki in docs folder is not pointed by index page |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1721](https://issues.apache.org/jira/browse/FALCON-1721) | Move checkstyle artifacts under parent |  Major | . | Shwetha G S | sandeep samudrala |
| [FALCON-1621](https://issues.apache.org/jira/browse/FALCON-1621) | Lifecycle of entity gets missed when prism and falcon server communicates |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1584](https://issues.apache.org/jira/browse/FALCON-1584) | Falcon allows invalid hadoop queue name for schedulable feed entities |  Major | . | Venkatesan Ramachandran | Venkatesan Ramachandran |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [FALCON-2099](https://issues.apache.org/jira/browse/FALCON-2099) | Update Installation-steps.txt and NOTICE.txt for 0.10 release |  Major | ease | Balu Vellanki | Balu Vellanki |
| [FALCON-2000](https://issues.apache.org/jira/browse/FALCON-2000) | Create branch 0.10 |  Major | general | Balu Vellanki | Balu Vellanki |
| [FALCON-1996](https://issues.apache.org/jira/browse/FALCON-1996) | Upgrade falcon POM for 0.10 release |  Major | build-tools | Balu Vellanki | Balu Vellanki |
| [FALCON-1993](https://issues.apache.org/jira/browse/FALCON-1993) | Update JIRA fix versions |  Major | general | Balu Vellanki | Balu Vellanki |
| [FALCON-1980](https://issues.apache.org/jira/browse/FALCON-1980) | Change input and output argument order for Spark process workflow |  Major | . | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-1954](https://issues.apache.org/jira/browse/FALCON-1954) | Steps to configure Oozie JMS for Falcon |  Major | messaging | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1938](https://issues.apache.org/jira/browse/FALCON-1938) | Add support to execute Spark SQL process |  Major | . | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-1937](https://issues.apache.org/jira/browse/FALCON-1937) | Add documentation for cluster update. |  Major | . | Balu Vellanki | Balu Vellanki |
| [FALCON-1929](https://issues.apache.org/jira/browse/FALCON-1929) | Extension job management: IT tests for CLIs |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1905](https://issues.apache.org/jira/browse/FALCON-1905) | Extension Job Management: IT tests for REST APIs and CLIs |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1904](https://issues.apache.org/jira/browse/FALCON-1904) | Extension Job Management: documentation for REST APIs and CLI |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1902](https://issues.apache.org/jira/browse/FALCON-1902) | Server side extension repository management CLI support |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1897](https://issues.apache.org/jira/browse/FALCON-1897) | Extension Job Management: CLI support |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1893](https://issues.apache.org/jira/browse/FALCON-1893) | Add documentation and examples for spark workflow engine |  Major | . | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-1892](https://issues.apache.org/jira/browse/FALCON-1892) | Remove client side Recipe logic |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1860](https://issues.apache.org/jira/browse/FALCON-1860) | ADFProviderService should be optional as default setting |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1853](https://issues.apache.org/jira/browse/FALCON-1853) | Add spark process workflow builder |  Major | oozie | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-1839](https://issues.apache.org/jira/browse/FALCON-1839) | Test case for APIs for entities scheduled on native scheduler |  Major | scheduler | Pragya Mittal | Pragya Mittal |
| [FALCON-1831](https://issues.apache.org/jira/browse/FALCON-1831) | Flaky WorkflowExecutionContextTest.testWorkflowStartEnd |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-1829](https://issues.apache.org/jira/browse/FALCON-1829) | Add regression for submit and schedule process on native scheduler (time based) |  Major | scheduler | Pragya Mittal | Pragya Mittal |
| [FALCON-1817](https://issues.apache.org/jira/browse/FALCON-1817) | Update xsd for Spark execution engine |  Major | . | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-1801](https://issues.apache.org/jira/browse/FALCON-1801) | Update CHANGES.txt in trunk to mark 0.9 as released |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-1790](https://issues.apache.org/jira/browse/FALCON-1790) | CLI support for instance search |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1789](https://issues.apache.org/jira/browse/FALCON-1789) | Extension Job Management: REST API |  Major | . | Sowmya Ramesh | Ying Zheng |
| [FALCON-1767](https://issues.apache.org/jira/browse/FALCON-1767) | Improve Falcon retention policy documentation |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1729](https://issues.apache.org/jira/browse/FALCON-1729) | Database ingest to support password alias via keystore file |  Major | acquisition | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1646](https://issues.apache.org/jira/browse/FALCON-1646) | Ability to export to database - Entity Definition |  Major | acquisition | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1496](https://issues.apache.org/jira/browse/FALCON-1496) | Flaky FalconPostProcessingTest |  Major | . | Pallavi Rao | Pallavi Rao |
| [FALCON-1335](https://issues.apache.org/jira/browse/FALCON-1335) | Backend support of instance search of a group of entities |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1334](https://issues.apache.org/jira/browse/FALCON-1334) | Improve search performance with Titan graph database indexing |  Major | . | Ying Zheng | Ying Zheng |
| [FALCON-1111](https://issues.apache.org/jira/browse/FALCON-1111) | Instance update on titan DB based on JMS notifications on workflow jobs |  Major | common, messaging | Sowmya Ramesh | Ying Zheng |
| [FALCON-1107](https://issues.apache.org/jira/browse/FALCON-1107) | Move trusted recipe processing to server side |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1106](https://issues.apache.org/jira/browse/FALCON-1106) | Documentation for extension |  Major | . | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1105](https://issues.apache.org/jira/browse/FALCON-1105) | Server side extension repository management REST API support |  Major | client | Sowmya Ramesh | Sowmya Ramesh |
| [FALCON-1085](https://issues.apache.org/jira/browse/FALCON-1085) | Allow cluster entities to be updated |  Major | . | Ajay Yadava | Balu Vellanki |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [FALCON-2063](https://issues.apache.org/jira/browse/FALCON-2063) | Add change log for 0.10 |  Major | . | Ajay Yadava | Ajay Yadava |
| [FALCON-1765](https://issues.apache.org/jira/browse/FALCON-1765) | Move to github pull request model |  Major | . | Ajay Yadava | Ajay Yadava |
| [FALCON-2008](https://issues.apache.org/jira/browse/FALCON-2008) | Add documentation for Graphite Notification Plugin |  Major | . | Praveen Adlakha | Praveen Adlakha |
| [FALCON-1948](https://issues.apache.org/jira/browse/FALCON-1948) | Document steps to configure Oozie for Falcon |  Major | docs | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1899](https://issues.apache.org/jira/browse/FALCON-1899) | Create examples artifact module in Falcon |  Major | . | Peeyush Bishnoi | Peeyush Bishnoi |
| [FALCON-1888](https://issues.apache.org/jira/browse/FALCON-1888) | Falcon JMS Notification details and documentation |  Major | docs | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1818](https://issues.apache.org/jira/browse/FALCON-1818) | Minor doc update for tar package locations after FALCON-1751 |  Minor | . | Deepak Barr | Deepak Barr |
| [FALCON-1806](https://issues.apache.org/jira/browse/FALCON-1806) | Update documentation for Import and Export |  Major | . | Venkatesan Ramachandran | Venkatesan Ramachandran |
| [FALCON-1567](https://issues.apache.org/jira/browse/FALCON-1567) | Test case for Lifecycle feature |  Major | merlin | Pragya Mittal | Pragya Mittal |
| [FALCON-1566](https://issues.apache.org/jira/browse/FALCON-1566) | Add test for SLA monitoring API |  Major | merlin | Pragya Mittal | Pragya Mittal |
