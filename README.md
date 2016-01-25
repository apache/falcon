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

## Online Documentation

You can find the documentation on [Apache Falcon website](http://falcon.apache.org/). 

## How to Contribute

Before opening a pull request, please go through the [Contributing to Apache Falcon wiki](https://cwiki.apache.org/confluence/display/FALCON/How+To+Contribute). It lists steps that are required before creating a PR and the conventions that we follow. If you are looking for issues to pick up then you can look at [starter tasks](https://issues.apache.org/jira/issues/?jql=project%20%3D%20FALCON%20AND%20resolution%20%3D%20Unresolved%20AND%20labels%20%3D%20newbie%20AND%20assignee%20in%20(EMPTY)%20ORDER%20BY%20priority%20DESC) or  [open tasks](https://issues.apache.org/jira/issues/?jql=project%20%3D%20FALCON%20AND%20resolution%20%3D%20Unresolved%20AND%20assignee%20in%20(EMPTY)%20ORDER%20BY%20priority%20DESC) 


## Release Notes
You can download release notes of previous releases from the following links.

[0.8](https://cwiki.apache.org/confluence/download/attachments/61318307/RelaseNotes-ApacheFalcon-0.7.pdf?api=v2)
 
[0.7](https://cwiki.apache.org/confluence/download/attachments/61318307/RelaseNotes-ApacheFalcon-0.7.pdf?api=v2)



