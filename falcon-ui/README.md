Falcon-ui
=========

Web UI to manage feeds, clusters and processes with the falcon REST API

Before starting
===============

NodeJs , npm, Grunt must be installed in the local pc.

- From git root cd to /falcon-ui

- npm install (this will install all the app related node modules)

To test in the express server
=============================

- grunt dev
This will launch an express server with the falcon-ui to localhost:3000
(You can test there all UI related behaviours and express will mock all falcon REST calls)

To deploy to the sandbox (v2-2)
===============================

- grunt deploy
This will build and send to the sandbox /usr/hdp/2.2.0.0-913/falcon/webapp/falcon/public/ location the falcon webapp
Then navigate to localhost:15000

!important - It is possible that you will need to add to the url /public like http://localhost:15000/public/ or replace /usr/hdp/2.2.0.0-913/falcon/webapp/falcon/index.html in the sandbox with incubator-falcon/html5-ui/index.html

To only build the app (in the /dist folder)
===========================================

grunt build

To unit test Javascript
grunt test

To end to end test
grunt testE2E
