/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function () {
  "use strict";

  var bodyParser = require('body-parser'),
    express = require('express'),
    mockData = require('./express-data/mockData.js'),
    server = express(),
    PORT = 3000;

  server.use('/', express.static(__dirname + '/dist'));
  server.use(bodyParser());
  server.use(function (req, res, next) {
    if (req.is('text/*')) {
      req.text = '';
      req.setEncoding('utf8');
      req.on('data', function (chunk) { req.text += chunk; });
      req.on('end', next);
    } else {
      next();
    }
  });

  server.get('/api/entities/list/:type', function (req, res) {
    var type = req.params.type;
    res.json(mockData.entitiesList[type]);
  });

  server.get('/api/entities/definition/:type/:name', function(req, res) {
    var type = req.params.type.toUpperCase(),
      name = req.params.name;
    if (mockData.definitions[type][name]) {
      res.send(200, mockData.definitions[type][name]);
    } else {
      res.send(404, "not found");
    }
  });

  server.post('/api/entities/submit/:type', function (req, res) {
    var type = req.params.type.toUpperCase(),
      text = req.text,
      name,
      indexInArray,
      responseSuccessMessage,
      responseFailedMessage,
      initialIndex = text.indexOf("name") + 6,
      finalIndex = getFinalIndexOfName(),
      i;
    function getFinalIndexOfName () {
      for (i = initialIndex; i < text.length; i++) {
        if (text[i] === '"' || text[i] === "'") {
          return i;
        }
      }
    }
    name = text.slice(initialIndex, finalIndex);
    responseSuccessMessage = {"status": "SUCCEEDED", "message": "default/successful (" + type + ") " + name + "\n\n","requestId":"default/546cbe05-2cb3-4e5c-8e7a-b1559d866c99\n"};
    responseFailedMessage = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><result><status>FAILED</status><message>(' + type + ') '+ name +' already registered with configuration store. Can\'t be submitted again. Try removing before submitting.</message><requestId>586fffcd-10c1-4975-8dda-4b34a712f2f4</requestId></result>';

    if (!mockData.definitions[type][name]) {
      mockData.definitions[type][name] = text;
      mockData.entitiesList[type.toLowerCase()].entity.push(
        {"type": type, "name": name, "status": "SUBMITTED"}
      );
      res.send(200, responseSuccessMessage);
    } else {
      res.send(404, responseFailedMessage);
    }
  });

  server.post('/api/entities/schedule/:type/:name', function (req, res) {
    var type = req.params.type.toLowerCase(),
      name = req.params.name,
      indexInArray = mockData.findByNameInList(type, name),
      responseMessage = {
        "status": "SUCCEEDED",
        "message": "default/" + name + "(" + type + ") scheduled successfully\n",
        "requestId": "default/546cbe05-2cb3-4e5c-8e7a-b1559d866c99\n"
      };
    mockData.entitiesList[type].entity[indexInArray].status = "RUNNING";
    res.json(200, responseMessage);
  });

  server.post('/api/entities/suspend/:type/:name', function (req, res) {
    var type = req.params.type.toLowerCase(),
      name = req.params.name,
      indexInArray = mockData.findByNameInList(type, name),
      responseMessage = {
        "status": "SUCCEEDED",
        "message": "default/" + name + "(" + type + ") suspended successfully\n",
        "requestId": "default/546cbe05-2cb3-4e5c-8e7a-b1559d866c99\n"
      };
    mockData.entitiesList[type].entity[indexInArray].status = "SUSPENDED";
    res.json(200, responseMessage);
  });

  server.post('/api/entities/resume/:type/:name', function (req, res) {
    var type = req.params.type.toLowerCase(),
      name = req.params.name,
      indexInArray = mockData.findByNameInList(type, name),
      responseMessage = {
        "status": "SUCCEEDED",
        "message": "default/" + name + "(" + type + ") resumed successfully\n",
        "requestId": "default/546cbe05-2cb3-4e5c-8e7a-b1559d866c99\n"
      };
    mockData.entitiesList[type].entity[indexInArray].status = "RUNNING";
    res.json(200, responseMessage);
  });

  server.delete('/api/entities/delete/:type/:name', function (req, res) {
    var type = req.params.type,
      name = req.params.name,
      responseMessage = {
        "status": "SUCCEEDED",
        "message": "falcon/default/" + name + "(" + type + ")removed successfully (KILLED in ENGINE)\n\n",
        "requestId": "falcon/default/13015853-8e40-4923-9d32-6d01053c31c6\n\n"
      },
      indexInArray = mockData.findByNameInList(type, name);
    mockData.entitiesList[type].entity.splice(indexInArray, 1);
    res.json(200, responseMessage);
  });

  server.listen(PORT, function () {
    console.log('Dev server listening on port ' + PORT);
  });

}());
