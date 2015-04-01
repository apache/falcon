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
  'use strict';

  var createProcessButton = element(by.id('createProcessButton'));
  var editXmlButton = element(by.id('editXmlButton'));
  var fieldWrapper = element(by.id('fieldWrapper'));
  var xmlPreviewArea = element(by.model('prettyXml'));
  var nextButton = element(by.id('nextButton'));

  var nameField = element(by.id('entityNameField'));
  var tagsSection = element(by.id('tagsSection'));
  var engineSection = element(by.id('engineSection'));
  var engineVersionField = element(by.id('engineVersionField'));
  var workflowNameField = element(by.id('workflowNameField'));
  var pigEngineRadio = element(by.id('pigEngineRadio'));
  var pigVersionOption = element(by.id('pigVersion1'));
  var pathField = element(by.id('pathField'));


  beforeEach(function() {
    browser.get('http://localhost.localdomain:3000');
  });

  describe('Process Entity', function() {

    describe('Create', function() {
      it('Should navigate to the entry form when clicking the Process on the navigation bar', function() {

        createProcessButton.click();

        var title = element(by.id('formTitle'));

        expect(title.getText()).toEqual('New Process');
      });

      it('Should present the xml disabled', function() {
        createProcessButton.click();

        expect(fieldWrapper.isEnabled()).toBe(true);
        expect(xmlPreviewArea.isEnabled()).toBe(false);
      });

      it('Should toggle the disable mode between the form and the xml edit field when pressing the edit xml button--', function() {
        createProcessButton.click();

        editXmlButton.click();
        expect(xmlPreviewArea.isEnabled()).toBe(true);

        editXmlButton.click();
        expect(xmlPreviewArea.isEnabled()).toBe(false);
      });

      it('Should present the general information fields', function() {
        createProcessButton.click();

        expect(nameField).toBeTruthy();
        expect(tagsSection).toBeTruthy();
        expect(workflowNameField).toBeTruthy();
        expect(engineSection).toBeTruthy();
        expect(engineVersionField).toBeTruthy();
        expect(pathField).toBeTruthy();
      });

      it('Should navigate to the Timing and Properties page', function() {
        createProcessButton.click();

        nameField.sendKeys('ProcessName');
        workflowNameField.sendKeys('emailCleanseWorkflow')
        pigEngineRadio.click();
        pathField.sendKeys('/user/ambari-qa/falcon/demo/apps/pig/id.pig');
        pigVersionOption.click();

        nextButton.click();
      });

    });
  });
})();