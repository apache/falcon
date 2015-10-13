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

  /*
   * tf: textfield
   * bt: button
   * lb: label
   * tl: title
   * valid: validations
   * msgs: messages
   */

	var cluster = {};
	cluster.bt = {};
	cluster.tf = {};
	cluster.tl = {};
	cluster.valid = {};
	cluster.msgs = {};

	cluster.bt.create = element(by.id('cluster.create'));
	cluster.tl.create = element(by.id('cluster.title'));
	cluster.bt.editXML = element(by.id('cluster.editXML'));
	cluster.bt.prettyXml = element(by.model('prettyXml'));

	cluster.tf.name = element(by.model('clusterEntity.clusterModel.cluster._name'));
	cluster.tf.colo = element(by.model('clusterEntity.clusterModel.cluster._colo'));
	cluster.tf.desc = element(by.model('clusterEntity.clusterModel.cluster._description'));
	cluster.tf.staging = element(by.id('location.staging'));
	cluster.tf.temp = element(by.id('location.temp'));
	cluster.tf.working = element(by.id('location.working'));

	cluster.bt.step1 = element(by.id('cluster.step1'));
	cluster.bt.backToStep1 = element(by.id('cluster.backToStep1'));
	cluster.bt.step2 = element(by.id('cluster.step2'));

   beforeEach(function() {
	browser.get('http://localhost:3000');
   });

   describe('Cluster Entity', function() {
	describe('Create', function() {

		it('Should navigate to the entry form when clicking the Cluster on the navigation bar', function() {

			cluster.bt.create.click();
			expect(cluster.tl.create.getText()).toEqual('New Cluster');

	      });

	      it('Should present the xml disabled', function() {

		cluster.bt.create.click();
		expect(cluster.bt.editXML.isEnabled()).toBe(true);
		expect(cluster.bt.prettyXml.isEnabled()).toBe(false);

	      });

	      it('Should toggle the disable mode between the form and the xml edit field when pressing the edit xml button--', function() {

		cluster.bt.create.click();

		cluster.bt.editXML.click();
		expect(cluster.bt.prettyXml.isEnabled()).toBe(true);

		cluster.bt.editXML.click();
		expect(cluster.bt.prettyXml.isEnabled()).toBe(false);
	      });

	      it('Should present the General fields', function() {

		cluster.bt.create.click();

		expect(cluster.tf.name).toBeTruthy();
		expect(cluster.tf.colo).toBeTruthy();
		expect(cluster.tf.desc).toBeTruthy();
		expect(cluster.tf.staging).toBeTruthy();
		expect(cluster.tf.temp).toBeTruthy();
		expect(cluster.tf.working).toBeTruthy();

	      });

	      it('Should validate the cluster name', function() {

		cluster.bt.create.click();

		cluster.tf.name.sendKeys('completeCluster');

		cluster.bt.step1.click();

					cluster.valid.name = element(by.css('.nameValidationMessage'));

		expect(cluster.valid.name.getText()).toEqual('The name you choosed is not available');

	      });

	      it('Should pass & go to the next step', function() {

		cluster.bt.create.click();

		cluster.tf.name.sendKeys('testCluster9999');
		cluster.tf.colo.sendKeys('testCluster9999colo');
		cluster.tf.desc.sendKeys('testCluster9999desc');
		cluster.tf.staging.sendKeys('testCluster9999staging');
		cluster.tf.temp.sendKeys('testCluster9999temp');
		cluster.tf.working.sendKeys('testCluster9999working');

		cluster.bt.step1.click();

		expect(cluster.bt.backToStep1.isEnabled()).toBe(true);
		expect(cluster.bt.step2.isEnabled()).toBe(true);

	      });

	      it('Should complete a save cluster', function() {

		cluster.bt.create.click();

		cluster.tf.name.sendKeys('testCluster9999');
		cluster.tf.colo.sendKeys('testCluster9999colo');
		cluster.tf.desc.sendKeys('testCluster9999desc');
		cluster.tf.staging.sendKeys('testCluster9999staging');
		cluster.tf.temp.sendKeys('testCluster9999temp');
		cluster.tf.working.sendKeys('testCluster9999working');

		cluster.bt.step1.click();
		cluster.bt.step2.click();

		cluster.msgs.success = element(by.css('.text-success'));
		expect(cluster.msgs.success.getText()).toEqual('SUCCEEDED');

	      });

	});
   });

})();