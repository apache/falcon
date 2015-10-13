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

	var feed = {};
	feed.bt = {};
	feed.tf = {};
	feed.tl = {};
	feed.valid = {};
	feed.msgs = {};

	feed.bt.create = element(by.id('feed.create'));
	feed.tl.create = element(by.id('feed.title'));
	feed.bt.editXML = element(by.id('feed.editXML'));
	feed.bt.prettyXml = element(by.model('prettyXml'));

	feed.tf.name = element(by.model('feed.name'));
	feed.tf.desc = element(by.model('feed.description'));
	feed.tf.owner = element(by.model('feed.ACL.owner'));
	feed.tf.group = element(by.model('feed.ACL.group'));
	feed.tf.location = element(by.model('feed.schema.location'));
	feed.tf.provider = element(by.model('feed.schema.provider'));
	feed.tf.frecuency = element(by.model('feed.frequency.quantity'));
	feed.tf.clusterName = element(by.model('cluster.name'));
	feed.tf.startDate = element(by.model('cluster.validity.start.date'));
	feed.tf.endDate = element(by.model('cluster.validity.end.date'));
	feed.tf.retention = element(by.model('cluster.retention.quantity'));

	feed.bt.step1 = element(by.id('feed.step1'));
	feed.bt.backToStep1 = element(by.id('feed.backToStep1'));
	feed.bt.step2 = element(by.id('feed.step2'));
	feed.bt.backToStep2 = element(by.id('feed.backToStep2'));
	feed.bt.step3 = element(by.id('feed.step3'));
	feed.bt.backToStep3 = element(by.id('feed.backToStep3'));
	feed.bt.step4 = element(by.id('feed.step4'));
	feed.bt.backToStep4 = element(by.id('feed.backToStep4'));
	feed.bt.step5 = element(by.id('feed.step5'));
	feed.bt.backToStep5 = element(by.id('feed.backToStep5'));

   beforeEach(function() {
	browser.get('http://localhost:3000');
   });

   describe('Feed Entity', function() {
	describe('Create', function() {

		it('Should navigate to the entry form when clicking the Cluster on the navigation bar', function() {

			feed.bt.create.click();
			expect(feed.tl.create.getText()).toEqual('New Feed');

	      });

	      it('Should present the xml disabled', function() {

		feed.bt.create.click();
		expect(feed.bt.editXML.isEnabled()).toBe(true);
		expect(feed.bt.prettyXml.isEnabled()).toBe(false);

	      });

	      it('Should toggle the disable mode between the form and the xml edit field when pressing the edit xml button--', function() {

		feed.bt.create.click();

		feed.bt.editXML.click();
		expect(feed.bt.prettyXml.isEnabled()).toBe(true);

		feed.bt.editXML.click();
		expect(feed.bt.prettyXml.isEnabled()).toBe(false);
	      });

	      it('Should present the General fields', function() {

		feed.bt.create.click();

		expect(feed.tf.name).toBeTruthy();
		expect(feed.tf.desc).toBeTruthy();
		expect(feed.tf.owner).toBeTruthy();
		expect(feed.tf.group).toBeTruthy();
		expect(feed.tf.location).toBeTruthy();

	      });

	      it('Should validate the cluster name', function() {

		feed.bt.create.click();

		feed.tf.name.sendKeys('feedOne');

		feed.valid.name = element(by.css('.nameValidationMessage'));

		feed.bt.step1.click();

		expect(feed.valid.name.getText()).toEqual('The name you choosed is not available');

	      });

	      it('Should complete a save cluster', function() {

		feed.bt.create.click();

		feed.tf.name.sendKeys('testFeed9999');
		feed.tf.desc.sendKeys('testFeed9999desc');
		feed.tf.owner.sendKeys('owner');
		feed.tf.group.sendKeys('group');
		feed.tf.location.sendKeys('testFeed9999location');
		feed.tf.provider.sendKeys('testFeed9999provider');

		expect(feed.bt.step1.isEnabled()).toBe(true);

		feed.bt.step1.click();

		feed.tf.frecuency.sendKeys('1');

		expect(feed.bt.backToStep1.isEnabled()).toBe(true);
		expect(feed.bt.step2.isEnabled()).toBe(true);

		feed.bt.step2.click();

		expect(feed.bt.backToStep2.isEnabled()).toBe(true);
		expect(feed.bt.step3.isEnabled()).toBe(true);

		feed.bt.step3.click();


		feed.tf.clusterName.all(by.tagName('option')).get(1).then(function(opt) {
		  opt.click();
		});

		feed.tf.startDate.sendKeys('29-December-2014');
		feed.tf.endDate.sendKeys('30-January-2015');
		feed.tf.retention.sendKeys('1');

		expect(feed.bt.backToStep3.isEnabled()).toBe(true);
		expect(feed.bt.step4.isEnabled()).toBe(true);

		feed.bt.step4.click();

		expect(feed.bt.backToStep4.isEnabled()).toBe(true);
		expect(feed.bt.step5.isEnabled()).toBe(true);

		feed.bt.step5.click();

		feed.msgs.success = element(by.css('.text-success'));
		expect(feed.msgs.success.getText()).toEqual('SUCCEEDED');

	      });

	});
   });

})();