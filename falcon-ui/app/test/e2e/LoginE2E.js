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

   var login = {};
   login.bt = {};
   login.tf = {};
   login.tl = {};

	login.tf.user = element(by.model('login.user'));
	login.tf.password = element(by.model('login.password'));
	login.bt.login = element(by.id('login.submit'));
	login.tl.cluster = element(by.id('cluster.create'));

   beforeEach(function() {
	browser.get('http://localhost:3000');
   });

	describe('Login', function() {
      it('Should login when clicking the login button', function() {

	login.tf.user.sendKeys('ambari-qa');
	login.tf.password.sendKeys('admin');
	login.bt.login.click();
	expect(login.tl.cluster.getText()).toEqual('Cluster');

      });
    });

})();