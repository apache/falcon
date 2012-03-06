/*
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

package org.apache.ivory.transaction;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ActionTest {

    @Test
    public void testGetName() throws Exception {
        Action action = new Action("hello11", "category22", "label33");
        Assert.assertEquals(action.getName(), "hello11");
        Assert.assertEquals(action.getCategory(), "category22");
        Assert.assertEquals(action.getLabel(), "label33");
    }

    @Test
    public void testFromLine() throws Exception {
        Action action = new Action("hello11", "category22", "label33");
        String str = action.toString();
        Action actionNew = Action.fromLine(str);

        Assert.assertEquals(action.getName(), actionNew.getName());
        Assert.assertEquals(action.getCategory(), actionNew.getCategory());
        Assert.assertEquals(action.getLabel(), actionNew.getLabel());
    }
}
