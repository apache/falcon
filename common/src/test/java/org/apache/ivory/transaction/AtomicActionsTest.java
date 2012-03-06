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

import org.apache.ivory.IvoryException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AtomicActionsTest {

    private static class MockJournal implements Journal {

        int begin = 0;
        int commit = 0;
        int rollback = 0;

        List<Action> actions = new ArrayList<Action>();

        @Override
        public void begin(AtomicActions id) throws IvoryException {
            begin++;
        }

        @Override
        public void commit(AtomicActions id) throws IvoryException {
            commit++;
        }

        @Override
        public void rollback(AtomicActions id) throws IvoryException {
            rollback++;
        }

        @Override
        public void onAction(AtomicActions id, Action action) throws IvoryException {
            actions.add(action);
        }

        @Override
        public List<AtomicActions> getUncommittedActions() throws IvoryException {
            return null;
        }
    }

    @Test
    public void testCommit() throws Exception {
        MockJournal mock = new MockJournal();
        AtomicActions trans = new AtomicActions();
        trans.setHandler(mock);
        Action action = new Action("1", "2", "3");
        trans.peform(action);
        Assert.assertEquals(trans.getUncommittedActions().size(), 1);
        Assert.assertEquals(trans.getUncommittedActions().get(0), action);
        trans.commit();
        Assert.assertEquals(mock.begin, mock.commit + mock.rollback);
        Assert.assertEquals(mock.commit, 1);
        Assert.assertEquals(trans.getUncommittedActions().size(), 0);
    }

    @Test
    public void testRollback() throws Exception {
        MockJournal mock = new MockJournal();
        AtomicActions trans = new AtomicActions();
        trans.setHandler(mock);
        Action action = new Action("1", "2", "3");
        trans.peform(action);
        Assert.assertEquals(trans.getUncommittedActions().size(), 1);
        Assert.assertEquals(trans.getUncommittedActions().get(0), action);
        trans.rollback();
        Assert.assertEquals(mock.begin, mock.commit + mock.rollback);
        Assert.assertEquals(mock.rollback, 1);
        Assert.assertEquals(trans.getUncommittedActions().size(), 0);
    }

    @Test
    public void testGetId() throws Exception {
        List<Action> actions = new ArrayList<Action>();
        UUID id = UUID.randomUUID();
        actions.add(new Action("a", "b", "c"));
        actions.add(new Action("d", "e", "f"));
        AtomicActions trans = new AtomicActions(id, actions);
        Assert.assertEquals(trans.getId(), id.toString());
        Assert.assertEquals(trans.getUncommittedActions().size(), actions.size());
        Assert.assertEquals(trans.getUncommittedActions(), actions);
    }
}
