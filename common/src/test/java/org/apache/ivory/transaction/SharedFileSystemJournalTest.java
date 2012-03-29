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
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;

public class SharedFileSystemJournalTest {

    private static Logger LOG = Logger.getLogger(SharedFileSystemJournalTest.class);
    private SharedFileSystemJournal journal =
            (SharedFileSystemJournal) SharedFileSystemJournal.get();

    private File journalPath = journal.getBasePath();

    @BeforeClass
    @AfterClass
    public void reset() {
        for (File file : journalPath.listFiles()) {
            LOG.info(file.getAbsolutePath() + " - " + file.delete());
        }
    }

    @Test
    public void testBegin() throws Exception {
        AtomicActions id = new AtomicActions();
        journal.begin(id);
        File file = new File(journalPath, id.getId());
        Assert.assertFalse(file.exists());
    }

    @Test
    public void testCommit() throws Exception {
        AtomicActions id = new AtomicActions();
        journal.onAction(id, new TestAction("2"));
        journal.commit(id);
        File file = new File(journalPath, id.getId());
        Assert.assertFalse(file.exists());

        id = new AtomicActions();
        try {
            journal.commit(id);
            Assert.fail("Expect commit to fail. Nothing to commit");
        } catch (IllegalStateException ignore) {
            //Test pass
        } catch (IvoryException e) {
            Assert.fail("Unexpected exception", e);
        }
    }

    @Test
    public void testRollback() throws Exception {
        AtomicActions id = new AtomicActions();
        journal.onAction(id, new TestAction("2"));
        journal.rollback(id);
        File file = new File(journalPath, id.getId());
        Assert.assertFalse(file.exists());

        id = new AtomicActions();
        try {
            journal.rollback(id);
            Assert.fail("Expect rollback to fail. Nothing to rollback");
        } catch (IllegalStateException ignore) {
            //Test pass
        } catch (IvoryException e) {
            Assert.fail("Unexpected exception", e);
        }
    }

    @Test
    public void testOnAction() throws Exception {
        AtomicActions id = new AtomicActions();
        Action action = new TestAction("2");
        int length = action.toString().length() + 1;

        journal.onAction(id, action);
        File file = new File(journalPath, id.getId());
        Assert.assertTrue(file.exists());
        Assert.assertEquals(file.length(), length);

        action = new TestAction("world");
        length += action.toString().length() + 1;

        journal.onAction(id, action);
        Assert.assertTrue(file.exists());
        Assert.assertEquals(file.length(), length);
    }

    @Test
    public void testGetUncommittedActions() throws Exception {
        AtomicActions id1 = new AtomicActions();
        Action action1 = new TestAction("2");
        journal.onAction(id1, action1);
        Map<String, Action> actionMap = new HashMap<String, Action>();
        actionMap.put(id1.getId(), action1);

        AtomicActions id2 = new AtomicActions();
        Action action2 = new TestAction("hello");
        journal.onAction(id2, action2);
        actionMap.put(id2.getId(), action2);

        List<AtomicActions> trans = journal.getUncommittedActions();
        Set<String> origids = new HashSet<String>();
        origids.add(id1.getId());
        origids.add(id2.getId());

        Set<String> ids = new HashSet<String>();
        for (AtomicActions tran : trans) {
            ids.add(tran.getId());
            LOG.info(tran.getUncommittedActions().get(0) + ", " + actionMap.get(tran.getId()));
//            Assert.assertEquals(tran.getUncommittedActions().get(0).toString(),
//                    actionMap.get(tran.getId()).toString());
        }
        Assert.assertTrue(ids.containsAll(origids));
    }
}