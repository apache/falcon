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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ivory.IvoryException;
import org.apache.log4j.Logger;

public class AtomicActions {

    private static Logger LOG = Logger.getLogger("TRANSACTIONLOG");

    private final UUID actionID;
    private AtomicBoolean begun = new AtomicBoolean(false);
    private AtomicBoolean finalized = new AtomicBoolean(false);

    private List<Action> actions = new ArrayList<Action>();

    private Journal handler = JournalFactory.getDefault();

    //For unit tests
    void setHandler(Journal inHandler) {
        handler = inHandler;
    }

    public AtomicActions() {
        actionID = UUID.randomUUID();
    }

    AtomicActions(UUID actionID, List<Action> actions) {
        this.actionID = actionID;
        this.actions = actions;
        //mark as read only
        begun.set(true);
        finalized.set(true);
    }

    private void begin() throws IvoryException {
        handler.begin(this);
        LOG.info(actionID + "; START");
    }

    public void commit() throws IvoryException {
        if (!finalized.compareAndSet(false, true)) checkState();
        handler.commit(this);
        actions.clear();
        LOG.info(actionID + "; COMMIT");
    }

    public void rollback() throws IvoryException {
        if (!finalized.compareAndSet(false, true)) checkState();
        handler.rollback(this);
        //rollback actions in reverse order
        ListIterator<Action> itr = actions.listIterator(actions.size());
        while(itr.hasPrevious())
            itr.previous().rollback();
        actions.clear();
        LOG.info(actionID + "; ROLLBACK");
    }

    public void peform(Action action) throws IvoryException {
        if (begun.compareAndSet(false, true)) begin();
        checkState();
        handler.onAction(this, action);
        actions.add(action);
        LOG.info(actionID + "; DO " + action);
    }

    private void checkState() {
        if (finalized.get()) {
            throw new IllegalStateException("Already finalized " + actionID);
        }
    }

    public String getId() {
        return actionID.toString();
    }

    public List<Action> getUncommittedActions() throws IvoryException {
        return Collections.unmodifiableList(actions);
    }
}
