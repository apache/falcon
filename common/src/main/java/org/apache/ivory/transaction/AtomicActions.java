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

    private static Logger TRANLOG = Logger.getLogger("TRANSACTIONLOG");
    private static Logger LOG = Logger.getLogger(AtomicActions.class);

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

    public boolean isFinalized() {
        return finalized.get();
    }
    
    public boolean isBegun() {
        return begun.get();
    }
    
    public void begin() throws IvoryException {
        begun.set(true);
        handler.begin(this);
        TRANLOG.info(actionID + "; START");
    }

    public void commit() throws IvoryException {
        if (!finalized.compareAndSet(false, true)) checkState();
        for(Action action:actions) {
            action.commit();
            LOG.info("Action " + actionID + ":" + action.getName() + " - " + action.getCategory() + " committed");
        }
        actions.clear();
        handler.commit(this);
        TRANLOG.info(actionID + "; COMMIT");
    }

    public void rollback() throws IvoryException {
        if (!finalized.compareAndSet(false, true)) checkState();
        //rollback actions in reverse order
        ListIterator<Action> itr = actions.listIterator(actions.size());
        while(itr.hasPrevious()) {
            Action action = itr.previous();
            action.rollback();
            LOG.info("Action " + actionID + ":" + action.getName() + " - " + action.getCategory() + " rolled back");
        }
        actions.clear();
        handler.rollback(this);
        TRANLOG.info(actionID + "; ROLLBACK");
    }

    public void peform(Action action) throws IvoryException {
        checkState();
        handler.onAction(this, action);
        actions.add(action);
        TRANLOG.info(actionID + "; DO " + action);
    }

    private void checkState() {
        if (!begun.get())
            throw new IllegalStateException("Not begun " + actionID);
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
