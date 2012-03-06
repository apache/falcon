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
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SharedFileSystemJournal implements Journal {

    private static Logger LOG = Logger.getLogger(SharedFileSystemJournal.class);

    private static final Journal INSTANCE = new SharedFileSystemJournal();

    public static Journal get() {
        return INSTANCE;
    }

    private final File basePath;

    private SharedFileSystemJournal() {
        basePath = new File(StartupProperties.get().
                getProperty("fs.journal.path", "/tmp/ivory/journal"));
        if ((!basePath.exists() && !basePath.mkdirs()) ||
                (basePath.exists() && !basePath.canWrite())) {
             throw new RuntimeException("Unable to initialize journal @" +
                     basePath);
        }
    }

    //Support unit tests
    File getBasePath() {
        return basePath;
    }

    @Override
    public void begin(AtomicActions id) throws IvoryException {
        //do nothing
    }

    @Override
    public void commit(AtomicActions id) throws IvoryException {
        File actionFile = new File(basePath, id.getId());
        if (!actionFile.exists()) {
            throw new IllegalStateException("No file for transaction id: " + id);
        } else {
            if (actionFile.delete()) {
                LOG.warn("Unable to remove transaction " + id);
                actionFile.deleteOnExit();
            }
        }
    }

    @Override
    public void rollback(AtomicActions id) throws IvoryException {
        commit(id);
    }

    @Override
    public void onAction(AtomicActions id, Action action)
            throws IvoryException {

        File actionFile = new File(basePath, id.getId());
        try {
            BufferedWriter out = new BufferedWriter(
                    new FileWriter(actionFile,  true));
            out.write(action.toString());
            out.newLine();
            out.close();
        } catch (IOException e) {
            throw new IvoryException("Unable to add journal entry for " +
                    action + " corresponding to transaction " + id, e);
        }
    }

    @Override
    public List<AtomicActions> getUncommittedActions() throws IvoryException {
        List<AtomicActions> transactions = new ArrayList<AtomicActions>();
        for (File actionFile : basePath.listFiles()) {
            try {
                List<Action> actions = new ArrayList<Action>();
                UUID id = UUID.fromString(actionFile.getName());
                BufferedReader reader = new BufferedReader(new FileReader(actionFile));
                String line;
                while ((line = reader.readLine()) != null) {
                    Action action = Action.fromLine(line);
                    actions.add(action);
                }
                transactions.add(new AtomicActions(id, actions));
            } catch (Exception e) {
                LOG.warn("Not able to read journal entry " +
                        actionFile.getAbsolutePath(), e);
            }
        }
        return transactions;
    }
}
