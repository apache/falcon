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

public class TransactionManager {
    private static ThreadLocal<AtomicActions> trans;
    
    public static void startTransaction() {
        if(trans == null) {
            trans = new ThreadLocal<AtomicActions>() {
                @Override
                protected AtomicActions initialValue() {
                    return new AtomicActions();
                }
            };

        }
    }
    
    public static String getTransactionId() {
        if(trans != null)
            return trans.get().getId();
        return null;
    }
    
    public static void performAction(Action action) throws IvoryException {
        if(trans != null) 
            trans.get().peform(action);
    }
    
    public static void rollback() throws IvoryException {
        if(trans != null)
            trans.get().rollback();
    }
    
    public static void commit() throws IvoryException {
        if(trans != null)
            trans.get().commit();
    }
}
