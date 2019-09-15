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
package org.apache.falcon.predicate;

import org.apache.falcon.FalconException;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.event.DataEvent;
import org.apache.falcon.notification.service.event.Event;
import org.apache.falcon.notification.service.event.EventType;
import org.apache.falcon.notification.service.event.RerunEvent;
import org.apache.falcon.notification.service.event.TimeElapsedEvent;
import org.apache.falcon.state.ID;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents the gating condition for which an instance is waiting before it is scheduled.
 * This will be serialized and stored in state store.
 */
public class Predicate implements Serializable {



    /**
     * Type of predicate, currently data and time are supported.
     */
    public enum TYPE {
        DATA,
        TIME,
        JOB_COMPLETION,
        RE_RUN
    }

    private final TYPE type;

    // A key-value pair of clauses that need make this predicate.
    private Map<String, Comparable> clauses = new TreeMap<>();

    // Id for a predicate used for comparison.
    private String id;

    // A generic "any" object that can be used when a particular key is allowed to have any value.
    public static final Comparable<? extends Serializable> ANY = new Any();

    /**
     * @return type of predicate
     */
    public TYPE getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    /**
     * @param key
     * @return the value corresponding to the key
     */
    public Comparable getClauseValue(String key) {
        return clauses.get(key);
    }

    /**
     * Compares this predicate with the supplied predicate.
     *
     * @param suppliedPredicate
     * @return true, if the clauses of the predicates match. false, otherwise.
     */
    public boolean evaluate(Predicate suppliedPredicate) {
        if (type != suppliedPredicate.getType()) {
            return false;
        }
        boolean eval = true;
        // Iterate over each clause and ensure it matches the clauses of this predicate.
        for (Map.Entry<String, Comparable> entry : suppliedPredicate.getClauses().entrySet()) {
            eval = eval && matches(entry.getKey(), entry.getValue());
            if (!eval) {
                return false;
            }
        }
        return true;
    }

    // Compares the two values of a key.
    private boolean matches(String lhs, Comparable<? extends Serializable> rhs) {
        if (clauses.containsKey(lhs) && clauses.get(lhs) != null
                && rhs != null) {
            if (clauses.get(lhs).equals(ANY) || rhs.equals(ANY)) {
                return true;
            } else {
                return clauses.get(lhs).compareTo(rhs) == 0;
            }
        }
        return false;
    }

    /**
     * @param type of predicate
     */
    public Predicate(TYPE type) {
        this.type = type;
        this.id = this.type + String.valueOf(System.currentTimeMillis());
    }

    /**
     * @return the name-value pairs that make up the clauses of this predicate.
     */
    public Map<String, Comparable> getClauses() {
        return clauses;
    }

    /**
     * @param lhs - The key in the key-value pair of a clause
     * @param rhs - The value in the key-value pair of a clause
     * @return This instance
     */
    Predicate addClause(String lhs, Comparable<? extends Serializable> rhs) {
        clauses.put(lhs, rhs);
        return this;
    }

    /**
     * Creates a Predicate of Type TIME.
     *
     * @param start
     * @param end
     * @param instanceTime
     * @return
     */
    public static Predicate createTimePredicate(long start, long end, long instanceTime) {
        return new Predicate(TYPE.TIME)
                .addClause("start", (start < 0) ? ANY : start)
                .addClause("end", (end < 0) ? ANY : end)
                .addClause("instanceTime", (instanceTime < 0) ? ANY : instanceTime);
    }

    /**
     * Creates a predicate of type DATA.
     *
     * @param noOfPaths Lengths of paths to check
     * @return
     */
    public static Predicate createDataPredicate(int noOfPaths) {
        return new Predicate(TYPE.DATA)
                .addClause("numpaths", noOfPaths);
    }

    /**
     * Creates a predicate of type JOB_COMPLETION.
     *
     * @param handler
     * @param id
     * @param parallelInstances
     * @return
     */
    public static Predicate createJobCompletionPredicate(NotificationHandler handler, ID id, int parallelInstances) {
        return new Predicate(TYPE.JOB_COMPLETION)
                .addClause("instanceId", id.toString())
                .addClause("handler", handler.getClass().getName())
                .addClause("parallelInstances", parallelInstances);
    }

    /**
     * Creates a predicate of type Rerun.
     * @param instanceTime
     * @return
     */
    public static Predicate createRerunPredicate(long instanceTime) {
        return new Predicate(TYPE.RE_RUN)
                .addClause("instanceTime", (instanceTime < 0) ? ANY : instanceTime);
    }

    /**
     * Creates a predicate from an event based on the event source and values in the event.
     *
     * @param event
     * @return
     * @throws FalconException
     */
    public static Predicate getPredicate(Event event) throws FalconException {
        if (event.getType() == EventType.DATA_AVAILABLE) {
            DataEvent dataEvent = (DataEvent) event;
            if (dataEvent.getDataLocations() != null) {
                return createDataPredicate(dataEvent.getDataLocations().size());
            } else {
                throw new FalconException("Event does not have enough data to create a predicate");
            }
        } else if (event.getType() == EventType.TIME_ELAPSED) {
            TimeElapsedEvent timeEvent = (TimeElapsedEvent) event;
            if (timeEvent.getStartTime() != null && timeEvent.getEndTime() != null) {
                long instanceTime = (timeEvent.getInstanceTime() == null)? -1 : timeEvent.getInstanceTime().getMillis();
                return Predicate.createTimePredicate(timeEvent.getStartTime().getMillis(),
                        timeEvent.getEndTime().getMillis(), instanceTime);
            } else {
                throw new FalconException("Event does not have enough data to create a predicate");
            }

        } else if (event.getType() == EventType.RE_RUN) {
            RerunEvent rerunEvent = (RerunEvent) event;
            if (rerunEvent.getInstanceTime() != null) {
                return Predicate.createRerunPredicate(rerunEvent.getInstanceTime().getMillis());
            } else {
                throw new FalconException("Event does not have enough data to create a predicate");
            }
        } else {
            throw new FalconException("Unhandled event type " + event.getType());
        }
    }

    /**
     * An "Any" class that returns '0' when compared to any other object.
     */
    private static class Any implements Comparable, Serializable {
        @Override
        public int compareTo(Object o) {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    public static boolean isEqualAwaitingPredicates(List<Predicate> thisAwaitingPredicates,
                                              List<Predicate> otherAwaitingPredicates) {
        if (thisAwaitingPredicates == null && otherAwaitingPredicates == null) {
            return true;
        } else if (thisAwaitingPredicates != null && otherAwaitingPredicates != null) {
            if (thisAwaitingPredicates.size() != otherAwaitingPredicates.size()) {
                return false;
            }
            Collections.sort(thisAwaitingPredicates, new PredicateComparator());
            Collections.sort(otherAwaitingPredicates, new PredicateComparator());

            Iterator<Predicate> thisIterator = thisAwaitingPredicates.iterator();
            Iterator<Predicate> otherIterator = otherAwaitingPredicates.iterator();

            while (thisIterator.hasNext()) {
                if (!thisIterator.next().evaluate(otherIterator.next())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    static class PredicateComparator implements Serializable, Comparator<Predicate> {
        @Override
        public int compare(Predicate o1, Predicate o2) {
            return o1.getId().compareTo(o2.getId());
        }
    }
}
