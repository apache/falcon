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

package org.apache.falcon.util;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Zookeeper based resource manager that coordinates multiple concurrent consumer processes (like Oozie Jobs) to
 * acquire named resources up to a specified limit collectively. If the request number of resources are not available,
 * the calling consumer job will wait (non-busy wait) until resources become available.
 */

public class ResourceManager {

    /* Default session timeout for Zookeeper */
    public static final int DEFAULT_SESSION_TIMEOUT = 5000;

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManager.class);

    /** Consumer ZNode status.
        ACTIVE - consumer is using the resources;
        QUEUED - consumer is waiting
     */

    enum STATUS {

        QUEUED('Q'),
        ACTIVE('A');

        private char val;

        STATUS(char status) {
            this.val = status;
        }

        public static STATUS fromVal(char status) {
            for(STATUS s : STATUS.values()) {
                if (s.val == status) {
                    return s;
                }
            }
            return null;
        }
    }

    private String zkHosts;
    private String zkBasePath;
    private ZooKeeper zk;
    private String resourceName;
    private Integer resourceLimit;
    private int sessionTimeout;

    public ResourceManager(String zkHosts, String resourceName, int resourceLimit, int sessionTimeout) {
        this.resourceName = resourceName;
        this.zkHosts = zkHosts;
        this.sessionTimeout = sessionTimeout;
        this.resourceLimit = Integer.valueOf(resourceLimit);
        zkBasePath = String.format("/falcon/DS/%s", this.resourceName);
    }

    public ResourceManager(String zkHosts, String resourceName, int resourceLimit) {
        this(zkHosts, resourceName, resourceLimit, DEFAULT_SESSION_TIMEOUT);
    }

    public static ResourceManager get(String hosts, String resourceName, int sessionTimeout)
        throws IOException, InterruptedException, KeeperException {
        ResourceManager rm = new ResourceManager(hosts, resourceName);
        ZooKeeper zk = connect(hosts, sessionTimeout);
        try {
            if (zk.exists(rm.zkBasePath, false) != null) {
                byte[] zdata = zk.getData(rm.zkBasePath, false, zk.exists(rm.zkBasePath, false));
                rm.resourceLimit = Integer.parseInt(new String(zdata));
                return rm;
            } else {
                throw new IOException(String.format("ZNode for resource name [%s] does not exists", resourceName));
            }
        } finally {
            zk.close();
        }
    }

    public static ResourceManager get(String hosts, String resourceName)
        throws IOException, InterruptedException, KeeperException {
        return get(hosts, resourceName, DEFAULT_SESSION_TIMEOUT);
    }

    public void initialize() throws IOException, InterruptedException, KeeperException {
        connect();
        try {
            if (zk.exists(zkBasePath, false) != null) {
                byte[] data = zk.getData(zkBasePath, false, zk.exists(zkBasePath, false));
                String exStr = String.format("Znode [%s] already exists with data [%s]", zkBasePath, new String(data));
                throw new IOException(exStr);
            }
            if (zk.exists("/falcon", false) == null) {
                zk.create("/falcon", "falcon".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists("/falcon/DS", false) == null) {
                zk.create("/falcon/DS", "falcon/DS".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String zNode = zk.create(zkBasePath, Integer.toString(resourceLimit).getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info("Created ZNode {}", zNode);
        } finally {
            disconnect();
        }
    }

    private ResourceManager(String hosts, String resourceName) {
        this(hosts, resourceName, 0, DEFAULT_SESSION_TIMEOUT);
    }

    private void connect() throws IOException, InterruptedException {
        this.zk = connect(this.zkHosts, this.sessionTimeout);
    }

    private void disconnect() throws InterruptedException {
        this.zk.close();
    }

    private static ZooKeeper connect(String hosts, int sessionTimeout) throws IOException, InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(hosts, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        connectedSignal.await();
        return zk;
    }

    boolean canScheduleConsumer(String basePath, String nodePath) throws Exception {
        ResourceQueue queue = getConsumerQueue(basePath);
        int pos = queue.findPosition(nodePath);
        if (pos < resourceLimit) {
            return true;
        }
        return false;
    }

    void wakeupWaitingConsumer(String basePath) throws Exception {
        // wakeup waiting consumers upto 'resourceLimit'
        ResourceQueue queue = getConsumerQueue(basePath);
        LOG.info("waking up waiting consumers.");
        Iterator<ResourceQueue.QueueElement> itr = queue.q.iterator();
        int i = 0;
        while((itr.hasNext()) && (i < resourceLimit)) {
            ResourceQueue.QueueElement e = itr.next();
            if (e.status == STATUS.QUEUED) {
                byte[] data = makeZNodeData(e.consumerId, STATUS.QUEUED);
                LOG.info("Waking up consumer {} with znode {} and set data to {}",
                        e.consumerId, e.childPath, new String(data));
                try {
                    zk.setData(e.childPath, data, e.version);
                } catch(KeeperException ex) {
                    String errStr = String.format("Exception while updating consumer ZNode [%s] data", e.childPath);
                    LOG.info(errStr, ex);
                }
                i++;
            }
        }
    }

    /**
     *  Query Zookeeper to get child znodes given the resource's ZK path.
     */

    ResourceQueue getConsumerQueue(String basePath) throws Exception {
        ResourceQueue queue = new ResourceQueue();
        List<String> children = null;
        final int retryTimes = 3;
        int retry = 0;
        while (retry++ < retryTimes) {
            try {
                children = zk.getChildren(basePath, false);
                if (children != null) {
                    break;
                }
            } catch(Exception e) {
                if (retry >=  retryTimes) {
                    String errStr = String.format("Error while getting children for ZNode [%s]", basePath);
                    LOG.info(errStr, e);
                    throw e;
                } else {
                    Thread.currentThread().sleep(1000);
                }
            }
        }
        if (children == null) {
            throw new Exception(String.format("Error while getting children for ZNode [%s]", basePath));
        }
        Stat stat = new Stat();
        for (String child : children) {
            try {
                String childPath = String.format("%s/%s", basePath, child);
                String childData = new String(zk.getData(childPath, false, stat));
                queue.add(child, childPath, childData, stat.getVersion());
            } catch(Exception e) {
                // ignore the exception because the znode may be deleted by the owner consumer that just finished
                String errStr = String.format("Ignoring exception while getting data for ZNode [%s]", basePath);
                LOG.info(errStr, e);
            }
        }
        return queue;
    }


    /**
     * Zookeeper watcher callback implementation.
     */

    class QueueWatcher implements Watcher {

        private String zkPathSelf;
        private String zkBasePath;
        private CountDownLatch latch;

        public QueueWatcher(String zkPath, String zkBasePath, CountDownLatch latch) {
            this.latch = latch;
            this.zkPathSelf = zkPath;
            this.zkBasePath = zkBasePath;
        }

        @Override
        public void process(WatchedEvent watchedEvent) {
            LOG.info("ZK callback : Processing event {} for znode {} self znode {}",
                watchedEvent.getType(), watchedEvent.getPath(), zkPathSelf);

            if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                if (watchedEvent.getPath().equals(zkPathSelf)) {
                    Stat stat = new Stat();
                    try {
                        String data = new String(zk.getData(watchedEvent.getPath(), false, stat));
                        if (STATUS.ACTIVE == getConsumerStatusFromZnode(data)) {
                            LOG.info("ZK callback : ACQUIRED resource for ZNode {} with Consumer ID {}",
                                zkPathSelf, getConsumerIdFromZnode(data));
                            latch.countDown();
                        } else {
                            // list the children for zkBasePath, sort and see if 'this' znode can be scheduled
                            zk.exists(zkPathSelf, this);
                            if (canScheduleConsumer(zkBasePath, zkPathSelf)) {
                                byte[] newData = makeZNodeData(getConsumerIdFromZnode(data), STATUS.ACTIVE);
                                Stat s = zk.setData(zkPathSelf, newData, stat.getVersion());
                                LOG.info("ZK callback : OK to ACQUIRE for ZNode {} consumerId {} "
                                    + "oldVersion {} newVersion {}", zkPathSelf, getConsumerIdFromZnode(data),
                                    stat.getVersion(), s.getVersion());
                            }
                        }
                    } catch(Exception e) {
                        String eStr = String.format("Exception while processing ZK callback znode [%s]", zkPathSelf);
                        LOG.info(eStr, e);
                    }
                }
            }
        }
    }

    /**
     * Get resource if available. otherwise wait until resources become available
     *
     * @param consumer
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */

    public String getResource(String consumer) throws IOException, InterruptedException, KeeperException {
        connect();
        try {
            CountDownLatch lock = new CountDownLatch(1);
            String zkConsumerNodeName = zk.create(makeConsumerZNodeName(zkBasePath),
                    null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            LOG.info("Created zNode {} for consumer {}", zkConsumerNodeName, consumer);
            Stat stat = new Stat();
            QueueWatcher watcher = new QueueWatcher(zkConsumerNodeName, zkBasePath, lock);
            zk.getData(zkConsumerNodeName, watcher, stat);
            byte[] data = makeZNodeData(consumer, STATUS.QUEUED);
            zk.setData(zkConsumerNodeName, data, stat.getVersion());
            LOG.info(String.format("setdata to znode [%s] = [%s] for consumer id [%s] and WAIT",
                    zkConsumerNodeName, new String(data), consumer));

            LOG.info("setdata to znode {} = {} for consumer id {} and WAIT",
                zkConsumerNodeName, new String(data), consumer);
            lock.await();
            LOG.info("got resource znode {} consumer id {}", zkConsumerNodeName, consumer);
            return zkConsumerNodeName;
        } finally {
            disconnect();
        }
    }

    /**
     * Release resources held by consumer specified by consumerId.
     * This call also wakes up one or more waiting consumers up to 'resourceLimit' number.
     *
     * @param consumerId
     * @return
     * @throws Exception
     */

    public String releaseResource(String consumerId) throws Exception {
        connect();
        try {
            ResourceQueue queue = getConsumerQueue(zkBasePath);
            Iterator<ResourceQueue.QueueElement> itr = queue.q.iterator();
            ResourceQueue.QueueElement e = null;
            while (itr.hasNext()) {
                e = itr.next();
                if (e.consumerId.equals(consumerId)) {
                    break;
                }
            }
            if (e != null) {
                LOG.info("Release resource for consumerId {} with znode {}", consumerId, e.childPath);
                zk.delete(e.childPath, e.version);
                wakeupWaitingConsumer(zkBasePath);
                return e.childPath;
            } else {
                String str = String.format("Resource Release unable to find zNode for consumerId [%s]", consumerId);
                LOG.error(str);
                throw new IOException(str);
            }
        } finally {
            disconnect();
        }
    }

    private static String getConsumerIdFromZnode(String data) {
        return data.split(":")[0];
    }

    private static STATUS getConsumerStatusFromZnode(String data) {
        return STATUS.fromVal(data.split(":")[1].charAt(0));
    }

    private static byte[] makeZNodeData(String consumerId, STATUS status) {
        return String.format("%s:%s", consumerId, status) .getBytes();
    }

    private static String makeConsumerZNodeName(String basePath) {
        return String.format("%s/Consumer-", basePath);
    }


    /**
     *  Queue that holds the znodes in sorted order.
     *  Znodes are sorted by status first and then by sequential number.
     */

    class ResourceQueue {

        class QueueElement {
            private String child;
            private String childPath;
            private STATUS status;
            private String consumerId;
            private int version;

            public QueueElement(String consumerId, String child, String childPath, STATUS status, int version) {
                this.consumerId = consumerId;
                this.child = child;
                this.status = status;
                this.version = version;
                this.childPath = childPath;
            }
        }

        private SortedSet<QueueElement> q;

        public ResourceQueue() {
            q = new TreeSet<>(new Comparator<QueueElement>() {
                @Override
                public int compare(QueueElement o1, QueueElement o2) {
                    String consumer1 = String.format("%s:%s", o1.status, o1.child);
                    String consumer2 = String.format("%s:%s", o2.status, o2.child);
                    return consumer1.compareTo(consumer2);
                }
            });
        }

        public void add(String child, String childPath, String zkNodeData, int version) {
            String consumerId = getConsumerIdFromZnode(zkNodeData);
            STATUS consumerStatus = getConsumerStatusFromZnode(zkNodeData);
            q.add(new QueueElement(consumerId, child, childPath, consumerStatus, version));
        }

        public int findPosition(String zkNodePath) {
            int pos = 0;
            Iterator<ResourceQueue.QueueElement> itr = q.iterator();
            while (itr.hasNext()) {
                if (itr.next().childPath.equals(zkNodePath)) {
                    break;
                }
                pos++;
            }
            return pos;
        }
    }

    public static void main(String[] args) throws Exception {
        String zkHost = "c6402.ambari.apache.org";
        String resourceName = "mysql-ds";
        int resourceLimit = 3;
        int numConsumers = 8;

        setup(zkHost, resourceName, resourceLimit);
        run(zkHost, resourceName, numConsumers);
    }

    public static void  setup(String zkHost, String resourceName, int resourceLimit) throws Exception {
        ResourceManager rm = new ResourceManager(zkHost, resourceName, resourceLimit);
        LOG.info("Creating a new Resource Manager with a resource Limit : " + resourceLimit);
        rm.initialize();
        LOG.info("Initialized Resource Manager");
    }

    public static void run(final String zkHosts, final String resourceName, int numConsumers) throws Exception {

        ResourceManager rm = ResourceManager.get(zkHosts, resourceName);
        LOG.info("Retrieved ResourceManager RM with max connection = {}", rm.resourceLimit);

        final CountDownLatch lock = new CountDownLatch(numConsumers);
        final Random random = new Random();
        Thread[] threads = new Thread[numConsumers];
        for (int i = 0; i < numConsumers; i++) {
            threads[i] = new Thread("CONSUMER-" + i) {
                @Override
                public void run() {
                    int delay = (5 + random.nextInt(16)) + 1000;
                    try {
                        ResourceManager rm = ResourceManager.get(zkHosts, resourceName);
                        String znode = rm.getResource(this.getName());
                        LOG.info("{} acquired lease znode {} work for {} seconds", getName(), znode, delay);
                        Thread.currentThread().sleep(delay);
                        znode = rm.releaseResource(getName());
                        LOG.info("{} released znode {}", getName(), znode);
                    } catch(Exception e) {
                        // ignore any exceptions
                        e.printStackTrace();
                    } finally {
                        lock.countDown();
                        LOG.info("{} is done.", getName());
                    }
                }
            };
            threads[i].start();
        }

        lock.await();
        LOG.info("ALL DONE");
    }
}

