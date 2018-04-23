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
package org.apache.falcon.notification.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.event.DataEvent;
import org.apache.falcon.notification.service.impl.DataAvailabilityService;
import org.apache.falcon.notification.service.request.DataNotificationRequest;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.ID;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for DataNotificationService.
 */
public class DataAvailabilityServiceTest extends AbstractTestBase {

    private static NotificationHandler handler = Mockito.mock(NotificationHandler.class);
    private static DataAvailabilityService dataAvailabilityService = Mockito.spy(new DataAvailabilityService());
    private static final String BASE_PATH = "jail://testCluster:00/data/user";

    @BeforeClass
    public void setup() throws Exception {
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        storeEntity(EntityType.CLUSTER, "testCluster");
        dataAvailabilityService.init();
    }

    @Test
    public void testDataNotificationServiceWithVaryingRequests() throws IOException,
            FalconException, InterruptedException {
        FileSystem fs = FileSystem.get(conf);
        // invalid request
        org.apache.falcon.entity.v0.process.Process mockProcess = new Process();
        mockProcess.setName("test");
        EntityClusterID id = new EntityClusterID(mockProcess, "testCluster");

        DataNotificationRequest dataNotificationRequest = getDataNotificationRequest(new ArrayList<Path>(), id);

        dataAvailabilityService.register(dataNotificationRequest);
        Thread.sleep(1000);
        Mockito.verify(handler, Mockito.times(1)).onEvent(Mockito.any(DataEvent.class));
        ArgumentCaptor<DataEvent> captor = ArgumentCaptor.forClass(DataEvent.class);
        Mockito.verify(handler).onEvent(captor.capture());
        Assert.assertEquals(captor.getValue().getStatus(), DataEvent.STATUS.AVAILABLE);
        Assert.assertEquals(captor.getValue().getTarget(), dataNotificationRequest.getCallbackId());

        cleanupDir(fs, BASE_PATH);

        String path1 = BASE_PATH + "/" + "2015";
        String path2 = BASE_PATH + "/" + "2016";

        fs.create(new Path(path1));
        List<Path> paths = new ArrayList<>();
        paths.add(new Path(path1));
        paths.add(new Path(path2));

        // Adding paths and verifying its in queue
        dataNotificationRequest = getDataNotificationRequest(paths, id);
        dataAvailabilityService.register(dataNotificationRequest);
        Mockito.verify(handler, Mockito.times(1)).onEvent(Mockito.any(DataEvent.class));


        // create path and check availability status
        fs.create(new Path(path2));
        Thread.sleep(1000);
        Mockito.verify(handler, Mockito.times(2)).onEvent(captor.capture());
        Assert.assertEquals(captor.getValue().getStatus(), DataEvent.STATUS.AVAILABLE);
        Assert.assertEquals(captor.getValue().getTarget(), dataNotificationRequest.getCallbackId());


        // Adding one more path and verify Unavailable case
        String path3 = BASE_PATH + "/" + "2017";
        paths.add(new Path(path3));
        dataNotificationRequest = getDataNotificationRequest(paths, id);
        dataAvailabilityService.register(dataNotificationRequest);
        Thread.sleep(2000);
        Mockito.verify(handler, Mockito.times(3)).onEvent(captor.capture());
        Assert.assertEquals(captor.getValue().getStatus(), DataEvent.STATUS.UNAVAILABLE);
        Assert.assertEquals(captor.getValue().getTarget(), dataNotificationRequest.getCallbackId());

        dataNotificationRequest = getDataNotificationRequest(paths, id);
        dataAvailabilityService.register(dataNotificationRequest);
        dataAvailabilityService.unregister(dataNotificationRequest.getHandler(),
                dataNotificationRequest.getCallbackId());
        fs.create(new Path(path3));
        Thread.sleep(1000);
        // It wont notify as event was unregistered
        Mockito.verify(handler, Mockito.times(3)).onEvent(captor.capture());
    }

    private void cleanupDir(FileSystem fs, String basePath) throws IOException {
        fs.delete(new Path(basePath), true);
    }

    private DataNotificationRequest getDataNotificationRequest(List<Path> locations, ID id) {
        DataAvailabilityService.DataRequestBuilder dataRequestBuilder =
                new DataAvailabilityService.DataRequestBuilder(handler, id);
        dataRequestBuilder.setPollingFrequencyInMillis(20).setCluster("testCluster")
                .setTimeoutInMillis(100).setLocations(locations);
        return dataRequestBuilder.build();
    }

}
