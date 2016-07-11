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
package org.apache.falcon.unit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * A Dummy implementation of RpcClientFactory that does not do RPC.
 * This is required as OozieClient tries to connect to RM via RPC to kill jobs which fails in local mode.
 */
public final class LocalFalconRPCClientFactory implements RpcClientFactory {

    private static LocalFalconRPCClientFactory self = new LocalFalconRPCClientFactory();

    @Override
    public Object getClient(Class<?> aClass, long l, InetSocketAddress inetSocketAddress, Configuration configuration) {
        return new LocalFalconApplicationClientProtocolImpl();
    }

    public static LocalFalconRPCClientFactory get() {
        return self;
    }

    private LocalFalconRPCClientFactory() {
    }


    @Override
    public void stopClient(Object o) {

    }

    /**
     * Dummy implementation of ApplicationClientProtocol that returns a empty list of applications.
     */
    public static class LocalFalconApplicationClientProtocolImpl implements ApplicationClientProtocol {

        public LocalFalconApplicationClientProtocolImpl() {

        }

        @Override
        public GetNewApplicationResponse getNewApplication(GetNewApplicationRequest getNewApplicationRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public SubmitApplicationResponse submitApplication(SubmitApplicationRequest submitApplicationRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public KillApplicationResponse forceKillApplication(KillApplicationRequest killApplicationRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetClusterMetricsResponse getClusterMetrics(GetClusterMetricsRequest getClusterMetricsRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest getClusterNodesRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest getQueueInfoRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetQueueUserAclsInfoResponse getQueueUserAcls(GetQueueUserAclsInfoRequest getQueueUserAclsInfoRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest
                moveApplicationAcrossQueuesRequest) throws YarnException, IOException {
            return null;
        }

        @Override
        public ReservationSubmissionResponse submitReservation(ReservationSubmissionRequest
                reservationSubmissionRequest) throws YarnException, IOException {
            return null;
        }

        @Override
        public ReservationUpdateResponse updateReservation(ReservationUpdateRequest reservationUpdateRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public ReservationDeleteResponse deleteReservation(ReservationDeleteRequest reservationDeleteRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetNodesToLabelsResponse getNodeToLabels(GetNodesToLabelsRequest getNodesToLabelsRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetLabelsToNodesResponse getLabelsToNodes(GetLabelsToNodesRequest getLabelsToNodesRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetClusterNodeLabelsResponse getClusterNodeLabels(GetClusterNodeLabelsRequest
                getClusterNodeLabelsRequest) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetApplicationReportResponse getApplicationReport(GetApplicationReportRequest
                getApplicationReportRequest) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetApplicationsResponse getApplications(GetApplicationsRequest getApplicationsRequest)
            throws YarnException, IOException {
            return GetApplicationsResponse.newInstance(new ArrayList<ApplicationReport>());
        }

        @Override
        public GetApplicationAttemptReportResponse getApplicationAttemptReport(GetApplicationAttemptReportRequest
                getApplicationAttemptReportRequest) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetApplicationAttemptsResponse getApplicationAttempts(GetApplicationAttemptsRequest
                getApplicationAttemptsRequest) throws YarnException, IOException {
            return null;
        }

        @Override
        public GetContainerReportResponse getContainerReport(GetContainerReportRequest getContainerReportRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetContainersResponse getContainers(GetContainersRequest getContainersRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public GetDelegationTokenResponse getDelegationToken(GetDelegationTokenRequest getDelegationTokenRequest)
            throws YarnException, IOException {
            return null;
        }

        @Override
        public RenewDelegationTokenResponse renewDelegationToken(RenewDelegationTokenRequest
                renewDelegationTokenRequest) throws YarnException, IOException {
            return null;
        }

        @Override
        public CancelDelegationTokenResponse cancelDelegationToken(CancelDelegationTokenRequest
                cancelDelegationTokenRequest) throws YarnException, IOException {
            return null;
        }
    }
}
