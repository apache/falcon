package org.apache.falcon.service;

import org.apache.falcon.FalconException;

import java.util.Date;

/**
 * Created by praveen on 18/6/16.
 */
public interface FeedSLAAlert {
    void highSLAMissed(String feedName , String clusterName, Date nominalTime) throws FalconException;
}
