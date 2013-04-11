package org.apache.falcon.rerun.policy;

import java.util.Date;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Frequency;

public class FinalPolicy extends AbstractRerunPolicy {

    @Override
    public long getDelay(Frequency delay, int eventNumber) throws FalconException {
        return 0;
    }

    @Override
    public long getDelay(Frequency delay, Date nominalTime, Date cutOffTime) throws FalconException {
        return (cutOffTime.getTime() - nominalTime.getTime());
    }
}
