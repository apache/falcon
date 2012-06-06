package org.apache.ivory.rerun.policy;

import java.util.Date;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Frequency;

public class FinalPolicy extends AbstractRerunPolicy {

    @Override
    public long getDelay(Frequency delay, int eventNumber) throws IvoryException {
        return 0;
    }

    @Override
    public long getDelay(Frequency delay, Date nominalTime, Date cutOffTime) throws IvoryException {
        return (cutOffTime.getTime() - nominalTime.getTime());
    }
}
