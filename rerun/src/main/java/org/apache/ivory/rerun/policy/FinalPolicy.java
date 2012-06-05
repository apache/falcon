package org.apache.ivory.rerun.policy;

import java.util.Date;

import org.apache.ivory.IvoryException;

public class FinalPolicy extends AbstractRerunPolicy{

	@Override
	public long getDelay(String delayUnit, int delay, int eventNumber)
			throws IvoryException {
		return 0;
	}

	@Override
	public long getDelay(String delayUnit, int delay, Date nominalTime,
			Date cutOffTime) throws IvoryException {
		return  (cutOffTime.getTime() - nominalTime.getTime());
	}
}
