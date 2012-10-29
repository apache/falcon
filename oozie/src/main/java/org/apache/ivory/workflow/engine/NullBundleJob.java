package org.apache.ivory.workflow.engine;

import java.util.Date;
import java.util.List;

import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;

public class NullBundleJob implements BundleJob{

    @Override
    public String getAppPath() {
        return null;
    }

    @Override
    public String getAppName() {
        return null;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public String getConf() {
        return null;
    }

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public String getUser() {
        return null;
    }

    @Override
    public String getGroup() {
        return null;
    }

    @Override
    public String getConsoleUrl() {
        return null;
    }

    @Override
    public Date getStartTime() {
        return null;
    }

    @Override
    public Date getEndTime() {
        return null;
    }

    @Override
    public void setStatus(Status status) {
    }

    @Override
    public void setPending() {
    }

    @Override
    public void resetPending() {
    }

    @Override
    public Date getPauseTime() {
        return null;
    }

    @Override
    public String getExternalId() {
        return null;
    }

    @Override
    public Timeunit getTimeUnit() {
        return null;
    }

    @Override
    public int getTimeout() {
        return 0;
    }

    @Override
    public List<CoordinatorJob> getCoordinators() {
        return null;
    }

    @Override
    public Date getKickoffTime() {
        return null;
    }

    @Override
    public Date getCreatedTime() {
        return null;
    }

    @Override
    public String getAcl() {
        return null;
    }
}