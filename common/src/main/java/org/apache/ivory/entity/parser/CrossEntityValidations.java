package org.apache.ivory.entity.parser;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.common.ELParser;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.entity.v0.process.Validity;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.SyncCoordDataset;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;

import java.util.Date;

public final class CrossEntityValidations {

    public static void validateInstanceRange(Process process, Input input, Feed feed) throws IvoryException {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-action-create");
        SyncCoordDataset ds = new SyncCoordDataset();
        SyncCoordAction appInst = new SyncCoordAction();
        String clusterName = process.getCluster().getName();
        configEvaluator(ds, appInst, eval, feed, clusterName, process.getValidity());

        try {

            org.apache.ivory.entity.v0.feed.Validity feedValidity = feed.getCluster(clusterName).getValidity();
            Date feedEnd = EntityUtil.parseDateUTC(feedValidity.getEnd());

            String instStartEL = input.getStartInstance();
            String instEndEL = input.getEndInstance();

            String instStartStr = CoordELFunctions.evalAndWrap(eval, "${elext:" + instStartEL + "}");
            if (instStartStr.equals(""))
                throw new ValidationException("Start instance  " + instStartEL + " of feed " + feed.getName()
                        + " is before the start of feed " + feedValidity.getStart());

            String instEndStr = CoordELFunctions.evalAndWrap(eval, "${elext:" + instEndEL + "}");
            if (instEndStr.equals(""))
                throw new ValidationException("End instance  " + instEndEL + " of feed " + feed.getName()
                        + " is before the start of feed " + feedValidity.getStart());

            Date instStart = EntityUtil.parseDateUTC(instStartStr);
            Date instEnd = EntityUtil.parseDateUTC(instEndStr);
            if (instEnd.before(instStart))
                throw new ValidationException("End instance " + instEndEL + " for feed " + feed.getName()
                        + " is before the start instance " + instStartEL);

            if (instEnd.after(feedEnd))
                throw new ValidationException("End instance " + instEndEL + " for feed " + feed.getName()
                        + " is after the end of feed " + feedValidity.getEnd());
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new IvoryException(e);
        }

    }

    public static void configEvaluator(SyncCoordDataset ds, SyncCoordAction appInst, ELEvaluator eval,
                                       Feed feed, String clusterName, Validity procValidity) throws IvoryException {
        try {
            org.apache.ivory.entity.v0.feed.Cluster cluster = feed.getCluster(clusterName);
            ds.setInitInstance(EntityUtil.parseDateUTC(cluster.getValidity().getStart()));
            ds.setFrequency(feed.getPeriodicity());
            ds.setTimeUnit(Frequency.valueOf(feed.getFrequency()).getTimeUnit());
            ds.setEndOfDuration(Frequency.valueOf(feed.getFrequency()).getEndOfDuration());
            ds.setTimeZone(DateUtils.getTimeZone(cluster.getValidity().getTimezone()));
            ds.setName(feed.getName());
            ds.setUriTemplate(feed.getLocations().get(LocationType.DATA).getPath());
            ds.setType("SYNC");
            ds.setDoneFlag("");

            appInst.setActualTime(EntityUtil.parseDateUTC(procValidity.getStart()));
            appInst.setNominalTime(EntityUtil.parseDateUTC(procValidity.getStart()));
            appInst.setTimeZone(EntityUtil.getTimeZone(procValidity.getTimezone()));
            appInst.setActionId("porcess@1");
            appInst.setName("process");

            eval.setVariable(OozieClient.USER_NAME, "test_user");
            eval.setVariable(OozieClient.GROUP_NAME, "test_group");
            CoordELFunctions.configureEvaluator(eval, ds, appInst);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    public static void validateFeedRetentionPeriod(String startInstance, Feed feed,
                                                   String clusterName) throws IvoryException {

        String feedRetention = feed.getCluster(clusterName).getRetention().getLimit();
        ELParser elParser = new ELParser();
        elParser.parseElExpression(startInstance);
        long requiredInputDuration = elParser.getRequiredInputDuration();
        elParser.parseOozieELExpression(feedRetention);
        long feedDuration = elParser.getFeedDuration();

        if (feedDuration - requiredInputDuration < 0) {
            throw new ValidationException("StartInstance :" + startInstance + " of process is out of range for Feed: " + feed.getName()
                    + "  in cluster: " + clusterName + "'s retention limit :" + feedRetention);
        }
    }

    // Mapping to oozie coord's dataset fields
    public static void validateInstance(Process process, Output output, Feed feed) throws IvoryException {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-action-create");
        SyncCoordDataset ds = new SyncCoordDataset();
        SyncCoordAction appInst = new SyncCoordAction();
        String clusterName = process.getCluster().getName();
        CrossEntityValidations.configEvaluator(ds, appInst, eval, feed, clusterName, process.getValidity());

        try {
            org.apache.ivory.entity.v0.feed.Validity feedValidity = feed.getCluster(clusterName).getValidity();
            Date feedEnd = EntityUtil.parseDateUTC(feedValidity.getEnd());

            String instEL = output.getInstance();
            String instStr = CoordELFunctions.evalAndWrap(eval, "${elext:" + instEL + "}");
            if (instStr.equals(""))
                throw new ValidationException("Instance  " + instEL + " of feed " + feed.getName() +
                        " is before the start of feed " + feedValidity.getStart());

            Date inst = EntityUtil.parseDateUTC(instStr);
            if (inst.after(feedEnd))
                throw new ValidationException("End instance " + instEL + " for feed " + feed.getName() +
                        " is after the end of feed " + feedValidity.getEnd());
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    public static void validateInputPartition(Input input, Feed feed) throws ValidationException {
        String[] parts = input.getPartition().split("/");
        if (feed.getPartitions() == null || feed.getPartitions().getPartition() == null
                || feed.getPartitions().getPartition().size() == 0
                || feed.getPartitions().getPartition().size() < parts.length)
            throw new ValidationException("Partition specification in input " + input.getName() + " is wrong");
    }

    public static void validateFeedDefinedForCluster(Feed feed, String clusterName) throws IvoryException {
        if (feed.getCluster(clusterName) == null)
            throw new ValidationException("Feed " + feed.getName() + " is not defined for cluster " + clusterName);
    }
}
