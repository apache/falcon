package org.apache.ivory.entity.parser;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.FeedHelper;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.expression.ExpressionHelper;

import java.util.Date;

public final class CrossEntityValidations {

    public static void validateInstanceRange(Process process, Input input, Feed feed) throws IvoryException {
        String clusterName = process.getCluster().getName();

        try {
            org.apache.ivory.entity.v0.feed.Validity feedValidity = FeedHelper.getCluster(feed, clusterName).getValidity();
            Date feedStart = EntityUtil.parseDateUTC(feedValidity.getStart());
            Date feedEnd = EntityUtil.parseDateUTC(feedValidity.getEnd());

            String instStartEL = input.getStart();
            String instEndEL = input.getEnd();
            ExpressionHelper evaluator = ExpressionHelper.get();

            ExpressionHelper.setReferenceDate(EntityUtil.parseDateUTC(process.getValidity().getStart()));
            Date instStart = evaluator.evaluate(instStartEL, Date.class);
            if (instStart.before(feedStart))
                throw new ValidationException("Start instance  " + instStartEL + " of feed " + feed.getName()
                        + " is before the start of feed " + feedValidity.getStart());

            Date instEnd = evaluator.evaluate(instEndEL, Date.class);
            if (instEnd.after(feedEnd))
                throw new ValidationException("End instance  " + instEndEL + " of feed " + feed.getName()
                        + " is before the start of feed " + feedValidity.getStart());

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

    public static void validateFeedRetentionPeriod(String startInstance, Feed feed,
                                                   String clusterName) throws IvoryException {

        String feedRetention = FeedHelper.getCluster(feed, clusterName).getRetention().getLimit().toString();
        ExpressionHelper evaluator = ExpressionHelper.get();

        Date now = new Date();
        ExpressionHelper.setReferenceDate(now);
        Date instStart = evaluator.evaluate(startInstance, Date.class);
        long feedDuration = evaluator.evaluate(feedRetention, Long.class);
        Date feedStart = new Date(now.getTime() - feedDuration);

        if (instStart.before(feedStart)) {
            throw new ValidationException("StartInstance :" + startInstance + " of process is out of range for Feed: " + feed.getName()
                    + "  in cluster: " + clusterName + "'s retention limit :" + feedRetention);
        }
    }

    // Mapping to oozie coord's dataset fields
    public static void validateInstance(Process process, Output output, Feed feed) throws IvoryException {
        String clusterName = process.getCluster().getName();

        try {
            org.apache.ivory.entity.v0.feed.Validity feedValidity = FeedHelper.getCluster(feed, clusterName).getValidity();
            Date feedStart = EntityUtil.parseDateUTC(feedValidity.getStart());
            Date feedEnd = EntityUtil.parseDateUTC(feedValidity.getEnd());

            String instEL = output.getInstance();
            ExpressionHelper evaluator = ExpressionHelper.get();
            ExpressionHelper.setReferenceDate(EntityUtil.parseDateUTC(process.getValidity().getStart()));
            Date inst = evaluator.evaluate(instEL, Date.class);
            if (inst.before(feedStart))
                throw new ValidationException("Instance  " + instEL + " of feed " + feed.getName() +
                        " is before the start of feed " + feedValidity.getStart());

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
        if (feed.getPartitions() == null || feed.getPartitions().getPartitions().isEmpty()
                || feed.getPartitions().getPartitions().size() < parts.length)
            throw new ValidationException("Partition specification in input " + input.getName() + " is wrong");
    }

    public static void validateFeedDefinedForCluster(Feed feed, String clusterName) throws IvoryException {
        if (FeedHelper.getCluster(feed, clusterName) == null)
            throw new ValidationException("Feed " + feed.getName() + " is not defined for cluster " + clusterName);
    }
}
