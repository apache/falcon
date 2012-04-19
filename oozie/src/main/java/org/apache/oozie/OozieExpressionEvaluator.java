package org.apache.oozie;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.util.StartupProperties;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.SyncCoordDataset;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.TimeZone;

public class OozieExpressionEvaluator  {

    public OozieExpressionEvaluator() {
        try {
            init();
        } catch (IvoryException e) {
            throw new IvoryRuntimException(e);
        }
    }

    public static void init() throws IvoryException {
        String uri = StartupProperties.get().getProperty("config.oozie.conf.uri");
        System.setProperty(Services.OOZIE_HOME_DIR, uri);
        File confFile = new File(uri + "/conf");
        if (!confFile.exists() && !confFile.mkdirs())
            throw new IvoryException("Failed to create conf directory in path " + uri);

        InputStream instream = OozieExpressionEvaluator.class.getResourceAsStream("/oozie-site.xml");
        try {
            IOUtils.copyStream(instream, new FileOutputStream(uri + "/conf/oozie-site.xml"));
            Services services = new Services();
            services.getConf().set("oozie.services", "org.apache.oozie.service.ELService");
            services.init();
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    public Date evaluateInstance(String expression, Date date) throws IvoryException {
        ELEvaluator eval = Services.get().get(ELService.class).
                createEvaluator("coord-action-create");
        try {
            String dateStr = CoordELFunctions.evalAndWrap(eval, expression);
            if (dateStr == null || dateStr.isEmpty()) {
                return null;
            } else {
                return EntityUtil.parseDateUTC(dateStr);
            }
        } catch (Exception e) {
            throw new IvoryException("Unable to evaluate " + expression, e);
        }
    }

    private void configureEvaluator(ELEvaluator eval, Date date) throws IvoryException {
        try {
            SyncCoordDataset ds = new SyncCoordDataset();
            SyncCoordAction appInst = new SyncCoordAction();
            ds.setInitInstance(date);
            appInst.setActualTime(date);
            appInst.setNominalTime(date);
            appInst.setTimeZone(TimeZone.getTimeZone("UTC"));

            CoordELFunctions.configureEvaluator(eval, ds, appInst);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }
}
