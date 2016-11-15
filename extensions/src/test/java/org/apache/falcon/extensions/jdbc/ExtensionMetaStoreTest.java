package org.apache.falcon.extensions.jdbc;

import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.service.FalconJPAService;
import org.apache.falcon.tools.FalconStateStoreDBCLI;
import org.apache.falcon.util.StateStoreProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Created by praveen on 15/11/16.
 */
public class ExtensionMetaStoreTest {
    private static final String DB_BASE_DIR = "target/test-data/persistancedb";
    protected static String dbLocation = DB_BASE_DIR + File.separator + "data.db";
    protected static String url = "jdbc:derby:"+ dbLocation +";create=true";
    protected static final String DB_SQL_FILE = DB_BASE_DIR + File.separator + "out.sql";
    protected LocalFileSystem fs = new LocalFileSystem();
    protected EmbeddedCluster dfsCluster;
    protected Configuration conf = new Configuration();

    private static ExtensionMetaStore stateStore;
    private static FalconJPAService falconJPAService = FalconJPAService.get();

    protected int execDBCLICommands(String[] args) {
        return new FalconStateStoreDBCLI().run(args);
    }

    public void createDB(String file) {
        File sqlFile = new File(file);
        String[] argsCreate = { "create", "-sqlfile", sqlFile.getAbsolutePath(), "-run" };
        int result = execDBCLICommands(argsCreate);
        Assert.assertEquals(0, result);
        Assert.assertTrue(sqlFile.exists());

    }

    @BeforeClass
    public void setup() throws Exception{
        StateStoreProperties.get().setProperty(FalconJPAService.URL, url);
        Configuration localConf = new Configuration();
        fs.initialize(LocalFileSystem.getDefaultUri(localConf), localConf);
        fs.mkdirs(new Path(DB_BASE_DIR));
        createDB(DB_SQL_FILE);
        falconJPAService.init();
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        stateStore = new ExtensionMetaStore();
    }

    @BeforeMethod
    public void init() {
    }

    @Test
    public void dbOpertaions(){
        //insert
        stateStore.storeExtensionMetadataBean("test","test_location","custom","test_description");
        Assert.assertEquals(stateStore.getAllExtensions(),1);
    }
}
