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

package org.apache.falcon.regression.Entities;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.log4j.Logger;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** Class for representing a falcon recipe. */
public final class RecipeMerlin {
    private static final Logger LOGGER = Logger.getLogger(RecipeMerlin.class);
    private static final String WORKFLOW_PATH_KEY = "falcon.recipe.workflow.path";
    private static final String RECIPE_NAME_KEY = "falcon.recipe.name";
    private static final String WRITE_DIR =
        Config.getProperty("recipe.location", "/tmp/falcon-recipe");

    private String template;
    private AbstractFileConfiguration properties;
    private String workflow;
    private ClusterMerlin recipeCluster;
    private ClusterMerlin srcCluster;
    private ClusterMerlin tgtCluster;


    public ClusterMerlin getRecipeCluster() {
        return recipeCluster;
    }

    public ClusterMerlin getSrcCluster() {
        return srcCluster;
    }

    public ClusterMerlin getTgtCluster() {
        return tgtCluster;
    }

    public FalconCLI.RecipeOperation getRecipeOperation() {
        return recipeOperation;
    }

    private FalconCLI.RecipeOperation recipeOperation;

    private RecipeMerlin() {
    }

    public String getName() {
        return properties.getString(RECIPE_NAME_KEY);
    }

    public void setUniqueName(String prefix) {
        properties.setProperty(RECIPE_NAME_KEY, prefix + UUID.randomUUID().toString().split("-")[0]);
    }

    public String getSourceDir() {
        return properties.getString("drSourceDir");
    }

    public RecipeMerlin withSourceDir(final String srcDir) {
        properties.setProperty("drSourceDir", srcDir);
        return this;
    }

    public String getTargetDir() {
        return properties.getString("drTargetDir");
    }

    public RecipeMerlin withTargetDir(final String tgtDir) {
        properties.setProperty("drTargetDir", tgtDir);
        return this;
    }

    public String getSourceDb() {
        return StringUtils.join(properties.getStringArray("sourceDatabase"), ',');
    }

    public RecipeMerlin withSourceDb(final String srcDatabase) {
        properties.setProperty("sourceDatabase", srcDatabase);
        return this;
    }

    public String getSourceTable() {
        return StringUtils.join(properties.getStringArray("sourceTable"), ',');
    }

    public RecipeMerlin withSourceTable(final String tgtTable) {
        properties.setProperty("sourceTable", tgtTable);
        return this;
    }

    public RecipeMerlin withSourceCluster(ClusterMerlin sourceCluster) {
        this.srcCluster = sourceCluster;
        if (recipeOperation == FalconCLI.RecipeOperation.HDFS_REPLICATION) {
            properties.setProperty("drSourceClusterFS", sourceCluster.getInterfaceEndpoint(Interfacetype.WRITE));
        } else {
            properties.setProperty("sourceCluster", sourceCluster.getName());
            properties.setProperty("sourceMetastoreUri", sourceCluster.getProperty("hive.metastore.uris"));
            properties.setProperty("sourceHiveServer2Uri", sourceCluster.getProperty("hive.server2.uri"));
            //properties.setProperty("sourceServicePrincipal",
            //    sourceCluster.getProperty("hive.metastore.kerberos.principal"));
            properties.setProperty("sourceStagingPath", sourceCluster.getLocation("staging"));
            properties.setProperty("sourceNN", sourceCluster.getInterfaceEndpoint(Interfacetype.WRITE));
            properties.setProperty("sourceRM", sourceCluster.getInterfaceEndpoint(Interfacetype.EXECUTE));
        }
        return this;
    }

    public RecipeMerlin withTargetCluster(ClusterMerlin targetCluster) {
        this.tgtCluster = targetCluster;
        if (recipeOperation == FalconCLI.RecipeOperation.HDFS_REPLICATION) {
            properties.setProperty("drTargetClusterFS", targetCluster.getInterfaceEndpoint(Interfacetype.WRITE));
        } else {
            properties.setProperty("targetCluster", targetCluster.getName());
            properties.setProperty("targetMetastoreUri", targetCluster.getProperty("hive.metastore.uris"));
            properties.setProperty("targetHiveServer2Uri", targetCluster.getProperty("hive.server2.uri"));
            //properties.setProperty("targetServicePrincipal",
            //    targetCluster.getProperty("hive.metastore.kerberos.principal"));
            properties.setProperty("targetStagingPath", targetCluster.getLocation("staging"));
            properties.setProperty("targetNN", targetCluster.getInterfaceEndpoint(Interfacetype.WRITE));
            properties.setProperty("targetRM", targetCluster.getInterfaceEndpoint(Interfacetype.EXECUTE));
        }
        return this;
    }

    public RecipeMerlin withRecipeCluster(ClusterMerlin paramRecipeCluster) {
        this.recipeCluster = paramRecipeCluster;
        properties.setProperty("falcon.recipe.cluster.name", paramRecipeCluster.getName());
        properties.setProperty("falcon.recipe.cluster.hdfs.writeEndPoint",
            paramRecipeCluster.getInterfaceEndpoint(Interfacetype.WRITE));
        return this;
    }

    public RecipeMerlin withValidity(final String start, final String end) {
        properties.setProperty("falcon.recipe.cluster.validity.start", start);
        properties.setProperty("falcon.recipe.cluster.validity.end", end);
        return this;
    }

    public String getValidityStart() {
        return properties.getString("falcon.recipe.cluster.validity.start");
    }

    public String getValidityEnd() {
        return properties.getString("falcon.recipe.cluster.validity.end");
    }

    public RecipeMerlin withFrequency(final Frequency frequency) {
        properties.setProperty("falcon.recipe.process.frequency", frequency.toString());
        return this;
    }

    public Frequency getFrequency() {
        return Frequency.fromString(properties.getString("falcon.recipe.process.frequency"));
    }

    public String getMaxEvents() {
        return properties.getString("maxEvents");
    }

    public String getReplicationMaxMaps() {
        return properties.getString("replicationMaxMaps");
    }

    public String getDistCpMaxMaps() {
        return properties.getString("distcpMaxMaps");
    }

    public String getMapBandwidth() {
        return properties.getString("distcpMapBandwidth");
    }

    public Retry getRetry() {
        final int retryAttempts = properties.getInt("falcon.recipe.retry.attempts");
        final String retryDelay = properties.getString("falcon.recipe.retry.delay");
        final String retryPolicy = properties.getString("falcon.recipe.retry.policy");

        Retry retry = new Retry();
        retry.setAttempts(retryAttempts);
        retry.setDelay(Frequency.fromString(retryDelay));
        retry.setPolicy(PolicyType.fromValue(retryPolicy));
        return retry;
    }

    public ACL getAcl() {
        ACL acl = new ACL();
        acl.setOwner(properties.getString("falcon.recipe.acl.owner"));
        acl.setGroup(properties.getString("falcon.recipe.acl.group"));
        acl.setPermission(properties.getString("falcon.recipe.acl.permission"));
        return acl;
    }


    /**
     * Read recipe from a given directory. Expecting that recipe will follow these conventions.
     * <br> 1. properties file will have .properties extension
     * <br> 2. template file will have end with -template.xml
     * <br> 3. workflow file will have end with -workflow.xml
     * @param readPath the location from where recipe will be read
     * @param recipeOperation operation of this recipe
     */
    public static RecipeMerlin readFromDir(final String readPath,
                                           FalconCLI.RecipeOperation recipeOperation) {
        Assert.assertTrue(StringUtils.isNotEmpty(readPath), "readPath for recipe can't be empty");
        Assert.assertNotNull(recipeOperation, "readPath for recipe can't be empty");
        RecipeMerlin instance = new RecipeMerlin();
        instance.recipeOperation = recipeOperation;
        LOGGER.info("Loading recipe from directory: " + readPath);
        File directory = null;
        try {
            directory = new File(RecipeMerlin.class.getResource("/" + readPath).toURI());
        } catch (URISyntaxException e) {
            Assert.fail("could not find dir: " + readPath);
        }
        final Collection<File> propertiesFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*\\.properties"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(propertiesFiles.size(), 1,
            "Expecting only one property file at: " + readPath +" found: " + propertiesFiles);
        try {
            instance.properties =
                new PropertiesConfiguration(propertiesFiles.iterator().next());
        } catch (ConfigurationException e) {
            Assert.fail("Couldn't read recipe's properties file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        instance.properties.setFileName(null); //prevent accidental overwrite of template
        //removing defaults - specific test need to supplied this
        instance.properties.clearProperty("sourceDatabase");
        instance.properties.clearProperty("sourceTable");
        instance.properties.clearProperty("targetDatabase");
        instance.properties.clearProperty("targetTable");
        instance.properties.setProperty("falcon.recipe.acl.owner", MerlinConstants.CURRENT_USER_NAME);
        instance.properties.setProperty("falcon.recipe.acl.group", MerlinConstants.CURRENT_USER_GROUP);
        instance.properties.setProperty("falcon.recipe.acl.permission", "*");

        final Collection<File> templatesFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*-template\\.xml"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(templatesFiles.size(), 1,
            "Expecting only one template file at: " + readPath + " found: " + templatesFiles);
        try {
            instance.template =
                FileUtils.readFileToString(templatesFiles.iterator().next());
        } catch (IOException e) {
            Assert.fail("Couldn't read recipe's template file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }

        final Collection<File> workflowFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*-workflow\\.xml"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(workflowFiles.size(), 1,
            "Expecting only one workflow file at: " + readPath + " found: " + workflowFiles);
        try {
            instance.workflow = FileUtils.readFileToString(workflowFiles.iterator().next());
        } catch (IOException e) {
            Assert.fail("Couldn't read recipe's workflow file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        return instance;
    }

    /**
     * Write recipe.
     */
    private void write() {
        final String templateFileLocation = OSUtil.concat(WRITE_DIR, getName() + "-template.xml");
        try {
            Assert.assertNotNull(templateFileLocation,
                "Write location for template file is unexpectedly null.");
            FileUtils.writeStringToFile(new File(templateFileLocation), template);
        } catch (IOException e) {
            Assert.fail("Couldn't write recipe's template file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }

        final String workflowFileLocation = OSUtil.concat(WRITE_DIR, getName() + "-workflow.xml");
        try {
            Assert.assertNotNull(workflowFileLocation,
                "Write location for workflow file is unexpectedly null.");
            FileUtils.writeStringToFile(new File(workflowFileLocation), workflow);
        } catch (IOException e) {
            Assert.fail("Couldn't write recipe's workflow file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        properties.setProperty(WORKFLOW_PATH_KEY, workflowFileLocation);
        properties.setProperty("falcon.recipe.workflow.name", getName() + "-workflow");

        final String propFileLocation = OSUtil.concat(WRITE_DIR, getName() + ".properties");
        try {
            Assert.assertNotNull(propFileLocation,
                "Write location for properties file is unexpectedly null.");
            properties.save(new File(propFileLocation));
        } catch (ConfigurationException e) {
            Assert.fail("Couldn't write recipe's process file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * Get submission command.
     */
    public List<String> getSubmissionCommand() {
        write();
        final List<String> cmd = new ArrayList<>();
        Collections.addAll(cmd, "recipe", "-name", getName(),
            "-operation", recipeOperation.toString());
        return cmd;
    }

    /**
     * Set tags for recipe.
     */
    public List<String> getTags() {
        final String tagsStr = properties.getString("falcon.recipe.tags");
        if (StringUtils.isEmpty(tagsStr)) {
            return new ArrayList<>();
        }
        return Arrays.asList(tagsStr.split(","));
    }

    /**
     * Set tags for recipe.
     */
    public void setTags(List<String> tags) {
        properties.setProperty("falcon.recipe.tags", StringUtils.join(tags, ','));
    }
}
