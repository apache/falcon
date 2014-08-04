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

package org.apache.falcon.regression.ui.pages;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.Point;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ProcessPage extends EntityPage<Process> {

    private static final Logger logger = Logger.getLogger(ProcessPage.class);
    private boolean isLineageOpened = false;

    private final static String INSTANCES_PANEL = "//div[@id='panel-instance']//span";
    private final static String INSTANCE_STATUS_TEMPLATE = INSTANCES_PANEL + "[contains(..,'%s')]";
    private final static String LINEAGE_LINK_TEMPLATE =
        "//a[@class='lineage-href' and @data-instance-name='%s']";

    //Lineage information xpaths
    private static final String CLOSE_LINEAGE_LINK_TEMPLATE =
        "//body[@class='modal-open']//button[contains(., 'Close')]";
    private static final String LINEAGE_MODAL = "//div[@id='lineage-modal']";
    private static final String SVG_ELEMENT = "//*[name() = 'svg']/*[name()='g']/*[name()='g']";
    private static final String VERTICES_BLOCKS = SVG_ELEMENT + "[not(@class='lineage-link')]";
    private static final String VERTICES_TEXT = VERTICES_BLOCKS +
        "//div[@class='lineage-node-text']";
    private static final String EDGE = SVG_ELEMENT + "[@class='lineage-link']//*[name()='path']";
    private static final String CIRCLE = "//*[name() = 'circle']";
    private static final String VERTICES = VERTICES_BLOCKS + CIRCLE;
    private static final String VERTEX_BLOCK_TEMPLATE = VERTICES_BLOCKS + "[contains(., '%s')]";
    private static final String VERTEX_TEMPLATE = VERTEX_BLOCK_TEMPLATE + CIRCLE;

    private static final String LINEAGE_INFO_PANEL_LIST = "//div[@id='lineage-info-panel']" +
        "//div[@class='col-md-3']";

    private static final String LINEAGE_TITLE = LINEAGE_MODAL + "//div[@class='modal-header']/h4";

    private static final String LINEAGE_LEGENDS_BLOCK = LINEAGE_MODAL +
        "//div[@class='modal-body']/div[ul[@class='lineage-legend']]";
    private static final String LINEAGE_LEGENDS_TITLE = LINEAGE_LEGENDS_BLOCK + "/h4";
    private static final String LINEAGE_LEGENDS_ELEMENTS = LINEAGE_LEGENDS_BLOCK + "/ul/li";

    public ProcessPage(WebDriver driver, ColoHelper helper, String entityName) {
        super(driver, helper, EntityType.PROCESS, Process.class, entityName);
    }

    /**
     * @param nominalTime particular instance of process, defined by it's start time
     */
    public void openLineage(String nominalTime) {
        waitForElement(String.format(LINEAGE_LINK_TEMPLATE, nominalTime), DEFAULT_TIMEOUT,
            "Lineage button didn't appear");
        logger.info("Working with instance: " + nominalTime);
        WebElement lineage =
            driver.findElement(By.xpath(String.format(LINEAGE_LINK_TEMPLATE, nominalTime)));
        logger.info("Opening lineage...");
        lineage.click();
        waitForElement(VERTICES, DEFAULT_TIMEOUT, "Circles not found");
        waitForDisplayed(LINEAGE_TITLE, DEFAULT_TIMEOUT, "Lineage title not found");
        isLineageOpened = true;
    }

    public void closeLineage() {
        logger.info("Closing lineage...");
        if (isLineageOpened) {
            WebElement close = driver.findElement(By.xpath(CLOSE_LINEAGE_LINK_TEMPLATE));
            close.click();
            isLineageOpened = false;
            waitForDisappear(CLOSE_LINEAGE_LINK_TEMPLATE, DEFAULT_TIMEOUT,
                "Lineage didn't disappear");
        }
    }

    @Override
    public void refresh() {
        super.refresh();
        isLineageOpened = false;
    }

    /**
     * @return map with instances names and their nominal start time
     */
    public HashMap<String, List<String>> getAllVertices() {
        logger.info("Getting all vertices from lineage graph...");
        HashMap<String, List<String>> map = null;
        if (isLineageOpened) {
            waitForElement(VERTICES_TEXT, DEFAULT_TIMEOUT,
                "Vertices blocks with names not found");
            List<WebElement> blocks = driver.findElements(By.xpath(VERTICES_TEXT));
            logger.info(blocks.size() + " elements found");
            map = new HashMap<String, List<String>>();
            for (WebElement block : blocks) {
                waitForElement(block, ".[contains(.,'/')]", DEFAULT_TIMEOUT,
                    "Expecting text to contain '/' :" + block.getText());
                String text = block.getText();
                logger.info("Vertex: " + text);
                String[] separate = text.split("/");
                String name = separate[0];
                String nominalTime = separate[1];
                if (map.containsKey(name)) {
                    map.get(name).add(nominalTime);
                } else {
                    List<String> instances = new ArrayList<String>();
                    instances.add(nominalTime);
                    map.put(name, instances);
                }
            }
        }
        return map;
    }

    /**
     * @return list of all vertices names
     */
    public List<String> getAllVerticesNames() {
        logger.info("Getting all vertices names from lineage graph...");
        List<String> list = new ArrayList<String>();
        if (isLineageOpened) {
            waitForElement(CLOSE_LINEAGE_LINK_TEMPLATE, DEFAULT_TIMEOUT,
                "Close Lineage button not found");
            waitForElement(VERTICES_BLOCKS, DEFAULT_TIMEOUT,
                "Vertices not found");
            List<WebElement> blocks = driver.findElements(By.xpath(VERTICES_BLOCKS));
            logger.info(blocks.size() + " elements found");
            for (WebElement block : blocks) {
                list.add(block.getText());
            }
        }
        logger.info("Vertices: " + list);
        return list;
    }

    /**
     * Vertex is defined by it's entity name and particular time of it's creation
     */
    public void clickOnVertex(String entityName, String nominalTime) {
        logger.info("Clicking on vertex " + entityName + '/' + nominalTime);
        if (isLineageOpened) {
            WebElement circle = driver.findElement(By.xpath(String.format(VERTEX_TEMPLATE,
                entityName + '/' + nominalTime)));
            Actions builder = new Actions(driver);
            builder.click(circle).build().perform();
            TimeUtil.sleepSeconds(0.5);
        }
    }

    /**
     * @return map of parameters from info panel and their values
     */
    public HashMap<String, String> getPanelInfo() {
        logger.info("Getting info panel values...");
        HashMap<String, String> map = null;
        if (isLineageOpened) {
            //check if vertex was clicked
            waitForElement(LINEAGE_INFO_PANEL_LIST, DEFAULT_TIMEOUT, "Info panel not found");
            List<WebElement> infoBlocks = driver.findElements(By.xpath(LINEAGE_INFO_PANEL_LIST));
            logger.info(infoBlocks.size() + " values found");
            map = new HashMap<String, String>();
            for (WebElement infoBlock : infoBlocks) {
                String text = infoBlock.getText();
                String[] values = text.split("\n");
                map.put(values[0], values[1]);
            }
        }
        logger.info("Values: " + map);
        return map;
    }

    /**
     * @return map of legends as key and their names on UI as values
     */
    public HashMap<String, String> getLegends() {
        HashMap<String, String> map = null;
        if (isLineageOpened) {
            map = new HashMap<String, String>();
            List<WebElement> legends = driver.findElements(By.xpath(LINEAGE_LEGENDS_ELEMENTS));
            for (WebElement legend : legends) {
                String value = legend.getText();
                String elementClass = legend.getAttribute("class");
                map.put(elementClass, value);
            }
        }
        return map;
    }

    /**
     * @return the main title of Lineage UI
     */
    public String getLineageTitle() {
        logger.info("Getting Lineage title...");
        if (isLineageOpened) {
            return driver.findElement(By.xpath(LINEAGE_TITLE)).getText();
        } else return null;
    }

    /**
     * @return the name of legends block
     */
    public String getLegendsTitle() {
        logger.info("Getting Legends title...");
        if (isLineageOpened) {
            return driver.findElement(By.xpath(LINEAGE_LEGENDS_TITLE)).getText();
        } else return null;
    }

    /**
     * @return list of edges present on UI. Each edge presented as two 2d points - beginning and
     * the end of the edge.
     */
    public List<Point[]> getEdgesFromGraph() {
        List<Point[]> pathsEndpoints = null;
        logger.info("Getting edges from lineage graph...");
        if (isLineageOpened) {
            pathsEndpoints = new ArrayList<Point[]>();
            List<WebElement> paths = driver.findElements(By.xpath(EDGE));
            logger.info(paths.size() + " edges found");
            for (WebElement path : paths) {
                String[] coordinates = path.getAttribute("d").split("[MLC,]");
                int x = 0, y, i = 0;
                while (i < coordinates.length) {
                    if (!coordinates[i].isEmpty()) {
                        x = (int) Double.parseDouble(coordinates[i]);
                        break;
                    } else {
                        i++;
                    }
                }
                y = (int) Double.parseDouble(coordinates[i + 1]);
                Point startPoint = new Point(x, y);
                x = (int) Math.round(Double.parseDouble(coordinates[coordinates.length - 2]));
                y = (int) Math.round(Double.parseDouble(coordinates[coordinates.length - 1]));
                Point endPoint = new Point(x, y);
                logger.info("Edge " + startPoint + '→' + endPoint);
                pathsEndpoints.add(new Point[]{startPoint, endPoint});
            }
        }
        return pathsEndpoints;
    }

    /**
     * @return common value for radius of every vertex (circle) on the graph
     */
    public int getCircleRadius() {
        logger.info("Getting value of vertex radius...");
        WebElement circle = driver.findElements(By.xpath(VERTICES)).get(0);
        return Integer.parseInt(circle.getAttribute("r"));
    }

    /**
     * Finds vertex on the graph by its name and evaluates its coordinates as 2d point
     * @param vertex the name of vertex which point is needed
     * @return Point(x,y) object
     */
    public Point getVertexEndpoint(String vertex) {
        /** get circle of start vertex */
        logger.info("Getting vertex coordinates...");
        WebElement block = driver.findElement(By.xpath(String.format(VERTEX_BLOCK_TEMPLATE, vertex)));
        String attribute = block.getAttribute("transform");
        attribute = attribute.replaceAll("[a-zA-Z]", "");
        String[] numbers = attribute.replaceAll("[()]", "").split(",");
        return new Point(Integer.parseInt(numbers[0]), Integer.parseInt(numbers[1]));
    }

    /**
     * Returns status of instance from instances panel
     * @param instanceDate date stamp of instance
     * @return status of instance from instances panel
     */
    public String getInstanceStatus(String instanceDate) {
        waitForInstancesPanel();
        logger.info("Getting status of " + instanceDate + " instance");
        List<WebElement> status =
            driver.findElements(By.xpath(String.format(INSTANCE_STATUS_TEMPLATE, instanceDate)));
        if (status.isEmpty()) {
            return null;
        } else {
            return status.get(0).getAttribute("class").replace("instance-icons instance-link-", "");
        }
    }

    /**
     * Checks if 'Lineage' link is present on instances panel
     * @param instanceDate date stamp of instance
     * @return true if link is present
     */
    public boolean isLineageLinkPresent(String instanceDate) {
        waitForInstancesPanel();
        logger.info("Checking if 'Lineage' link is present for " + instanceDate);
        List<WebElement> lineage =
            driver.findElements(By.xpath(String.format(LINEAGE_LINK_TEMPLATE, instanceDate)));
        return !lineage.isEmpty();
    }

    private void waitForInstancesPanel() {
        waitForElement(INSTANCES_PANEL, DEFAULT_TIMEOUT, "Instances panel didn't appear");
    }

    /**
     * Checks whether vertex is terminal or not
     * @param vertexName name of vertex
     * @return whether it is terminal or not
     */
    public boolean isTerminal(String vertexName) {
        logger.info("Checking if " + vertexName + " is 'terminal' instance");
        waitForElement(String.format(VERTEX_TEMPLATE, vertexName), DEFAULT_TIMEOUT,
            "Vertex not found");
        WebElement vertex = driver.findElement(By.xpath(String.format(VERTEX_TEMPLATE, vertexName)));
        String vertexClass = vertex.getAttribute("class");
        return vertexClass.contains("lineage-node-terminal");
    }
}
