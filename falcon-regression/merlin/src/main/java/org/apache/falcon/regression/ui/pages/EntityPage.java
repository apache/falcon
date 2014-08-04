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


import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;

public class EntityPage<T> extends Page {

    private Class<T> type;

    protected EntityPage(WebDriver driver, ColoHelper helper, EntityType type, Class<T> entity, String entityName) {
        super(driver, helper);
        URL += String.format("/entity.html?type=%s&id=%s", type.toString().toLowerCase(), entityName);
        this.type = entity;
        expectedElement = "//textarea[@id='entity-def-textarea' and contains(text(), 'xml')]";
        notFoundMsg = String.format(" %s '%s' not found!", type, entityName);
    }

    /**
     * Returns page of defined CLUSTER entity
     * @param entityName name of defined entity
     * @return page of defined CLUSTER entity
     */
    public static EntityPage<Cluster> getClusterPage(WebDriver driver, ColoHelper helper, String entityName) {
        return new EntityPage<Cluster>(driver, helper, EntityType.CLUSTER, Cluster.class, entityName);
    }

    /**
     * Returns page of defined FEED entity
     * @param entityName name of defined entity
     * @return page of defined FEED entity
     */
    public static EntityPage<Feed> getFeedPage(WebDriver driver, ColoHelper helper, String entityName) {
        return new EntityPage<Feed>(driver, helper, EntityType.FEED, Feed.class, entityName);
    }

    /**
     * Returns entity object
     * @return entity object
     * @throws JAXBException
     */
    @SuppressWarnings("unchecked")
    public T getEntity() throws JAXBException {
        String entity = driver.findElement(By.id("entity-def-textarea")).getText();
        JAXBContext jc = JAXBContext.newInstance(type);
        Unmarshaller u = jc.createUnmarshaller();
        return (T) u.unmarshal(new StringReader(entity));
    }

}
