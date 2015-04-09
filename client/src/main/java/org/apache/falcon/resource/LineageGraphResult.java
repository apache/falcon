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

package org.apache.falcon.resource;

import org.apache.commons.lang3.StringUtils;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * LineageGraphResult is the output returned by all the apis returning a DAG.
 */
@XmlRootElement(name = "result")
@XmlAccessorType (XmlAccessType.FIELD)
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class LineageGraphResult {

    private String[] vertices;

    @XmlElement(name="edges")
    private Edge[] edges;

    private static final JAXBContext JAXB_CONTEXT;

    static {
        try {
            JAXB_CONTEXT = JAXBContext.newInstance(LineageGraphResult.class);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    public LineageGraphResult() {
        // default constructor for JAXB
    }

    /**
     * A class to represent an edge in a DAG.
     */
    @XmlRootElement(name = "edge")
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class Edge {
        @XmlElement
        private String from;
        @XmlElement
        private String to;
        @XmlElement
        private String label;

        public Edge() {

        }

        public Edge(String from, String to, String label) {
            this.from = from;
            this.to = to;
            this.label = label;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }

        public String getTo() {
            return to;
        }

        public void setTo(String to) {
            this.to = to;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getDotNotation() {
            StringBuilder result = new StringBuilder();
            if (StringUtils.isNotBlank(this.from) && StringUtils.isNotBlank(this.to)
                    && StringUtils.isNotBlank(this.label)) {
                result.append("\"" + this.from +"\"");
                result.append(" -> ");
                result.append("\"" + this.to + "\"");
                result.append(" [ label = \"" + this.label + "\" ] \n");
            }
            return result.toString();
        }

        @Override
        public String toString() {
            return getDotNotation();
        }

    }


    public String getDotNotation() {
        StringBuilder result = new StringBuilder();
        result.append("digraph g{ \n");
        if (this.vertices != null) {
            for (String v : this.vertices) {
                result.append("\"" + v + "\"");
                result.append("\n");
            }
        }

        if (this.edges != null) {
            for (Edge e : this.edges) {
                result.append(e.getDotNotation());
            }
        }
        result.append("}\n");
        return result.toString();
    }

    public String[] getVertices() {
        return vertices;
    }

    public void setVertices(String[] vertices) {
        this.vertices = vertices;
    }

    public Edge[] getEdges() {
        return edges;
    }

    public void setEdges(Edge[] edges) {
        this.edges = edges;
    }


    @Override
    public String toString() {
        return getDotNotation();
    }

}
