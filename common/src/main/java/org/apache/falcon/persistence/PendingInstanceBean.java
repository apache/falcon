package org.apache.falcon.persistence;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * Created by praveen on 8/3/16.
 */
@Entity
@NamedQueries({
        @NamedQuery(name = PersistenceConstants.GET_PENDING_INSTANCES, query = "select OBJECT(a) from PendingInstanceBean a where a.feedName = :feedName"),
        @NamedQuery(name = PersistenceConstants.DELETE_PENDING_NOMINAL_INSTANCES , query = "delete from PendingInstanceBean a where a.feedName = :feedName and a.clusterName = :clusterName and a.nominalTime = :nominalTime"),
        @NamedQuery(name = PersistenceConstants.DELETE_ALL_PENDING_INSTANCES , query = "delete from PendingInstanceBean a where a.feedName = :feedName and a.clusterName = :clusterName"),
        @NamedQuery(name = PersistenceConstants.GET_DATE_FOR_PENDING_INSTANCES, query = "select a.nominalTime from PendingInstanceBean a where a.feedName = :feedName and a.clusterName = :clusterName"),
        @NamedQuery(name= PersistenceConstants.GET_ALL_PENDING_INSTANCES ,query = "select  OBJECT(a) from PendingInstanceBean a ")
})
@Table(name = "PENDING_INSTANCES")
public class PendingInstanceBean {
    @NotNull
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Id
    private String id;

    @Basic
    @NotNull
    @Column(name = "feed_name")
    private String feedName;

    @Basic
    @NotNull
    @Column(name = "cluster_name")
    private String clusterName;

    @Basic
    @NotNull
    @Column(name = "nominal_time")
    private Date nominalTime;

    public Date getNominalTime() {
        return nominalTime;
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTime = nominalTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getFeedName() {
        return feedName;
    }

    public void setFeedName(String feedName) {
        this.feedName = feedName;
    }
}
