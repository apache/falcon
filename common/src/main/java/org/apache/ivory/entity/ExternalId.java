package org.apache.ivory.entity;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;

//TODO Handle late data coords
public class ExternalId {
    private static final String SEPARATOR = "/";
    private String id;

    public ExternalId(String id) {
        this.id = id;
    }
    
    public String getId() {
        return id;
    }
    
    public ExternalId(String name, String elexpr) {
        if(StringUtils.isEmpty(name) || StringUtils.isEmpty(elexpr))
            throw new IllegalArgumentException("Empty inputs!");
        
        id = name + SEPARATOR + elexpr;        
    }
    
    public ExternalId(String name, Date date) {
        this(name, EntityUtil.formatDateUTC(date));
    }
    
    public String getName() {
        String[] parts = id.split(SEPARATOR);
        return parts[0];
    }
    
    public Date getDate() throws IvoryException {
        return EntityUtil.parseDateUTC(getDateAsString());            
    }
    
    public String getDateAsString() throws IvoryException {
        String[] parts = id.split(SEPARATOR);
        return parts[1];
    }
}
