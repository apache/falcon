package org.apache.ivory.entity;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;

public class ExternalId {
    private static final String SEPARATOR = "/";
    private String id;

    public ExternalId(String id) {
        this.id = id;
    }
    
    public String getId() {
        return id;
    }
    
    public ExternalId(String name, String tag, String elexpr) {
        if(StringUtils.isEmpty(name) || StringUtils.isEmpty(tag) || StringUtils.isEmpty(elexpr))
            throw new IllegalArgumentException("Empty inputs!");
        
        id = name + SEPARATOR + tag + SEPARATOR + elexpr;        
    }
    
    public ExternalId(String name, String tag, Date date) {
        this(name, tag, EntityUtil.formatDateUTC(date));
    }
    
    public String getName() {
        String[] parts = id.split(SEPARATOR);
        return parts[0];
    }
    
    public Date getDate() throws IvoryException {
        return EntityUtil.parseDateUTC(getDateAsString());            
    }
    
    public String getDateAsString() {
        String[] parts = id.split(SEPARATOR);
        return parts[2];
    }
    
	public String getTag() {
		String[] parts = id.split(SEPARATOR);
		return parts[1];
	}

	public String getDFSname() {
		return id.replace(":", "-");
	}
}
