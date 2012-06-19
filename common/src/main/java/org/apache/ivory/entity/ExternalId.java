package org.apache.ivory.entity;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.v0.SchemaHelper;

import java.util.Date;

public class ExternalId {
    private static final String SEPARATOR = "/";
    private String id;

    public ExternalId(String id) {
        this.id = id;
    }
    
    public String getId() {
        return id;
    }
    
    public ExternalId(String name, Tag tag, String elexpr) {
        if(StringUtils.isEmpty(name) || tag == null || StringUtils.isEmpty(elexpr))
            throw new IllegalArgumentException("Empty inputs!");
        
        id = name + SEPARATOR + tag.name() + SEPARATOR + elexpr;
    }
    
    public ExternalId(String name, Tag tag, Date date) {
        this(name, tag, SchemaHelper.formatDateUTC(date));
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
    
	public Tag getTag() {
		String[] parts = id.split(SEPARATOR);
		return Tag.valueOf(parts[1]);
	}

	public String getDFSname() {
		return id.replace(":", "-");
	}
}
