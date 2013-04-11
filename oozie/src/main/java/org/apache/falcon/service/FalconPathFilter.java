package org.apache.falcon.service;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public interface FalconPathFilter extends PathFilter{
    
    String getJarName(Path path);
}
