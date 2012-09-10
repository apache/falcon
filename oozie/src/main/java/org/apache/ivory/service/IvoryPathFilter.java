package org.apache.ivory.service;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public interface IvoryPathFilter extends PathFilter{
    
    String getJarName(Path path);
}
