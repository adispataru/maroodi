package eu.scape_project.HDFSFTPServer.handlers;

import org.apache.hadoop.fs.FileStatus;

public interface FileFormatter {
	
	public String format(FileStatus file);
}
