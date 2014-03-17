package eu.scape_project.HDFSFTPServer.handlers;

import java.io.IOException;

import org.apache.ftpserver.command.impl.listing.ListArgument;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DirectoryLister {
	
	private FileFormatter formatter = null;
	
	public String listFiles(final FileSystem fs, ListArgument arg, FileFormatter f) throws IOException{
		
		formatter = f;
			
		StringBuilder sb = new StringBuilder();
		
		FileStatus[] files = fs.listStatus(new Path(fs.getWorkingDirectory(), arg.getFile()));
		if(files != null){
			sb.append(traverseFiles(files, fs));
		
		}
		return sb.toString();
	}
	
	private String traverseFiles(final FileStatus[] files, 
			final FileSystem fs){
		StringBuilder sb = new StringBuilder();
		
		sb.append(traverseFiles(files, true));
		sb.append(traverseFiles(files, false));
		
		return sb.toString();
	}
	
	private String traverseFiles(final FileStatus[] files, boolean matchDir){
		
		StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i < files.length; i++){
			if(files[i] == null)
				continue;
			if(files[i].isDir() == matchDir)
				sb.append(formatter.format(files[i]));
				
			
		}
		return sb.toString();
	}
	
}
