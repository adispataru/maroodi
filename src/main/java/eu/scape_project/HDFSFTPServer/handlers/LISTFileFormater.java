package eu.scape_project.HDFSFTPServer.handlers;

import org.apache.ftpserver.util.DateUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

public class LISTFileFormater implements FileFormatter{

	private final static char DELIM = ' ';
	private final static char[] NEWLINE = {'\r', '\n'};
	
	public String format(FileStatus file) {
		StringBuilder sb = new StringBuilder();
		sb.append(getPermision(file));
		sb.append(DELIM);
		sb.append(DELIM);
		sb.append(DELIM);
		sb.append(file.isDir() ? 3 : 1);
		sb.append(DELIM);
		sb.append(file.getOwner());
		sb.append(DELIM);
		sb.append(file.getGroup());
		sb.append(DELIM);
		sb.append(getLength(file));
		sb.append(DELIM);
		sb.append(getLastModified(file));
		sb.append(DELIM);
		String name = file.getPath().toString();
		sb.append(normalizePath(name));
		sb.append(NEWLINE);
		
		return sb.toString();
	}
	
	private String normalizePath(String path){
		return path.substring(path.lastIndexOf("/") + 1);
	}
	
	private char[] getPermision(FileStatus file){
		char[] permission = new char[10];
		
		//get permissions
		FsPermission fspm = file.getPermission();
		String userPerm = fspm.getUserAction().SYMBOL;
		String groupPerm = fspm.getGroupAction().SYMBOL;
		String otherPerm = fspm.getOtherAction().SYMBOL;
		
		
		permission[0] = file.isDir() ? 'd' : '-';
		//user permissions
		for(int i = 0; i < userPerm.length(); i++)
			permission[i + 1] = userPerm.charAt(i);
		
		//group permissions
		for(int i = 0; i < groupPerm.length(); i++)
			permission[i + 4] = groupPerm.charAt(i);
		
		//others permissions
		for(int i = 0; i < otherPerm.length(); i++)
			permission[i + 7] = otherPerm.charAt(i);
		
		return permission;
	}
	
	private String getLastModified(FileStatus file){
		return DateUtils.getUnixDate(file.getModificationTime());
	}
	
	private String getLength(FileStatus file){
		String initStr = "            ";
		long size = 0;
		if(!file.isDir())
			size = file.getLen();
		String sizeString = String.valueOf(size);
		if(sizeString.length() > initStr.length())
			return sizeString;
		
		return initStr.substring(0, initStr.length() - sizeString.length()) + sizeString;
		
	}
	
}
