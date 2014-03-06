package eu.scape_project.HDFSFTPServer.handlers;

import org.apache.ftpserver.util.DateUtils;
import org.apache.hadoop.fs.FileStatus;

public class MLSDFileFormater implements FileFormatter{
	private static final String[] DEFAULT_TYPES = new String[] { "Size", 
		"Modify", "Type" };
	private final static char[] NEWLINE = { '\r', '\n' };

    private String[] selectedTypes = DEFAULT_TYPES;

    
    public MLSDFileFormater(String[] selectedTypes) {
        if (selectedTypes != null) {
            this.selectedTypes = selectedTypes.clone();
        }
    }
    
    public String format(FileStatus file){
    	StringBuilder sb = new StringBuilder();

        for (int i = 0; i < selectedTypes.length; ++i) {
            String type = selectedTypes[i];
            if (type.equalsIgnoreCase("size")) {
                sb.append("Size=");
                sb.append(String.valueOf(file.getLen()));
                sb.append(';');
            } else if (type.equalsIgnoreCase("modify")) {
                String timeStr = DateUtils.getFtpDate(file.getModificationTime());
                sb.append("Modify=");
                sb.append(timeStr);
                sb.append(';');
            } else if (type.equalsIgnoreCase("type")) {
                if (file.isDir()) {
                    sb.append("Type=dir;");
                } else{
                    sb.append("Type=file;");
                }
            } else if (type.equalsIgnoreCase("perm")) {
                sb.append("Perm=");
                if (isReadable(file)) {
                    if (!file.isDir()) {
                        sb.append('r');
                    } else {
                        sb.append('e');
                        sb.append('l');
                    }
                }
                if (isWritable(file)) {
                    if (!file.isDir()) {
                        sb.append('a');
                        sb.append('d');
                        sb.append('f');
                        sb.append('w');
                    } else {
                        sb.append('f');
                        sb.append('p');
                        sb.append('c');
                        sb.append('m');
                    }
                }
                sb.append(';');
            }
        }
        sb.append(' ');
        String name = file.getPath().toString();
        sb.append(normalizePath(name));

        sb.append(NEWLINE);

        return sb.toString();
    }

    private String normalizePath(String path){
		return path.substring(path.lastIndexOf("/") + 1);
	}
    
    private boolean isReadable(FileStatus f){
    	if(f.getPermission().getUserAction().SYMBOL.substring(0, 3).contains("r")){
    		return true;
    	}
    	return false;
    }
    
    private boolean isWritable(FileStatus f){
    	if(f.getPermission().getUserAction().SYMBOL.substring(0, 3).contains("w")){
    		return true;
    	}
    	return false;
    }
    
}


