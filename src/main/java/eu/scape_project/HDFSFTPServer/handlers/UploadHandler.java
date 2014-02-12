package eu.scape_project.HDFSFTPServer.handlers;

import java.io.File;
import java.io.IOException;

import org.apache.ftpserver.ftplet.DefaultFtplet;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.FtpletResult;

public class UploadHandler extends DefaultFtplet{
	
	private HDFSUploader hdfsUploader = null;
	
	private File getRealFile(FtpSession session, FtpFile ftpFile) throws FtpException{
        String filePath = session.getUser().getHomeDirectory() + ftpFile.getAbsolutePath();
        return new File(filePath);
	}
	
	private FtpFile getFtpFile(FtpSession session, FtpRequest request) throws FtpException{
	        return session.getFileSystemView().getFile(request.getArgument());
	}
	
	@Override
	public FtpletResult onUploadEnd(FtpSession session, FtpRequest request) throws FtpException, IOException {
	        FtpFile ftpFile = getFtpFile(session, request);
	        File file = getRealFile(session, ftpFile);
	        String filePath = file.getAbsolutePath();
	        
	        if(hdfsUploader == null){
	        	//If the uploader hasn't been initialized, it means it will run locally and it will 
	        	//put the file in the directory user/scape from HDFS.
	        	hdfsUploader = new HDFSUploader("user/scape");
	        }
	        
	        hdfsUploader.upload(filePath);
	        
	        return super.onUploadEnd(session, request);
	        
	}
	
	
	
	
}