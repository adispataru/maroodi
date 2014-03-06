package eu.scape_project.HDFSFTPServer.commands;

import java.io.IOException;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scape_project.HDFSFTPServer.env.HadoopEnv;

public class CDUP extends AbstractCommand {

	private final Logger LOG = LoggerFactory.getLogger(CDUP.class);
	@Override
	public void execute(FtpIoSession session, FtpServerContext context,
			FtpRequest request) throws IOException, FtpException {
		
		session.resetState();

		FileSystem fs = HadoopEnv.getFileSystem(session.getUser().getName());
		String dirName = fs.getWorkingDirectory().toString();
		dirName = dirName.substring(0, dirName.lastIndexOf("/"));
		
		
		//user shall not go upper than the FTP root dir.
		if(dirName.length() < HadoopEnv.getHomeDirectory(session.getUser())
				.toString().length()){
			
			LOG.info("User {} trying to go beyond his powers", session.getUser().getName());
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_553_REQUESTED_ACTION_NOT_TAKEN_FILE_NAME_NOT_ALLOWED,
                    "CWD", "Cannot change directory beyond this one."));
            return;
		}
		
		try {
			
            fs.setWorkingDirectory(new Path(dirName));
            dirName = fs.getWorkingDirectory().toString();
            Path homeDir = HadoopEnv.getHomeDirectory(session.getUser());
            
            if(dirName.length() < homeDir.toString().length())
            	fs.setWorkingDirectory(homeDir);
            
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_250_REQUESTED_FILE_ACTION_OKAY, "CWD",
                    dirName));
            
        } catch (Exception ex) {
            LOG.debug("Failed to change directory in file system", ex);
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
                    "CWD", null));
        }
		
	}

}
