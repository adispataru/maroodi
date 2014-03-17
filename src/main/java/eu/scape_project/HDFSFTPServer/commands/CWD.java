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

public class CWD extends AbstractCommand{

	private final Logger Log = LoggerFactory.getLogger(CWD.class);
	
	@Override
	public void execute(FtpIoSession session, FtpServerContext context,
			FtpRequest request) throws IOException, FtpException {
		
		
		session.resetState();

        // change directory
		FileSystem fs = HadoopEnv.getFileSystem(session.getUser(), session);
		
		String dirName = HadoopEnv.getHomeDirectory(session.getUser()).toString();
		
		 if (request.hasArgument()) {
	            dirName = dirName + "/" + request.getArgument();
	        }
		
        try {
            fs.setWorkingDirectory(new Path(dirName));
            dirName = fs.getWorkingDirectory().toString();
            
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_250_REQUESTED_FILE_ACTION_OKAY, "CWD",
                    dirName));
            
        } catch (Exception ex) {
            Log.debug("Failed to change directory in file system", ex);
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
                    "CWD", null));
        }
	}
	
}
