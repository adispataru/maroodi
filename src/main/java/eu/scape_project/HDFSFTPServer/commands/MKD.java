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

public class MKD extends AbstractCommand {
	
	private Logger LOG = LoggerFactory.getLogger(MKD.class);
	
	@Override
	public void execute(FtpIoSession session, FtpServerContext context,
			FtpRequest request) throws IOException, FtpException {
		// reset state
        session.resetState();

        // argument check
        String fileName = request.getArgument();
        
        if (fileName == null) {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
                    "MKD", null));
            return;
        }

        
        // get file object
        FileSystem fs = HadoopEnv.getFileSystem(session.getUser(), session);
        Path filePath = null;
        if(fileName.charAt(0) == '/'){
        	filePath = new Path(HadoopEnv.getHomeDirectory(session.getUser())
        				.toString() + fileName);
        }else{
        	filePath = new Path(fs.getWorkingDirectory().toString() + "/"
        				+ fileName);
        }
        
        
        // check file existence
        if (fs.exists(filePath)) {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
                    "MKD.exists", fileName));
            return;
        }

        
        
        // now create directory
        if (fs.mkdirs(filePath)) {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_257_PATHNAME_CREATED, "MKD", fileName));

            // write log message
            String userName = session.getUser().getName();
            LOG.info("Directory create : " + userName + " - " + fileName);

        } else {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN, "MKD",
                    fileName));
        }

	}

}
