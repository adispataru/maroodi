package eu.scape_project.HDFSFTPServer.commands;

import java.io.IOException;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scape_project.HDFSFTPServer.env.HadoopEnv;

public class DELE extends AbstractCommand {

    private final Logger LOG = LoggerFactory.getLogger(DELE.class);

    /**
     * Execute command.
     */
    public void execute(final FtpIoSession session,
            final FtpServerContext context, final FtpRequest request)
            throws IOException, FtpException {

        // reset state variables
        session.resetState();

        // argument check
        String fileName = request.getArgument();
        if (fileName == null) {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
                    "DELE", null));
            return;
        }

        FileSystem fs = HadoopEnv.getFileSystem(session.getUser(), session);
        Path filePath = null;
        if(fileName.charAt(0) == '/'){
        	filePath = new Path(HadoopEnv.getHomeDirectory(session.getUser())
        				.toString() + fileName);
        }else{
        	filePath = new Path(fs.getWorkingDirectory().toString() + "/"
        				+ fileName);
        }
        
        if(!fs.exists(filePath)) {
            LOG.debug("Could not get file ");
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
                    "DELE.invalid", fileName));
            return;
        }
        

        FileStatus fileStatus = fs.getFileStatus(filePath);
        
        if (fileStatus.isDir()) {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
                    "DELE.invalid", fileName));
            return;
        }
        
        

        // now delete
        if (fs.delete(filePath, false)) {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_250_REQUESTED_FILE_ACTION_OKAY, "DELE",
                    fileName));

            // log message
            String userName = session.getUser().getName();

            LOG.info("File delete : " + userName + " - " + fileName);
            
        } else {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_450_REQUESTED_FILE_ACTION_NOT_TAKEN, "DELE",
                    fileName));
        }
    }

}
