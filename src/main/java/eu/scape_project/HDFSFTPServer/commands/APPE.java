package eu.scape_project.HDFSFTPServer.commands;

import java.io.IOException;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;

public class APPE extends AbstractCommand{

	@Override
	public void execute(FtpIoSession session, FtpServerContext context,
			FtpRequest request) throws IOException, FtpException {
		
		session.resetState();
		
		session.write(LocalizedFtpReply.translate(session, request, context,
				FtpReply.REPLY_502_COMMAND_NOT_IMPLEMENTED, "APPE", null));
		
	}

}
