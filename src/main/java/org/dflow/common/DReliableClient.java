package org.dflow.common;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class DReliableClient {

	private ZContext ctx;
	private ZMsg msg;
	
	private final  int REQUEST_TIMEOUT = 2500;    //  msecs, (> 1000!)
	private final  int REQUEST_RETRIES = 3;       //  Before we abandon
	private final  String server_address ;
	
	/*
	 * Constructor
	 */
	public DReliableClient(ZContext ctx_, String endpoint_, ZMsg msg_){
		
		this.ctx= ctx_ ; 
		this.server_address = endpoint_ ; 
		this.msg= msg_ ;	
	}
	
	
	public ZMsg sendRecive(){

		// create socket
		Socket client = ctx.createSocket(ZMQ.REQ);
		assert(client!=null);

		client.connect(server_address);
		
		

		int retriesLeft = REQUEST_RETRIES;
			//int sent_count=0;
			ZMsg reply=null;
			while (retriesLeft > 0 && !Thread.currentThread().isInterrupted()) {

				boolean sendok= msg.send(client,false);
				assert(sendok==true);
				


				int expect_reply = 1;
				while (expect_reply > 0) {
					//  Poll socket for a reply, with timeout
					PollItem items[] = {new PollItem(client, Poller.POLLIN)};
					int rc = ZMQ.poll(items, REQUEST_TIMEOUT);
					if (rc == -1)
						break;          //  Interrupted

					if (items[0].isReadable()) {
						//  We got a reply from the server
						//String reply = client.recvStr();
						reply = ZMsg.recvMsg(client);
						if (reply == null)
							break;      //  Interrupted

						if(!reply.isEmpty()){
							
							retriesLeft = 0;
							expect_reply = 0;
							break;
						} 
							

					} else if (--retriesLeft == 0) {
						
						return null;
					} else {
						
						//  Old socket is confused; close it and open a new one
						ctx.destroySocket(client);
						
						client = ctx.createSocket(ZMQ.REQ);
						client.connect(server_address);
						//  Send request again, on new socket
						msg.send(client,false);

					}
				}
			}

			
			msg.destroy();
			ctx.destroySocket(client);
			
			return reply;
			
		

	}
	
}
