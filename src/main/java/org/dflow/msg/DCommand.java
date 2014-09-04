package org.dflow.msg;

import java.util.Iterator;

import org.dflow.common.DReliableClient;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class DCommand {

	private static final String DCMD = "DCMD"; //to identify the command message
	//private String dest; 
	private DCommand.Type type;
	private ZMsg msg; // hold the message to send
		 
	 public enum Type {
		 CMDACK,		// used for ack
		 AREYOUREADY,		// used by DCompFactory to sync component.
		 DFLOWHELLO,		// used by dflow to say hello
		 CREATE_PORT, // dflow send this command to the components to create ports
		 RECYCLE,    // dflow send this command to the commponents to recicle 
		 READY,	    // dflow send this command to ask the component if it is ready
		 QUIT	// Quit execution
	 }
	 

	// Constructor 1
		 public DCommand(Type type ){
			 
			 //dest=dest_id_;
			 this.type = type;
			 this.msg=new ZMsg();
			 
			 this.msg.add(DCMD); // to identify a command message
			 this.msg.add(type.name()); //type
		 }
		 
		 public DCommand.Type getType(){
			 return type;
		 }
		 
		 //Constructor2
		 public DCommand(ZMsg msg){
			
			 if (isCommand(msg)){
				 msg.popString(); // get ride of "DCMD"
				 this.type= DCommand.Type.valueOf(msg.popString());
				 this.msg = msg;
				 
			 }else {
				 // throw exception
				 
			 }
		 }
		 public String getAttribute(String name){
				
			 Iterator<ZFrame> iter = msg.iterator();
	        
			 while (iter.hasNext()) {
				 
	             ZFrame frame = iter.next();
	             String data = frame.toString();
	             
	             if (data.startsWith(name)){
	            	 String[] parts =  data.split("-",2);
	            	 return parts[1];
	             }
	             
	         }
	         return null;
		 }
		 
		 public void addAttribute(String name, String value){
			 
			 String attribute =String.format( "%s-%s", name , value ) ;
			 msg.add( attribute );
			 
		 }
		 
		 public static void replyACK(Socket s){
			 
			 DCommand cmd_ack = new DCommand(DCommand.Type.CMDACK);
			 cmd_ack.send(s);
				
		 }
		 
		 private void send(Socket s){
			 this.msg.send(s);
		 }
		 
		 
		public ZMsg sendRecive(ZContext ctx_, String endpoint_){
			 
			 DReliableClient client = new DReliableClient(ctx_,endpoint_,msg);
			 ZMsg msg = client.sendRecive();
			 return msg;
		 }
		 
		 public  static boolean isCommand(ZMsg msg){
				String first =  msg.peekFirst().toString();
				
				if (first.equals(DCMD)){
					return true;
				}else
					return false;
			 }
		 
		 
}
