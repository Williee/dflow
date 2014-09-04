package org.dflow;

import java.util.ArrayList;
import java.util.List;

import org.dflow.msg.DCommand;
import org.dflow.msg.DDocument;
import org.dflow.msg.DEvent;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;




public abstract class DComponent {

	public enum MOD  {LOCAL, REMOTE} 
	
	private String id;
	
	private  Socket s_sub=null;
	private  Socket s_rep=null;
	private  Socket s_push=null;
	
	private List<Socket>  l_sockets_in; // List of sockets in
	private List<Socket>  l_sockets_out; // List of out sockets

	
	private  String sub_addr;
	private  String rep_addr;
	private  String push_addr;
	private  ZContext ctx;
    private  long tlasthb;
    private  final int SEC = 1000; //1s
    private  ZLoop loop;
	
 // Constructor
 	public DComponent(String rep_addr, String id){
 		
 		assert(rep_addr!=null);
 		assert(id!=null);
 		
 		this.l_sockets_in=  new ArrayList<Socket>();
 		this.l_sockets_out = new ArrayList<Socket>();  
 		
 		this.rep_addr =rep_addr.replace("localhost", "*");
 		this.id= id;
 		this.tlasthb = System.currentTimeMillis();	
 		
 		// get context
 		this.ctx = DContext.getInstance().getCtx();
 		
 		//create REP socket 
 		s_rep = ctx.createSocket(ZMQ.REP);
 		assert( s_rep!=null );
 		
 		//bind rep socket
 		int rc= s_rep.bind(rep_addr);
 		assert(rc!=-1);
 	}
    
 	private String getId(){
		return this.id;
	}
 	
 	
	public void run(){
			/* Create event loop and start it */
			startEventLoop();
	}
 	

private void startEventLoop() {
		
		ZLoop.IZLoopHandler repSocketEvent = new ZLoop.IZLoopHandler() {

	        @Override
	        public int handle(ZLoop loop, PollItem item, Object arg) {
	        	
	        	Socket in = (Socket)arg;
	        	ZMsg msg = ZMsg.recvMsg(in);
	        	
	        	if (DCommand.isCommand(msg)){
	        		DCommand cmd = new DCommand(msg); 
	        		int r = onCommand_(cmd);
	        		if (r!=-1){
	        			s_rep.send("OK");
	        			return r;
	        		}
	        		else
	        			return -1; //stop loop
	        	}else{
	        		
	        		
	        	}
	        	return 0;
	        }
	    };
	    
	   
	    
	    ZLoop.IZLoopHandler doWorkTimerEvent = new ZLoop.IZLoopHandler() {

	        @Override
	        public int handle(ZLoop loop, PollItem item, Object arg) {
	        	//Logger.Log("do work timer");
	        	DComponent comp = (DComponent)arg;
	        	assert(comp!=null);
	        	comp.doWork();
	        	return 0;
	        }
	    };

	    loop = new ZLoop();
	    assert (loop != null);
	     
	    // add do work timer
	    loop.addTimer(1, 0, doWorkTimerEvent,this);
	    
	     // Add reactor for rep socket 
        PollItem pollInputRep = new PollItem(s_rep, Poller.POLLIN );
        int rc = loop.addPoller (pollInputRep, repSocketEvent, s_rep);
        assert(rc==0);
        
        loop.start (); /// EVENT LOOOP
         
        // after event loop, component is exiting 
        
        loop.removePoller(pollInputRep);
        loop.destroy();

        // send ok command  (context could be terminated)
        s_rep.send("OK");
        
		
	}

private int onCommand_(DCommand cmd){
	

	
	if (cmd.getType().equals(DCommand.Type.QUIT)){
		
		
		
		return -1; //exit loop 
		
	}else if (cmd.getType().equals( DCommand.Type.AREYOUREADY)){
		
		DCommand.replyACK(this.s_rep);  // reply ack 
		
	}else if (cmd.getType().equals( DCommand.Type.AREYOUREADY)){
		
	}else if (cmd.getType().equals( DCommand.Type.CREATE_PORT)){
		
		String stype = cmd.getAttribute("TYPE");
		
		int type = Integer.parseInt(stype);
		String address = cmd.getAttribute("ADDRESS");
		String mode = cmd.getAttribute("MODE");
		
		createNewSocket(type,address,mode);
	}else{
		// custom command , call base class to resolve
		onCommand(cmd);
	}
		
		
	return 0;
}
	
class EvntArg{
	public Socket socket;
	public DComponent dcomp;
}

private void createNewSocket(int type, String address, String mode) {
	
	Socket s = ctx.createSocket(type);
	
	if (mode.equals("BIND")){ //output sockets
		
		address = address.replace("localhost","*");
		int rc = s.bind( address );
		assert( rc!=-1 );
		
		l_sockets_out.add(s); // add to sockets out list
		
	}else{
		
		s.connect( address );
		
		//add to listen queue
		ZLoop.IZLoopHandler socketEvent = new ZLoop.IZLoopHandler() {

	        @Override
	        public int handle(ZLoop loop, PollItem item, Object arg) {
	        	
	        	
	        	EvntArg earg = (EvntArg)arg; 
	        	Socket in = earg.socket;
	        	ZMsg zmsg = ZMsg.recvMsg(in);
	        	DDocument msg = new DDocument(zmsg);
	        	earg.dcomp.onMessage(msg);
	        	return 0;
	        }
			
	    };
		
		 // Add reactor for rep socket 
        PollItem pollInputRep = new PollItem(s, Poller.POLLIN );
        
        EvntArg earg = new EvntArg();
        earg.socket = s;
        earg.dcomp = this;
        
        int rc = loop.addPoller (pollInputRep, socketEvent,earg);
        assert(rc==0);
	
        l_sockets_in.add(s); //add to sockets in list
	}
			
}

protected void send( DDocument msg ){
	
	for(int i = 0 ; i< l_sockets_out.size() ; i++){
		 Socket s = l_sockets_out.get(i);
		 msg.send(s, false);
	 }
	
	msg.destroy();	
}

private void notify_event(DEvent evnt){
	evnt.send(s_push, false);
}


/*Destructor*/
public void close(){
	
	if (s_sub!=null){
		ctx.destroySocket(s_sub);
	}
	if (s_rep!=null){
		ctx.destroySocket(s_rep);
	}
	if (s_push!=null){
		ctx.destroySocket(s_push);
	}
	
	//close create sockets
	 for(int i = 0 ; i< l_sockets_out.size() ; i++){
		 Socket s = l_sockets_out.get(i);
		 ctx.destroySocket(s);
	 }
	 for(int i = 0 ; i< l_sockets_in.size() ; i++){
		 Socket s = l_sockets_in.get(i);
		 ctx.destroySocket(s);
	 }
	 
	 l_sockets_out.clear();
	 l_sockets_in.clear();
	
	
}

// method to be overrited on the base clase
protected abstract void onMessage(DDocument msg);

// method to be overrited on the base clase
protected abstract void onCommand(DCommand cmd);

// method to be overrited on the base clase
protected abstract void doWork();

	
	public class Proxy{
		
		public String address;
		public MOD mode;
		public String full_name;
		public String id; 
		
	}
}
