package org.dflow;

import org.zeromq.ZContext;

/**
 * DContext provides a high-level Context management class
 * 
 * It needs to be call to create the  JeroMQ Context 
 
 */
public class DContext {

	private static boolean init=false;
	private static final DContext _instance = new DContext();
	private ZContext ctx; 
	private static final int MIN_NUM_GENERATOR = 5557;
	private int tcp_port;
	private int ipc_num;
	private String protocol;
	
	
	public static DContext getInstance(){
		
		if (init==false){
			throw new java.lang.RuntimeException("Pelase call the static INIT() method first.");
		}
		return _instance;
	}
	
	/*
	 * Start JeroMQ Context , initialize global variables.
	 */
	public static void INIT(){
		init=true;
		DContext.getInstance();	
	}
	
	
	/*
	 * Terminate and destroy JeroMQ Context and global variables, resources
	 */
	public static void DESTROY(){
		DContext shared = DContext.getInstance();
		shared.getCtx().destroy();
	}
	
	/*
	 * Private constructor
	 */
	
	private DContext(){
		
		this.ctx = new ZContext(); // ensure that there is only one context creation
		
		this.tcp_port=MIN_NUM_GENERATOR;
		this.ipc_num=MIN_NUM_GENERATOR;
		this.protocol  ="tcp";  // default protocol is TCP
	}
	
	
	/*
	 * Get Context
	 */
	public ZContext getCtx(){
		return ctx;
	}
	
	
	/*
	 * Get new endpoint address
	 */
	public String getNewAddress(final String protocol_){
		
		String address="";
		
		if (protocol_.equals("tcp")) {
			address = "tcp://" + getHostName() + ":" +tcp_port;  
			tcp_port++;
		} else if (protocol_.equals("ipc")) {
			address = "ipc:///" + generateIPC();  
		}else if (protocol_.equals("inproc")) {
			address = "inproc://" + generateIPC(); 
		}
		
		return address;
	}
	
	private String getHostName(){
		
		return "localhost";
	}
	
	private String generateIPC(){

		String addres =  String.format("test") +ipc_num+ "ipc"  ;
		ipc_num++;
		return addres;
	}
	
}
