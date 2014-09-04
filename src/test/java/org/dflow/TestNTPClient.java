package org.dflow;


import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;

import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.junit.Test;

public class TestNTPClient {

	@Test
	public void test() {
		
		String[] args = {"pool.ntp.org"};
				
		 if (args.length == 0) {
	            System.err.println("Usage: NTPClient <hostname-or-address-list>");
	            System.exit(1);
	        }

	        NTPUDPClient client = new NTPUDPClient();
	        // We want to timeout if a response takes longer than 10 seconds
	        client.setDefaultTimeout(10000);
	        try {
	            client.open();
	            for (String arg : args)
	            {
	                System.out.println();
	                try {
	                    InetAddress hostAddr = InetAddress.getByName(arg);
	                    System.out.println("> " + hostAddr.getHostName() + "/" + hostAddr.getHostAddress());
	                    TimeInfo info = client.getTime(hostAddr);
	                    NTPClient.processResponse(info);
	                    
	                } catch (IOException ioe) {
	                    ioe.printStackTrace();
	                }
	            }
	        } catch (SocketException e) {
	            e.printStackTrace();
	        }

	        client.close();
		
	}



}