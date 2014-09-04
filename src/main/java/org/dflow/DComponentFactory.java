package org.dflow;

import java.util.ArrayList;
import java.util.List;

import org.dflow.msg.DCommand;
import org.zeromq.ZContext;
import org.zeromq.ZMsg;

public class DComponentFactory {

	private final ZContext ctx;
	private List<DComponent.Proxy>  l_components; // List of components created
	
public DComponentFactory(){
		
		// get context
		this.ctx = DContext.getInstance().getCtx();
		this.l_components=  new ArrayList<DComponent.Proxy>();
	}



public void quitComponent(DComponent.Proxy comp){
	
	String address = comp.address;
	// Create port command to first component
	DCommand cmd = new DCommand(DCommand.Type.QUIT);
	ZMsg recive = cmd.sendRecive(ctx,address);
	
	if (recive!=null){
		String msg = recive.popString();
		assert(msg.equals("OK"));
	}
	
	l_components.remove(comp);

}

	
}
