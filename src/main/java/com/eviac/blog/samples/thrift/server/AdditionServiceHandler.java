package com.eviac.blog.samples.thrift.server;

import org.apache.thrift.TException;

public class AdditionServiceHandler implements AdditionService.Iface {

 	@Override
 	public int add(int n1, int n2) throws TException {
  		return n1 + n2;
 	}

 	public double multiply(double n1, double n2) throws org.apache.thrift.TException{
 		return n1 * n2;
 	}

    public java.lang.String get_name() throws org.apache.thrift.TException{
    	return "my name ls LG";
    }
}
