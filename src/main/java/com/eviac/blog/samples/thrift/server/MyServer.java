package com.eviac.blog.samples.thrift.server;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import com.lg.zookeeper.ZKClient;

public class MyServer {

    public static void StartsimpleServer(AdditionService.Processor<AdditionServiceHandler> processor){
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            //TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

            //Use this for a multithreaded server
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
   
            System.out.println("Starting the simple server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
                         
    public static void main(String[] args) {

        ZKClient zkClient = new ZKClient();
        String zkHosts = "10.0.0.39:2181,10.0.0.39:2182,10.0.0.39:2183";
        try{
            zkClient.connect(zkHosts);
            if (zkClient.zk.exists("/LGthrift", false) == null){
                zkClient.createPerNode("/LGthrift", null);
            }
            zkClient.createEphNode("/LGthrift/"+args[0], args[0].getBytes());
        }
        catch (Exception e){
            e.printStackTrace();
        }
        
        StartsimpleServer(new AdditionService.Processor<AdditionServiceHandler>(new AdditionServiceHandler()));
    }
}
