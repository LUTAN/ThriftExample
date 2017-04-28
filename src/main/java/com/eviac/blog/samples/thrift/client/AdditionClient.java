package com.eviac.blog.samples.thrift.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import com.eviac.blog.samples.thrift.server.AdditionService;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.lang.*;

import com.lg.zookeeper.ZKClient;


public class AdditionClient {

  public static void main(String[] args) {
    ZKClient zkClient = new ZKClient();        
    String zkHosts = "10.0.0.39:2181,10.0.0.39:2182,10.0.0.39:2183";
    try{
        zkClient.connect(zkHosts);
    }
    catch (Exception e){
        e.printStackTrace();
    }

    Random rd = new Random();
    try {

        List<String> children = zkClient.zk.getChildren("/LGthrift", false);
        int index = rd.nextInt(children.size());

        System.out.println(children.toString());
        System.out.println("========================" + children.get(index) + "===================");
        byte[] value = zkClient.zk.getData("/LGthrift/"+children.get(index), false, null);
        String host = new String(value, StandardCharsets.UTF_8);

        TTransport transport;
        transport = new TSocket(host, 9090);
        transport.open();

        TProtocol protocol = new TBinaryProtocol(transport);
        AdditionService.Client client = new AdditionService.Client(protocol);

        System.out.println(client.add(Integer.parseInt(args[0]), Integer.parseInt(args[1])));

        Double res = client.multiply(Double.parseDouble(args[0]), Double.parseDouble(args[1]));
        System.out.println("the result is: " + res);

        System.out.println("my name is:" + client.get_name());

        transport.close();
    } catch (TTransportException e) {
        e.printStackTrace();
    } catch (TException x) {
        x.printStackTrace();
    }
    catch (Exception e) {
        e.printStackTrace();
    }
  }

}
