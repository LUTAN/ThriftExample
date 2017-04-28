package com.lg.mysqlconn;

import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;  
import java.text.SimpleDateFormat; 

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.*;

import com.opencsv.CSVParser;

/**
 * @author: lutan
 *
 */
public class App 
{
    static JsonParser parser = new JsonParser();
    static int non_json_count = 0; 
    //static HashSet<String> first_keys = new HashSet<String>();
    static HashSet<String> first_keys = new HashSet<String>(Arrays.asList("birthday", "qq", "leader", "address", "sex", "mobile", "degree", "stamp", "political", "title", "community", "work_area", "team_name", "major", "nationality", "phone", "car", "school", "idcard", "name", "company", "_id", "department", "email"));
    
    static HashMap<String, HashSet<String>> second_keys = new HashMap<String, HashSet<String>>();

    public static void main(String[] args)
    {
        String fileName = "/Users/lutan/Desktop/ICBC/gongshangyinhang.json";
        HashSet<String> car = new HashSet<String>(Arrays.asList("car_model", "colour", "engine_no", "plate_no", "car_brand", "from", "vin"));
        HashSet<String> stamp = new HashSet<String>(Arrays.asList("update_timestamp", "reg_timestamp", "from", "dimension"));
        HashSet<String> ids = new HashSet<String>(Arrays.asList("$oid"));
        second_keys.put("car", car);
        second_keys.put("stamp", stamp);
        second_keys.put("_id", ids);

        // String filePathString = "/Users/lutan/Desktop/ICBC/keys.txt";
        // File f = new File(filePathString);
        // if(!f.exists()) { 
        //     long count = 0;
        //     //read file into stream, try-with-resources
        //     try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
        //         String line;
        //         while ((line = br.readLine()) != null) {
        //             getAllTheKey(line);
        //         }
        //     }
        //     catch(Exception e){
        //         e.printStackTrace();
        //     }

        //     System.out.println("Writing to file: " + filePathString);
        //     // Files.newBufferedWriter() uses UTF-8 encoding by default
        //     try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePathString))) {
        //         writer.write(first_keys.toString() + "\n");
        //         writer.write(second_keys.toString() + "\n");
        //     }
        //     catch(Exception e){
        //         System.out.println("writing exception");
        //     } 
        //     System.out.println("non_json_count is: " + non_json_count);

        // }
        // else{
        //     //TODO:read from file and initialize first_key and second_key
        //     try (BufferedReader br = new BufferedReader(new FileReader(filePathString))) {
        //         String line1 = br.readLine();
        //         String line2 = br.readLine();
                
        //     }
        //     catch(Exception e){
        //         e.printStackTrace();
        //     }
        //     System.out.println("non_json_count is: " + non_json_count);
        // }

        System.out.println(first_keys.toString());
        System.out.println(second_keys.toString());

        BufferedWriter final_writer = null;
        try{
            final_writer = Files.newBufferedWriter(Paths.get("/Users/lutan/Desktop/ICBC/result.txt")); 
        }
        catch(Exception e){
            System.out.println("writing final exception");
        }

        System.out.println("begin writing");
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            long count = 0;
            while ((line = br.readLine()) != null) {
                //System.out.println("read line: " + line);
                String re_str = processLine(line);
                final_writer.write(re_str + "\n");                

                count ++;
                if(count % 1000000 == 0){
                    System.out.println("l:" + count);
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("non_json_count is: " + non_json_count);
    }

    public static String processLine(String line){
        String result = null;
        try{
            JsonObject json = (JsonObject)parser.parse(line);
            
            Iterator<String> first_keys_it = first_keys.iterator();
            while(first_keys_it.hasNext()){
                String key = (String)first_keys_it.next();
                if(!json.has(key)){
                    result = addToString(result, "");
                    continue;
                }

                //System.out.println("key is: " + key);
                if(second_keys.containsKey(key)){
                    //System.out.println("containsKey: ");
                    HashSet<String> set = second_keys.get(key);
                    Iterator<String> sub_keys_it = set.iterator();

                    JsonObject js = (JsonObject)json.get(key);
                    while(sub_keys_it.hasNext()){
                        String sub_key = sub_keys_it.next();
                        //System.out.println("sub key is: " + key);
                        if(!js.has(sub_key)){
                            result = addToString(result, "");
                            continue;
                        }
                        JsonElement e = js.get(sub_key);
                        //System.out.println("sub value is: " + e.getAsString());
                        if(!e.isJsonObject() && !e.isJsonNull()){
                            result = addToString(result, e.toString());
                        }
                        else{
                            if(e.isJsonNull()){
                                result = addToString(result, "");
                            }
                            else{
                                System.out.println("have third children");
                                System.out.println("key is: " + key);
                                System.out.println("json is: " + json.toString());
                                System.out.println("e is: " + e.toString());
                            }
                        }
                    }
                }
                else{
                    //System.out.println("not containsKey: ");
                    JsonElement e = json.get(key);
                    //System.out.println("value is: " + e.getAsString());
                    if(!e.isJsonObject() && !e.isJsonNull()){
                        result = addToString(result, e.toString());
                    }
                    else{
                        if(e.isJsonNull()){
                            result = addToString(result, "");
                        }
                        else{
                            System.out.println("something is wrong");
                            System.out.println("key is: " + key);
                            System.out.println("json is: " + json.toString());
                            System.out.println("e is: " + e.toString());
                        }
                        
                    }
                }
            }    
            
        }
        catch(Exception e){
            non_json_count += 1;
            e.printStackTrace();
        }
        //System.out.println("processed: " + result);
        return result;
    }

    public static String addToString(String line, String line1){
        String result = line;
        if(result == null){
            result = line1;
        }
        else{
            result = result + "," + line1;
        }
        return result;
    }

    public static void getAllTheKey(String line){
        try{
            JsonObject json = (JsonObject)parser.parse(line);
            Set<Map.Entry<String, JsonElement>> key_sets = json.entrySet();
            Iterator<Map.Entry<String, JsonElement>> iterator = key_sets.iterator();
                
            while(iterator.hasNext()){
                Map.Entry<String, JsonElement> map = (Map.Entry<String, JsonElement>)iterator.next();
                String key = map.getKey();
                first_keys.add(key);
                JsonElement element = map.getValue();
                if(element.isJsonObject()){
                    JsonObject sub_json = (JsonObject)element;
                    ArrayList<String> ks = getKeys(sub_json);
                    HashSet<String> s = null;
                    if(second_keys.containsKey(key)){
                        s = second_keys.get(key);
                    }
                    else{
                        s = new HashSet<String>();
                    }
                    for(String sub_key: ks){
                        s.add(sub_key);
                    }
                    second_keys.put(key, s);
                }
            }
        }
        catch(Exception e){
            non_json_count += 1;
        }
    }

    public static ArrayList<String> getKeys(JsonObject js){

        ArrayList<String> arr = new ArrayList<String>();
        Set<Map.Entry<String, JsonElement>> key_sets = js.entrySet();
        Iterator<Map.Entry<String, JsonElement>> iterator = key_sets.iterator();  

        while(iterator.hasNext()){
            Map.Entry<String,JsonElement> map = (Map.Entry<String, JsonElement>)iterator.next();
            String key = map.getKey();
            arr.add(key);
            JsonElement element = map.getValue();
            if(element.isJsonObject()){
                System.out.println("have third json objects");    
            }
        }
        return arr;
    }



}
