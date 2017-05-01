package ru.spbu.apmath.st033672;

import com.google.gson.*;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;



public class CouchdbConnector{

    private static Gson gson = new Gson();

    private String ip;
    private String port;
    private String dbName;
    private String userName;
    private String userPassword;

    DefaultHttpClient httpclient = new DefaultHttpClient();

    public CouchdbConnector(String ip, String port, String dbName, String userName, String userPassword){
        this.ip = ip;
        this.port = port;
        this.dbName = dbName;
        this.userName = userName;
        this.userPassword = userPassword;
    }


    public void write(Object obj) throws IllegalArgumentException, IOException{

        String json = gson.toJson(obj);
        //System.out.println(json);

        writeJson(json);

    }


    public void writeJson(String json) throws IllegalArgumentException, IOException{
        String uuid;
        String request = "http://" + userName + ":" + userPassword + "@"
                + ip + ":" + port + "/_uuids";
        //System.out.println(request);

        HttpGet httpGet = new HttpGet(request);
        try{
            HttpResponse response1 = httpclient.execute(httpGet);
            String reasonPhrase = response1.getStatusLine().getReasonPhrase();
            //System.out.println(reasonPhrase);
            if( !"OK".equals(response1.getStatusLine().getReasonPhrase()) ){
                throw new IOException("reasonPhrase: " + reasonPhrase);
            }
            HttpEntity entity1 = response1.getEntity();
            try{
                Scanner scanner = new Scanner(entity1.getContent());
                try{
                    String tmp = scanner.next();
                    //System.out.println(tmp);
                    uuid = tmp.substring(tmp.indexOf("[") + 2,tmp.indexOf("]")-1);
                    //System.out.println(uuid);
                }finally{
                    scanner.close();
                }
            }finally{
                EntityUtils.consume(entity1);
            }
        }finally{
            httpGet.releaseConnection();
        }

        request = "http://" + userName + ":" + userPassword + "@"+ ip + ":" + port + "/" + dbName + "/" + uuid;
        //System.out.println(request);

        HttpPut httpPut = new HttpPut(request);
        try{
            StringEntity entity2 = new StringEntity( json, StandardCharsets.UTF_8);
            httpPut.setEntity(entity2);
            HttpResponse response1 = httpclient.execute(httpPut);
            String reasonPhrase = response1.getStatusLine().getReasonPhrase();
            //System.out.println(reasonPhrase);
            if( !"Created".equals(response1.getStatusLine().getReasonPhrase()) ){
                System.out.println(response1.getStatusLine());
                throw new IOException("reasonPhrase:" + reasonPhrase);
            }
            //System.out.println(response1.getStatusLine());
        }finally{
            httpPut.releaseConnection();
        }
    }


    public <T> void writeBulk(List<T> list)throws IOException{

        String request = "http://" + userName + ":" + userPassword + "@"
                + ip + ":" + port + "/" + dbName + "/_bulk_docs";
        //System.out.println(request);

        StringBuilder sb = new StringBuilder( "{\"docs\":[" );

        Iterator<T> it = list.iterator();
        if( it.hasNext() ){
            sb.append(gson.toJson(it.next()));
        }
        while( it.hasNext() ){
            sb.append("," + gson.toJson(it.next()));
        }
        sb.append("]}");
        String json = sb.toString();

        //System.out.println(json);



        StringEntity entity = new StringEntity( json, StandardCharsets.UTF_8);

        HttpPost httpPost = new HttpPost(request);
        try{
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-Type", "application/json");
            HttpResponse response = httpclient.execute(httpPost);
            String reasonPhrase = response.getStatusLine().getReasonPhrase();
            //System.out.println(reasonPhrase);
            if( !"Created".equals(response.getStatusLine().getReasonPhrase()) ){
                throw new IOException("reasonPhrase: " + reasonPhrase);
            }

            /*
            try{
                Scanner sc = new Scanner(response.getEntity().getContent());
                while( sc.hasNext() ) System.out.println(sc.next());
            }catch (Exception e) {
                e.printStackTrace();
            }
            */

        }finally{
            httpPost.releaseConnection();
        }


    }


    public void writeBulkJsons(List<String> jsons)throws IOException {
        String request = "http://" + userName + ":" + userPassword + "@"
                + ip + ":" + port + "/" + dbName + "/_bulk_docs";
        //System.out.println(request);

        StringBuilder sb = new StringBuilder( "{\"docs\":[" );

        Iterator<String> it = jsons.iterator();
        if( it.hasNext() ){
            sb.append(it.next());
        }
        while( it.hasNext() ){
            sb.append("," + it.next());
        }
        sb.append("]}");
        String json = sb.toString();

        //System.out.println(json);



        StringEntity entity = new StringEntity( json, StandardCharsets.UTF_8);

        HttpPost httpPost = new HttpPost(request);
        try{
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-Type", "application/json");
            HttpResponse response = httpclient.execute(httpPost);
            String reasonPhrase = response.getStatusLine().getReasonPhrase();
            //System.out.println(reasonPhrase);
            if( !"Created".equals(response.getStatusLine().getReasonPhrase()) ){
                throw new IOException("reasonPhrase: " + reasonPhrase);
            }

            /*
            try{
                Scanner sc = new Scanner(response.getEntity().getContent());
                while( sc.hasNext() ) System.out.println(sc.next());
            }catch (Exception e) {
                e.printStackTrace();
            }
            */

        }finally{
            httpPost.releaseConnection();
        }

    }



}
