package edu.yu.cs.com3800.stage1;

import org.junit.*;
import org.junit.Test;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertTrue;

public class Stage1Test {
    static SimpleServerImpl server;
    public String[] sendRequest(String contentType, Map<String, String> params, String body, String context,
                                String method) throws IOException {
        HttpURLConnection server = null;
        String response;
        int responseCode;
        try {
            URL url = new URL("http://localhost:9000" + context);
            server = (HttpURLConnection) url.openConnection();
            server.setRequestProperty("Content-type", contentType);
            server.setRequestMethod(method);
            server.setDoOutput(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] data = body.getBytes(StandardCharsets.UTF_8);
        InputStream errorInputStream = null;
        InputStream normalInputStream = null;
        OutputStream outputStream = null;
        byte[] responseByte = null;
        byte[] errorResponse = null;


        outputStream = server.getOutputStream();
        outputStream.write(data);
        try {
            normalInputStream = server.getInputStream();
            responseByte = normalInputStream.readAllBytes();
            response = new String(responseByte);
            responseCode = server.getResponseCode();
            return new String[]{String.valueOf(responseCode), response};
        } catch (IOException e){
            errorInputStream = server.getErrorStream();
            errorResponse = errorInputStream.readAllBytes();
            response = new String(errorResponse);
            responseCode = server.getResponseCode();
            return new String[]{String.valueOf(responseCode), response};
        }
    }

    @BeforeClass
    public static void before() throws IOException {
        server = new SimpleServerImpl(9000);
        server.start();
    }
    @AfterClass
    public static void after() throws IOException {
        server.stop();
    }

    @Test(expected = IllegalArgumentException.class)
    public void clientSendNullCompileAndRunRequest() throws IOException, InterruptedException {
        ClientImpl client = new ClientImpl("localhost", 9000);
        System.out.println("Expected response:\n IllegalArgumentException");
        client.sendCompileAndRunRequest(null);
    }
    @Test
    public void clientGetResponseBeforeSendingRequest() throws IOException, InterruptedException {
        ClientImpl client = new ClientImpl("localhost", 9000);
        System.out.println("Expected response:\n null");
        assertNull(client.getResponse());
        System.out.println("Actual response:\n "+ client.getResponse());
    }
    @Test
    public void clientSendBadCode() throws IOException, InterruptedException {
        ClientImpl client = new ClientImpl("localhost", 9000);
        client.sendCompileAndRunRequest("fake");
        Client.Response response = client.getResponse();
        System.out.println("Expected response:\n 400)");
        System.out.println("Actual response:\n "+ client.getResponse().getCode());

        System.out.println("Expected response:\n contains(\"No class name found in code\")");
        System.out.println("Actual response:\n "+ client.getResponse().getBody());
        assertEquals(response.getCode(), 400);
        assertTrue(response.getBody().contains("No class name found in code"));
    }
    @Test
    public void clientSendCodeWithCompileError() throws IOException, InterruptedException {
        //note the error on line 3 (Strng is a typo)
        String code = "public class Test { " +
                "\npublic Test(){} " +
                "\npublic Strng run(){ return \"hi\";}}";
        ClientImpl client = new ClientImpl("localhost", 9000);
        client.sendCompileAndRunRequest(code);
        Client.Response response = client.getResponse();
        assertEquals(response.getCode(), 400);
        assertTrue(response.getBody().contains("Error on line 3"));
        System.out.println("Expected response:\n 400)");
        System.out.println("Actual response:\n "+ client.getResponse().getCode());
        System.out.println("Expected response:\n contains(Error on line 3)");
        System.out.println("Actual response:\n "+ client.getResponse().getBody());
    }
    @Test
    public void clientSendCodeCorrect() throws IOException, InterruptedException {
        //note the error on line 3 (Strng is a typo)
        String code = "public class Test { " +
                "\npublic Test(){} " +
                "\npublic String run(){ return \"hello world!\";}}";
        ClientImpl client = new ClientImpl("localhost", 9000);
        client.sendCompileAndRunRequest(code);
        Client.Response response = client.getResponse();
        assertEquals(response.getCode(), 200);
        assertTrue(response.getBody().contains("hello world!"));
        System.out.println("Expected response:\n 200)");
        System.out.println("Actual response:\n "+ client.getResponse().getCode());
        System.out.println("Expected response:\n contains(hello world!)");
        System.out.println("Actual response:\n "+ client.getResponse().getBody());
    }
    @Test
    public void clientLastsMoreThanOneRequest() throws IOException, InterruptedException {
        //note the error on line 3 (Strng is a typo)
        String code = "public class Test { " +
                "\npublic Test(){} " +
                "\npublic String run(){ return \"hello world!\";}}";
        ClientImpl client = new ClientImpl("localhost", 9000);
        client.sendCompileAndRunRequest(code);
        Client.Response response = client.getResponse();
        assertEquals(response.getCode(), 200);
        assertTrue(response.getBody().contains("hello world!"));
        System.out.println("Expected response:\n 200)");
        System.out.println("Actual response:\n "+ client.getResponse().getCode());
        System.out.println("Expected response:\n contains(hello world!)");
        System.out.println("Actual response:\n "+ client.getResponse().getBody());
        //second run
        client = new ClientImpl("localhost", 9000);
        client.sendCompileAndRunRequest(code);
        response = client.getResponse();
        assertEquals(response.getCode(), 200);
        assertTrue(response.getBody().contains("hello world!"));
        System.out.println("Expected response:\n 200)");
        System.out.println("Actual response:\n "+ client.getResponse().getCode());
        System.out.println("Expected response:\n contains(hello world!)");
        System.out.println("Actual response:\n "+ client.getResponse().getBody());
    }

    @Test
    public void contentTypeNotJava() throws IOException, InterruptedException {
        String[] ret = sendRequest("test", new HashMap<>(), "this is a non-code body", "/compileandrun", "POST");
        assertEquals("400", ret[0]);
        assertEquals("", ret[1]);
        System.out.println("Expected response:\n 400)");
        System.out.println("Actual response:\n "+ ret[1]);
        System.out.println("Expected response:\n ");
        System.out.println("Actual response:\n "+ ret[1]);
    }
    @Test
    public void invalidContext() throws IOException, InterruptedException {
        String[] ret = sendRequest("test", new HashMap<>(), "this is a non-code body", "/fake", "POST");
        assertEquals("404", ret[0]);
        assertTrue(ret[1].contains("No context found"));
        System.out.println("Expected response:\n 404)");
        System.out.println("Actual response:\n "+ ret[1]);
        System.out.println("Expected response:\n \"No context found\" ");
        System.out.println("Actual response:\n "+ ret[1]);
    }
    @Test
    public void contentTypeEmpty() throws IOException, InterruptedException {
        String[] ret = sendRequest("", new HashMap<>(), "this is a non-code body", "/compileandrun", "POST");
        assertEquals("400", ret[0]);
        assertEquals("", ret[1]);
        System.out.println("Expected response:\n 400)");
        System.out.println("Actual response:\n "+ ret[1]);
        System.out.println("Expected response:\n ");
        System.out.println("Actual response:\n "+ ret[1]);
    }
    @Test
    public void contentTypeIsJavaBadCode() throws IOException, InterruptedException {
        String[] ret = sendRequest("text/x-java-source", new HashMap<>(), "this is a non-code body", "/compileandrun", "POST");
        assertEquals("400", ret[0]);
        assertTrue(ret[1].contains("No class name found in code"));
        System.out.println("Expected response:\n 400)");
        System.out.println("Actual response:\n "+ ret[1]);
        System.out.println("Expected response:\n contains(\"No class name found in code\") ");
        System.out.println("Actual response:\n "+ ret[1]);
    }
    @Test
    public void contentTypeIsJavaBadCodeSpecificLineNumber() throws IOException, InterruptedException {
        //note the error on line 3 (Strng is a typo)
        String code = "public class Test { " +
                "\npublic Test(){} " +
                "\npublic Strng run(){ return \"hi\";}}";
        String[] ret = sendRequest("text/x-java-source", new HashMap<>(), code, "/compileandrun", "POST");
        assertEquals("400", ret[0]);
        assertTrue(ret[1].contains("Error on line 3"));
        System.out.println("Expected response:\n 400)");
        System.out.println("Actual response:\n "+ ret[1]);
        System.out.println("Expected response:\n contains(\"Error on line 3\") ");
        System.out.println("Actual response:\n "+ ret[1]);
    }
    @Test
    public void contentTypeIsJavaValidCode() throws IOException, InterruptedException {
        String code = "public class Test { public Test(){} public String run(){ return \"hello world!\";}}";

        String[] ret = sendRequest("text/x-java-source", new HashMap<>(), code, "/compileandrun", "POST");
        assertEquals("200", ret[0]);
        assertTrue(ret[1].contains("hello world!"));
        System.out.println("Expected response:\n 200)");
        System.out.println("Actual response:\n "+ ret[1]);
        System.out.println("Expected response:\n contains(\"hello world!\") ");
        System.out.println("Actual response:\n "+ ret[1]);
    }
}
