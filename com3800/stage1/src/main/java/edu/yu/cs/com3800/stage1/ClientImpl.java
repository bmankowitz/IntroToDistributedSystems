package edu.yu.cs.com3800.stage1;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ClientImpl implements Client {
    HttpURLConnection server;
    String response;
    int responseCode;
    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        try {
            URL url = new URL("http://" + hostName + ":" +hostPort + "/compileandrun");
            server = (HttpURLConnection) url.openConnection();
            server.setRequestProperty("Content-type", "text/x-java-source");
            server.setRequestMethod("POST");
            server.setDoOutput(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sendCompileAndRunRequest(String src) throws IOException {
        if(src == null) throw new IllegalArgumentException();
        byte[] data = src.getBytes(StandardCharsets.UTF_8);
        InputStream errorInputStream = null;
        InputStream normalInputStream = null;
        OutputStream outputStream = null;
        byte[] response = null;
        byte[] errorResponse = null;

            outputStream = server.getOutputStream();
            outputStream.write(data);
            try {
                normalInputStream = server.getInputStream();
                response = normalInputStream.readAllBytes();
                this.response = new String(response);
                this.responseCode = server.getResponseCode();
//                System.out.println(this.response);
            } catch (IOException e){
                errorInputStream = server.getErrorStream();
                errorResponse = errorInputStream.readAllBytes();
                this.response = new String(errorResponse);
                this.responseCode = server.getResponseCode();
            }

    }

    @Override
    public Response getResponse() throws IOException {
        if(response == null || responseCode == 0) return null;
        return new Response(responseCode,response);
    }
}
