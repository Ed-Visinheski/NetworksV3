// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Eduardo Cook Visinheski
// 220057799
// eduardo.cook-visinheski@city.ac.uk

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

// DO NOT EDIT starts
interface TemporaryNodeInterface {

    public boolean start(String startingNodeName, String startingNodeAddress);
    public boolean store(String key, String value);
    public String get(String key);
}
// DO NOT EDIT ends

public class TemporaryNode implements TemporaryNodeInterface {
    private BufferedReader reader;
    private Writer writer;
    private Socket socket;
    private String version = "1";
    private String nodeName;
    private String contactNodeName;
    private String contactNodeAddress;
    private Random random = new Random();
    private String startingNodeName;

    public boolean start(String startingNodeName, String startingNodeAddress) {

        try {
            contactNodeName = startingNodeName;
            contactNodeAddress = startingNodeAddress;
            nodeName = "eduardo.cook-visinheski@city.ac.uk" + random.nextInt(10000);
            String[] parts = startingNodeAddress.split(":");
            if (parts.length != 2) {
                System.out.println("Invalid address format. Please use IP:Port format.");
                return false;
            }
            String ipAddress = parts[0];
            int port = Integer.parseInt(parts[1]);

            socket = new Socket(ipAddress, port);
            reader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
            writer = new OutputStreamWriter(this.socket.getOutputStream());

            //System.out.println("TemporaryNode connected to " + startingNodeName + " at " + ipAddress + ":" + port);
            String startMessage = "START 1 " + nodeName + "\n";
            writer.write(startMessage);
            writer.flush();
            System.out.println("Sending message: " + startMessage);
            String response = reader.readLine();
            System.out.println("Response from server: " + response);
            return true;
        } catch (IOException e) {
            System.out.println("Failed to connect: " + e.getMessage());
            return false;
        }
    }


    //6.4. PUT? Request
    //
    //   The requester MAY send a PUT request.  This will attempt to add a
    //   (key, value) pair to the hash table.  A PUT request is three or
    //   more lines.  The first line has two parts:
    //
    //   PUT? <number> <number>
    //
    //   The first number indicates the how many lines of key follow.  This MUST
    //   be at least one. The second number indicates how many line of value
    //   follow.  This MUST be at least one.
    //
    //   When the responder gets a PUT request it must compute the hashID
    //   for the value to be stored.  Then it must check the network
    //   directory for the three closest nodes to the key's hashID.  If the
    //   responder is one of the three nodes that are closest then
    //   it MUST store the (key, value) pair and MUST respond with a single
    //   line:
    //
    //   SUCCESS
    //
    //   If the responder finds three nodes that are closer to the hashID
    //   then it MUST refuse to store the value and MUST respond with a
    //   single line:
    //
    //   FAILED
    //
    //   For example if a requester sends:
    //
    //   PUT? 1 2
    //   Welcome
    //   Hello
    //   World!
    //
    //   The response might store the pair ("Welcome\n","Hello\nWorld!\n")
    //   and return
    //
    //   SUCCESS
    //
    //   or
    //
    //   FAILED
    //
    //   depending on the distance between the responder's hashID and the
    //   key's hashID and what other nodes are in its network directory.


    public boolean store(String key, String value) {
        try{
            //Calculate number of lines in the key and value
            String[] keyLines = key.split("\n");
            String[] valueLines = value.split("\n");
            String keyMessage = "PUT? " + keyLines.length + " " + valueLines.length + "\n";
            System.out.println("Key message: " + keyMessage);
            for (String line : keyLines) {
                keyMessage += line + "\n";
            }
            for (String line : valueLines) {
                keyMessage += line + "\n";
            }
            writer.write(keyMessage);
            writer.flush();
            System.out.println("Sending message: " + keyMessage);
            String response = reader.readLine();
            if(response.equals("SUCCESS")){
                writer.write("END Message Stored Successfully\n");
                writer.flush();
                closeConnection();
                return true;
            } else {
                writer.write("END Message Storage Failed\n");
                closeConnection();
                return false;
            }
        } catch (Exception e) {
            System.out.println("FAILED");
            closeConnection();
            return false;
        }
    }



    //6.5. GET? request
    //
    //   The requester MAY send a GET request.  This will attempt to find
    //   the value corresponding to the given key.  A GET request is two or
    //   more lines.  The first line is two parts:
    //
    //   GET? <number>
    //
    //   The number is the number of lines of key that follow.  This MUST be
    //   more than one.  The responder MUST see if it has a value stored for
    //   that key. If it does it MUST return a VALUE response.  A VALUE
    //   response is two or more lines.  The first line has two parts:
    //
    //   VALUE <number>
    //
    //   The number indicates the number of lines in the value.  This MUST
    //   be at least one.  The second part of the VALUE response is the
    //   value that is stored for the key.
    //
    //   If the responder does not have a value stored which has the
    //   requested key, it must respond with a single line:
    //
    //   NOPE
    //
    //   For example if a requester sends:
    //
    //   GET? 1
    //   Welcome
    //
    //   Then the response would either be:
    //
    //   VALUE 2
    //   Hello
    //   World!
    //
    //   or
    //
    //   NOPE

    public String get(String key){
        try {
            System.out.println("Getting key: " + key);
            HashID hasher = new HashID();
            byte[] hashedKey = hasher.computeHashID(key);
            String hashedKeyString = hashedKey.toString();
            System.out.println("Hashed key: " + hashedKeyString);
            String[] keyLines = key.split("\n");
            String keyMessage = "GET? " + keyLines.length + "\n";
            for (String line : keyLines) {
                keyMessage += line + "\n";
            }
            writer.write(keyMessage);
            writer.flush();
            String response = reader.readLine();
            String[] responseParts = response.split(" ");
            System.out.println("Response: " + response);
            if(responseParts[0].equals("VALUE")){
                int valueLines = Integer.parseInt(responseParts[1]);
                String value = "";
                for (int i = 0; i < valueLines; i++) {
                    value += reader.readLine() + "\n";
                }
                writer.write("END Message Retrieved Successfully\n");
                writer.flush();
                closeConnection();
                return value;
            } else {
                System.out.println("Key not found. Looking for nearest nodes.");
                writer.write("NEAREST? " + hashedKeyString + "\n");
                writer.flush();
                String nearestResponse = reader.readLine();
                String[] nearestParts = nearestResponse.split(" ");
                System.out.println("Nearest response: " + nearestResponse);
                if(nearestParts[0].equals("NODES")){
                    Map<String, String> nearestNodesMap = new HashMap<>();
                    int nearestLines = Integer.parseInt(nearestParts[1]);
                    for (int i = 0; i < nearestLines; i++) {
                        String nodeName = reader.readLine();
                        String nodeAddress = reader.readLine();
                        nearestNodesMap.put(nodeName, nodeAddress);
                        System.out.println("Node: " + nodeName + " Address: " + nodeAddress);
                    }
                    while(nearestNodesMap.size() > 0){
                        String nearestNodeName = nearestNodesMap.keySet().iterator().next();
                        String nearestNodeAddress = nearestNodesMap.get(nearestNodeName);
                        nearestNodesMap.remove(nearestNodeName);
                        String[] parts = nearestNodeAddress.split(":");
                        if (parts.length != 2) {
                            System.out.println("Invalid address format. Please use IP:Port format.");
                            return null;
                        }else {
                            String ipAddress = parts[0];
                            int port = Integer.parseInt(parts[1]);
                            Socket getSocket = new Socket(ipAddress, port);
                            BufferedReader getReader = new BufferedReader(new InputStreamReader(getSocket.getInputStream()));
                            Writer getWriter = new OutputStreamWriter(getSocket.getOutputStream());
                            getWriter.write(keyMessage);
                            getWriter.flush();
                            String nearestResponse2 = getReader.readLine();
                            String[] nearestParts2 = nearestResponse2.split(" ");
                            if (nearestParts2[0].equals("VALUE")) {
                                int valueLines = Integer.parseInt(nearestParts2[1]);
                                String value = "";
                                for (int i = 0; i < valueLines; i++) {
                                    value += getReader.readLine() + "\n";
                                }
                                getWriter.write("END Message Retrieved Successfully\n");
                                getWriter.flush();
                                getReader.close();
                                getWriter.close();
                                getSocket.close();
                                return value;
                            }
                        }
                    }
                    writer.write("END Message Not Found\n");
                    writer.flush();
                    closeConnection();
                    return null;
                }
                else {
                    writer.write("END Message Not Found\n");
                    writer.flush();
                    closeConnection();
                    return null;
                }
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void closeConnection() {
        try {
            if (writer != null) writer.close();
            if (reader != null) reader.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error closing network resources: " + e.getMessage());
        }
    }



}
