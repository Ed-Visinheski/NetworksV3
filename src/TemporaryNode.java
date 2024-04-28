// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Eduardo Cook Visinheski
// 220057799
// eduardo.cook-visinheski@city.ac.uk

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
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
    private String nodeName;
    private Random random = new Random();
    private String startingNodeName;

    public boolean start(String startingNodeName, String startingNodeAddress) {

        try {
            nodeName = "eduardo.cook-visinheski@city.ac.uk:MyTemporaryNode-Implementation," + random.nextInt(99999);
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

            String startMessage = "START 1 " + nodeName + "\n";
            writer.write(startMessage);
            writer.flush();
            System.out.println("Sending message: " + startMessage);
            String response = reader.readLine();
            System.out.println("Response from" + startingNodeName+" : " + response);
            return true;
        } catch (IOException e) {
            System.out.println("Failed to connect: " + e.getMessage());
            return false;
        }
    }
    public boolean store(String key, String value) {
        try{
            if (socket == null || socket.isClosed() || !socket.isConnected()) {
                System.err.println("Socket is closed or not connected.");
                return false;
            }

            System.out.println("Storing key: " + key + " with value: " + value + " in node: " + nodeName);
            HashID hasher = new HashID();
            byte[] hashedKey = hasher.computeHashID(key);
            String hashedKeyString = hasher.bytesToHex(hashedKey);
            String[] keyLines = key.split("\n");
            String[] valueLines = value.split("\n");
            String keyMessage = "PUT? " + keyLines.length + " " + valueLines.length + "\n";
            for (String line : keyLines) {
                keyMessage += line + "\n";
            }
            for (String line : valueLines) {
                keyMessage += line + "\n";
            }

            writer.write(keyMessage);
            writer.flush();

            String response = reader.readLine();
            if (response == null) {
                System.out.println("Response from server is null, possibly connection was closed.");
                return false;
            }

            System.out.println("Response from server "+ startingNodeName+ ":\n"+response);
            if (response.equals("SUCCESS")) {
                writer.write("END Message Stored\n");
                writer.flush();
                closeConnection();
                return true;
            } else {
                return handleNearestNodes(hashedKeyString, keyMessage, "PUT?") != null;
            }
        } catch (UnknownHostException e) {
            System.err.println("Unknown host: " + e.getMessage());
            return false;
        } catch (IOException e) {
            System.err.println("IO error: " + e.getMessage());
            return false;
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            return false;
        } finally {
            if(socket != null && !socket.isClosed()){
                closeConnection();
            }
        }
    }

    public String get(String key){
        try {
            if (socket == null || socket.isClosed() || !socket.isConnected()) {
                System.err.println("END Connection Closed\n");
                writer.write("END Connection Closed\n");
                writer.flush();
                reader.readLine();
                closeConnection();
                return null;
            }

            System.out.println("Getting key: " + key + " from node: " + nodeName);
            HashID hasher = new HashID();
            byte[] hashedKey = hasher.computeHashID(key);
            String hashedKeyString = hasher.bytesToHex(hashedKey);
            System.out.println("Hashed key: " + hashedKeyString);

            String[] keyLines = key.split("\n");
            String keyMessage = "GET? " + keyLines.length + "\n";
            for (String line : keyLines) {
                keyMessage += line + "\n";
            }

            writer.write(keyMessage);
            writer.flush();

            String response = reader.readLine();
            if (response == null) {
                System.out.println("END Connection Closed to node: " + nodeName + "\n");
                writer.write("END Connection Closed\n");
                writer.flush();
                reader.readLine();
                closeConnection();
                return null;
            }

            System.out.println("Response from node "+ startingNodeName + ":\n"+response);
            String[] responseParts = response.split(" ");
            if (responseParts[0].equals("VALUE")) {
                return handleValueResponse(reader, Integer.parseInt(responseParts[1]));
            } else {
                return handleNearestNodes(hashedKeyString, keyMessage, "GET?");
            }
        } catch (UnknownHostException e) {
            System.err.println("Unknown host: " + e.getMessage());
            return null;
        } catch (IOException e) {
            System.err.println("IO error: " + e.getMessage());
            return null;
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            return null;
        } finally {
            if(socket != null && !socket.isClosed()){
                closeConnection();
            }
        }
    }

    private String handleNearestNodes(String hashedKeyString, String keyMessage, String operation) throws IOException {
        writer.write("NEAREST? " + hashedKeyString + "\n");
        writer.flush();

        String nearestResponse = reader.readLine();
        if (nearestResponse == null) {
            System.out.println("Nearest node response is null, connection may have been closed.");
            writer.write("END Connection Closed\n");
            writer.flush();
            reader.readLine();
            closeConnection();
            return null;
        }

        System.out.println("Nearest response from node "+ startingNodeName + ":\n"+nearestResponse);
        String[] nearestParts = nearestResponse.split(" ");
        if (!nearestParts[0].equals("NODES")) {
            System.out.println("Unexpected response type for NEAREST request.");
            writer.write("END Unexpected Response\n");
            writer.flush();
            reader.readLine();
            closeConnection();
            return null;
        }

        Map<Integer, Map<String, String>> nearestNodesMap = new HashMap<>();
        int nearestLines = Integer.parseInt(nearestParts[1]);
        for (int i = 0; i < nearestLines; i++) {
            String nodeDetails = reader.readLine();
            String nodeAddressPort = reader.readLine();
            System.out.println("Node name: " + nodeDetails);
            System.out.println("Node address: " + nodeAddressPort);
            if (nodeDetails == null) break;
            String[] nodeData = nodeAddressPort.split(":");
            if (nodeData.length != 2) {
                System.err.println("Invalid node address format.");
                continue;
            }
            int distance = HashID.calculateDistance(hashedKeyString.getBytes(), nodeData[0].getBytes());
            nearestNodesMap.put(distance, Map.of(nodeDetails, nodeAddressPort));
        }

        if(operation.equals("GET?")){
            return handleGetNearest(nearestNodesMap, keyMessage);
        } if(operation.equals("PUT?")) {
            return handleStoreNearest(nearestNodesMap, keyMessage);
        }
        System.out.println("Value not found in any nearest nodes.");
        writer.write("END Value Not Found\n");
        writer.flush();
        reader.readLine();
        closeConnection();
        return null;
    }

    private String handleStoreNearest(Map<Integer, Map<String, String>> nearestNodesMap, String keyMessage) throws IOException {
        for(Map.Entry<Integer, Map<String, String>> entry : nearestNodesMap.entrySet()){
            String nodeAddress = entry.getValue().values().iterator().next();
            String[] nodeData = nodeAddress.split(":");
            try (Socket nodeSocket = new Socket(nodeData[0], Integer.parseInt(nodeData[1]));
                 BufferedReader nodeReader = new BufferedReader(new InputStreamReader(nodeSocket.getInputStream()));
                 Writer nodeWriter = new OutputStreamWriter(nodeSocket.getOutputStream())) {
                nodeWriter.write("START 1 " + nodeName + "\n");
                nodeWriter.flush();
                System.out.println("Sending START message to nearest node: " + entry.getKey() + ":" + entry.getValue());
                String nodeResponse = nodeReader.readLine();
                System.out.println("Response from node " + entry.getKey() + ": \n"+ nodeResponse);
                if(nodeResponse.contains("START")) {
                    nodeWriter.write(keyMessage);
                    nodeWriter.flush();
                    System.out.println("Sending PUT request to nearest node: " + entry.getKey() + ":" + entry.getValue());

                    nodeResponse = nodeReader.readLine();
                    System.out.println("Response from nearest node: " + nodeResponse);
                    if (nodeResponse.contains("SUCCESS")) {
                        nodeWriter.write("END Message Stored\n");
                        nodeWriter.flush();
                        reader.readLine();
                        nodeSocket.close();
                        nodeReader.close();
                        nodeWriter.close();
                        closeConnection();
                        return "Message stored successfully in node: " + entry.getKey() + ":" + entry.getValue();
                    }
                }
            } catch (IOException e) {
                System.err.println("Failed to connect or communicate with node: " + entry.getKey() + ":" + entry.getValue());
                closeConnection();
            }
        }
        System.out.println("Failed to store message in any nearest nodes.");
        writer.write("END Failed to Store Message\n");
        writer.flush();
        reader.readLine();
        closeConnection();
        return null;
    }

    public String handleGetNearest(Map<Integer, Map<String, String>> nearestNodesMap, String keyMessage){
        for(Map.Entry<Integer, Map<String, String>> entry : nearestNodesMap.entrySet()){
            String nodeAddress = entry.getValue().values().iterator().next();
            String[] nodeData = nodeAddress.split(":");
            try (Socket nodeSocket = new Socket(nodeData[0], Integer.parseInt(nodeData[1]));
                 BufferedReader nodeReader = new BufferedReader(new InputStreamReader(nodeSocket.getInputStream()));
                 Writer nodeWriter = new OutputStreamWriter(nodeSocket.getOutputStream())) {
                nodeWriter.write("START 1 " + nodeName + "\n");
                nodeWriter.flush();
                System.out.println("Sending START message to nearest node: " + entry.getKey() + ":" + entry.getValue());
                String nodeResponse = nodeReader.readLine();
                System.out.println("Response from nearest node: " + nodeResponse);
                if(nodeResponse.contains("START")) {
                    nodeWriter.write(keyMessage);
                    nodeWriter.flush();
                    System.out.println("Sending GET request to nearest node: " + entry.getKey() + ":" + entry.getValue());
                    nodeResponse = nodeReader.readLine();
                    System.out.println("Response from nearest node: " + entry.getKey() + ":" + entry.getValue() + ":\n" + nodeResponse);
                    if (nodeResponse.contains("VALUE")) {
                        return handleValueResponse(nodeReader, Integer.parseInt(nodeResponse.split(" ")[1]));
                    }
                }
            } catch (IOException e) {
                System.err.println("Failed to connect or communicate with node: " + entry.getKey() + ":" + entry.getValue());
                closeConnection();
            }
        }
        System.out.println("Value not found in any nearest nodes.");
        return null;
    }

    private String handleValueResponse(BufferedReader reader, int valueLines) throws IOException {
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < valueLines; i++) {
            value.append(reader.readLine());
            if (i < valueLines - 1) value.append("\n");
        }
        return value.toString();
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
