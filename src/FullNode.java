// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// YOUR_NAME_GOES_HERE
// YOUR_STUDENT_ID_NUMBER_GOES_HERE
// YOUR_EMAIL_GOES_HERE


import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.util.*;

// DO NOT EDIT starts
interface FullNodeInterface {
    public boolean listen(String ipAddress, int portNumber);
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress);
}
// DO NOT EDIT ends

public class FullNode implements FullNodeInterface{

    private Map<Integer, Map<String, String>> networkMap = new HashMap<>();
    // Map for the Key and the value
    private Map<String, String> keyValueMap = new HashMap<>();
    private ServerSocket serverSocket;
    private String nodeName;
    private String address;
    private String currentNodeName;
    private String currentNodeAddress;
    private Random random = new Random();

    public boolean listen(String ipAddress, int portNumber) {
        try{
            //Add Threads to handle multiple connections
            this.nodeName = "eduardo.cook-visinheski@city.ac.uk:FullNode," + random.nextInt(10000);
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(ipAddress, portNumber));
            System.out.println("FullNode listening on " + ipAddress + ":" + portNumber);
            return true;
        } catch (IOException e) {
            System.out.println("Could not listen on " + ipAddress + ":" + portNumber);
            return false;
        }
    }


    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
        try {
            connectToStartingNode(startingNodeName, startingNodeAddress);
            Thread thread = new Thread(() -> {
                while (true) {
                    try {
                        Socket socket = serverSocket.accept();
                        HandleClient(socket, startingNodeName, startingNodeAddress);
                    } catch (IOException e) {
                        System.out.println("Error accepting connections.");
                    }
                }
            });
            thread.start();
        } catch (Exception e) {
            System.out.println("Error creating thread.");
        }
    }


    public void stop() {
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.out.println("Error closing the server.");
        }
    }

    public void HandleClient(Socket socket, String startingNodeName, String startingAddress) {
        try {
            HashID hashID = new HashID();
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            writer.write("START 1 " + nodeName + "\n");
            writer.flush();
            System.out.println("Sent START message to " + startingNodeName);
            String line = reader.readLine();
            String[] parts = line.split(" ");
            if (parts.length != 3) {
                System.out.println("Invalid START message.");
                writer.write("END Invalid START message\n");
                writer.flush();
                return;
            }
            String version = parts[1];
            System.out.println("Received START message from " + startingNodeName+ " with version " + version);
            while ((!reader.readLine().contains("END") && !socket.isClosed())) {
                line = reader.readLine();
                parts = line.split(" ");
                switch (parts[0]){
                    case"NOTIFY":{
                        String name = parts[1];
                        String address = parts[2];
                        AddToNetworkMap(name, address);
                        writer.write("NOTIFIED\n");
                        break;
                    }
                    case"PUT?": {
                        int keyLines = Integer.parseInt(parts[1]);
                        int valueLines = Integer.parseInt(parts[2]);
                        String key = "";
                        String value = "";
                        for(int i = 0; i < keyLines; i++){
                            key += reader.readLine()+"\n";
                        }
                        for(int i = 0; i < valueLines; i++){
                            value += reader.readLine()+"\n";
                        }
                        byte[] keyHashID = hashID.computeHashID(key);
                        if (calculateStoreDistance(keyHashID)) {
                            //Store the key and value on the current node
                            keyValueMap.put(key, value);
                            writer.write("SUCCESS\n");
                            writer.flush();
                        } else {
                            //Find the closest nodes to the keyHashID
                            Map<String, String> closestNodes = findClosestToKey(keyHashID);
                            //Send the key and value to the closest node
                            for (String node : closestNodes.keySet()) {
                                String[] nodeParts = node.split(":");
                                String nodeAddress = closestNodes.get(node);
                                Socket closestNodeSocket = new Socket(nodeParts[0], Integer.parseInt(nodeParts[1]));
                                BufferedWriter closestNodeWriter = new BufferedWriter(new OutputStreamWriter(closestNodeSocket.getOutputStream()));
                                closestNodeWriter.write("PUT? " + key + " " + parts[2] + "\n");
                                closestNodeWriter.flush();
                                closestNodeSocket.close();
                            }
                        }
                        break;
                    }

                    case "GET?": {
                        String keyToGet = parts[1];
                        if (keyValueMap.containsKey(keyToGet)) {
                            String[] valueSlit = keyValueMap.get(keyToGet).split("\n");
                            writer.write("VALUE " + valueSlit.length + "\n" + keyValueMap.get(keyToGet));
                            writer.flush();
                        } else {
                            findClosestToKey(hashID.computeHashID(keyToGet));

                        }
                        break;
                    }
                    case "END": {
                        System.out.println("Received END message from " + startingNodeName);
                        socket.close();
                        break;
                    }
                    case "ECHO?":{
                        writer.write("OHCE\n");
                        writer.flush();
                        break;
                    }
                    case "NEAREST?":{
                        byte[] keyHashID = hashID.computeHashID(parts[1]);
                        Map<String, String> closestNodes = findClosestToKey(keyHashID);
                        for (String node : closestNodes.keySet()) {
                            writer.write("NEAREST! " + node + " " + closestNodes.get(node) + "\n");
                            writer.flush();
                        }
                        break;
                    }
                }
            }
    } catch (IOException e) {
            System.out.println("Error handling client.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public boolean calculateStoreDistance(byte[] keyHashID) {
        try {
            HashID hasher = new HashID();
            int networkSize = networkMap.size();
            int portion = 256 / networkSize;
            byte[] currentNodeHashID = hasher.computeHashID(currentNodeName);
            //calculates distance between the current node and the keyHashID
            int keyDistance = hasher.calculateDistance(currentNodeHashID, keyHashID);
            if (keyDistance <= portion) {
                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            System.out.println("Error calculating distance.");
        }
        return true;
    }

    public void AddToNetworkMap(String name, String address) {
        try {
            HashID hasher = new HashID();
            byte[] nodeHashID = hasher.computeHashID(name);
            int distance = hasher.calculateDistance(nodeHashID, hasher.computeHashID(currentNodeName));
            if (networkMap.containsKey(distance)) {
                Map<String, String> nodes = networkMap.get(distance);
                if (nodes.size() < 3) {
                    nodes.put(name, address);
                } else {
                    // Find the node with the highest distance
                    int maxDistance = 0;
                    String maxNode = "";
                    for (String node : nodes.keySet()) {
                        int nodeDistance = hasher.calculateDistance(nodeHashID, hasher.computeHashID(node));
                        if (nodeDistance > maxDistance) {
                            maxDistance = nodeDistance;
                            maxNode = node;
                        }
                    }
                    if (maxDistance > distance) {
                        nodes.remove(maxNode);
                        nodes.put(name, address);
                    }
                }
            } else {
                Map<String, String> nodes = new HashMap<>();
                nodes.put(name, address);
                networkMap.put(distance, nodes);
            }
        } catch (Exception e) {
            System.out.println("Error adding to network map.");
        }
    }

    public Map<String, String> findClosestToKey (byte[] keyHashID) {
        try {
            HashID hasher = new HashID();
            int distance = hasher.calculateDistance(hasher.computeHashID(currentNodeName), keyHashID);
            //Find the closest nodes to the keyHashID
            if(!networkMap.containsKey(distance)){
                while(!networkMap.containsKey(distance) && distance > 0){
                    distance--;
                }
                return networkMap.get(distance);
            }
            else{
                return networkMap.get(distance);
            }
        } catch (Exception e) {
            System.out.println("Error finding closest nodes.");
        }
        return null;
    }

    public void connectToStartingNode(String startingNodeName, String startingNodeAddress) {
        try {
            String[] addressParts = startingNodeAddress.split(":");
            InetAddress ipAddress = InetAddress.getByName(addressParts[0]);
            int port = Integer.parseInt(addressParts[1]);

            try (Socket socket = new Socket(ipAddress, port);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                writer.write("START 1 " + nodeName + "\n");
                writer.flush();
                System.out.println("Connected to " + startingNodeName + " at " + startingNodeAddress);
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("Message from " + startingNodeName + ": " + line);
                    if (line.contains("END")) break;  // Example condition to end connection
                }
            }
        } catch (IOException e) {
            System.out.println("Could not connect to " + startingNodeName + " at " + startingNodeAddress + ": " + e.getMessage());
        }
    }



    public static void main(String[] args) {
    }
}

