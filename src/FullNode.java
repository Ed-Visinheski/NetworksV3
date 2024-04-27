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
    private Socket socket;
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
            //connectToStartingNode(startingNodeName, startingNodeAddress);
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

    //Handles the communication between the current node and a temporary node which has connected to the current node
    public void HandleClient(Socket socket, String startingNodeName, String startingAddress) {
        try {
            HashID hashID = new HashID();
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            writer.write("START 1 " + nodeName + "\n");
            writer.flush();
            String line = reader.readLine();
            String[] parts = line.split(" ");
            if (parts.length != 3) {
                System.out.println("Invalid START message.");
                writer.write("END Invalid START message\n");
                writer.flush();
                return;
            }
            String version = parts[1];
            System.out.println("Received START message from Client");
            while ((!reader.readLine().contains("END") && !socket.isClosed())) {
                line = reader.readLine();
                parts = line.split(" ");
                System.out.println("Received message from Client in format:\n" + line);
                System.out.println("Message parts 0 : " + parts[0]);
                switch (parts[0]){
                    case"NOTIFY":{
                        String name = parts[1];
                        String address = parts[2];
                        AddToNetworkMap(name, address);
                        writer.write("NOTIFIED\n");
                        break;
                    }
                    case"PUT?": {
                        System.out.println("Received PUT? message from Client in format:\n" + line);
                        int keyLines = Integer.parseInt(parts[1]);
                        int valueLines = Integer.parseInt(parts[2]);
                        String key = "";
                        String value = "";
                        for(int i = 0; i < keyLines; i++){
                            key += reader.readLine()+"\n";
                        }
                        System.out.println("Key:\n" + key);
                        for(int i = 0; i < valueLines; i++){
                            value += reader.readLine()+"\n";
                        }
                        System.out.println("Value:\n" + value);
                        byte[] keyHashID = hashID.computeHashID(key);
                        if(calculateStoreDistance(keyHashID)){
                            keyValueMap.put(key, value);
                            writer.write("SUCCESS\n");
                        } else {
                            writer.write("FAILED\n");
                        }
                        break;
                    }
                    case "GET?": {
                        int keyLines = Integer.parseInt(parts[1]);
                        String keyToGet = "";
                        for (int i = 0; i < keyLines; i++) {
                            keyToGet += reader.readLine() + "\n";
                        }
                        if (keyValueMap.containsKey(keyToGet)) {
                            String[] valueSlit = keyValueMap.get(keyToGet).split("\n");
                            writer.write("VALUE " + valueSlit.length + "\n" + keyValueMap.get(keyToGet));
                            writer.flush();
                        } else {
                            writer.write(getFromClosestNode(hashID.computeHashID(keyToGet)));
                            writer.flush();
                        }
                        break;
                    }
                    case "END": {
                        System.out.println("Received END message from Client");
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
                    default: {
                        System.out.println("Invalid message received.");
                        writer.write("END Invalid message\n");
                        writer.flush();
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
        try{
            if(findClosestToKey(keyHashID).size() == 3){
                return false;
            }
            HashID hasher = new HashID();
            int networkSize = networkMap.size();
            int portion = 256 / networkSize;
            int minDistance = 256;
            for (int distance : networkMap.keySet()) {
                if (distance < minDistance) {
                    Map<String, String> nodes = networkMap.get(distance);
                    for (String node : nodes.keySet()) {
                        byte[] nodeHashID = hasher.computeHashID(node);
                        distance = hasher.calculateDistance(nodeHashID, keyHashID);
                        if (distance < minDistance) {
                            minDistance = distance;
                        }
                    }
                }
            }
            return minDistance < portion;
        } catch (Exception e) {
            System.out.println("Error calculating store distance.");
            return false;
        }
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

    //Find the 3 closest nodes to that the key's hashID's
    //Returns a map of the closest nodes

    public Map<String, String> findClosestToKey (byte[] keyHashID) {
        try{
            HashID hasher = new HashID();
            int networkSize = networkMap.size();
            int portion = 256 / networkSize;
            Map<String, String> closestNodes = new HashMap<>();
            for (int i = 0; i < 3; i++) {
                int minDistance = 256;
                String minNode = "";
                for (int distance : networkMap.keySet()) {
                    if (distance < minDistance) {
                        Map<String, String> nodes = networkMap.get(distance);
                        for (String node : nodes.keySet()) {
                            byte[] nodeHashID = hasher.computeHashID(node);
                            distance = hasher.calculateDistance(nodeHashID, keyHashID);
                            if (distance < minDistance) {
                                minDistance = distance;
                                minNode = node;
                            }
                        }
                    }
                }
                closestNodes.put(minNode, networkMap.get(hasher.calculateDistance(hasher.computeHashID(minNode), keyHashID)).get(minNode));
            }
            return closestNodes;
        } catch (Exception e) {
            System.out.println("Error finding closest nodes.");
            return null;
        }
    }

    //Sends GET request to the closest node to the key's hashID
    public String getFromClosestNode(byte[] keyHashID) {
        try {
            Map<String, String> closestNodes = findClosestToKey(keyHashID);
            for (String node : closestNodes.keySet()) {
                String[] nodeParts = node.split(":");
                String nodeAddress = closestNodes.get(node);
                Socket closestNodeSocket = new Socket(nodeParts[0], Integer.parseInt(nodeParts[1]));
                BufferedWriter closestNodeWriter = new BufferedWriter(new OutputStreamWriter(closestNodeSocket.getOutputStream()));
                BufferedReader closestNodeReader = new BufferedReader(new InputStreamReader(closestNodeSocket.getInputStream()));
                closestNodeWriter.write("GET? " + keyHashID + "\n");
                closestNodeWriter.flush();
                String response = closestNodeReader.readLine();
                String[] responseParts = response.split(" ");
                if (responseParts[0].equals("VALUE")) {
                    int valueLines = Integer.parseInt(responseParts[1]);
                    String value = "";
                    for (int i = 0; i < valueLines; i++) {
                        value += closestNodeReader.readLine() + "\n";
                    }
                    System.out.println(value);
                    closestNodeWriter.write("END Message Retrieved Successfully\n");
                    closestNodeWriter.flush();
                    closestNodeSocket.close();
                    return value;
                } else {
                    closestNodeWriter.write("END Message Not Found\n");
                    closestNodeWriter.flush();
                    closestNodeSocket.close();
                    return "NOPE";
                }
            }
        } catch (IOException e) {
            System.out.println("Error getting from closest node.");
        }
        return "NOPE";
    }

    public void connectToStartingNode(String startingNodeName, String startingNodeAddress) {
        try {
            String[] addressParts = startingNodeAddress.split(":");
            InetAddress ipAddress = InetAddress.getByName(addressParts[0]);
            int port = Integer.parseInt(addressParts[1]);

            try {
                socket = new Socket(ipAddress, port);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                System.out.println("Connected to server: " + startingNodeName + " at " + startingNodeAddress);

                    writer.write("START 1 " + nodeName + "\n");
                    writer.flush();
                    System.out.println("Sent START message to server" + startingNodeName);
                    String line;
                    Scanner scanner = new Scanner(System.in);
                    line = reader.readLine();
                    System.out.println(line);
                    while (line != null && !socket.isClosed() && !line.contains("END")){
                        String input = scanner.nextLine();
                        writer.write(input + "\n");
                        writer.flush();
                        reader.readLine();
                        System.out.println(reader.readLine());
                    }
            } catch (IOException e) {
                System.out.println("Could not connect to server " + startingNodeName + " at " + startingNodeAddress + ": " + e.getMessage());
            }
        } catch (IOException e) {
            System.out.println("Could not connect to server " + startingNodeName + " at " + startingNodeAddress + ": " + e.getMessage());
        }
    }

    //Will handle communications between this node and the starting node




    public static void main(String[] args) {
    }
}

