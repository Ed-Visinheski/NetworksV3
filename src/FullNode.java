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
    private Map<Integer, Map<String,String>>keyValueMap = new HashMap<>();
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
            networkMap.put(0, new HashMap<>());
            networkMap.get(0).put(nodeName, ipAddress + ":" + portNumber);
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
            while ((!socket.isClosed())) {
                String command = reader.readLine();
                parts = command.split(" ");
                System.out.println("Received message from Client in format:\n" + command);
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
                        System.out.println("Received PUT? message from Client in format:\n" + command);
                        int keyLines = Integer.parseInt(parts[1]);
                        int valueLines = Integer.parseInt(parts[2]);
                        String key = "";
                        String value = "";
                        for(int i = 0; i < keyLines; i++){
                            key += reader.readLine() + "\n";
                        }
                        key = key.endsWith("\n") ? key : key + "\n";  // Ensure ends with newline
                        System.out.println("Key:\n" + key);
                        for(int i = 0; i < valueLines; i++){
                            value += reader.readLine() + "\n";
                        }
                        value = value.endsWith("\n") ? value : value + "\n";  // Ensure ends with newline
                        System.out.println("Value:\n" + value);
                        byte[] keyHashID = hashID.computeHashID(key);
                        if(!checkIfCloserNodesExist(keyHashID)){
                            keyValueMap.put(hashID.calculateDistance(keyHashID, hashID.computeHashID(nodeName)), new HashMap<>());
                            keyValueMap.get(hashID.calculateDistance(keyHashID, hashID.computeHashID(nodeName))).put(key, value);
                            writer.write("SUCCESS\n");
                        }
                        else{
                            writer.write("NOPE\n");
                        }
                        writer.flush();
                        break;
                    }
                    case "GET?": {
                        HashID hasher = new HashID();
                        int keyLines = Integer.parseInt(parts[1]);
                        String keyToGet = "";
                        for (int i = 0; i < keyLines; i++) {
                            keyToGet += reader.readLine() + "\n";
                        }
                        int distance = hasher.calculateDistance(hasher.computeHashID(keyToGet), hasher.computeHashID(nodeName));
                        if (keyValueMap.get(distance).containsKey(keyToGet)) {
                            String valueSlit[] = keyValueMap.get(distance).get(keyToGet).split("\n");
                            writer.write("VALUE " + valueSlit.length + "\n" + keyValueMap.get(keyToGet));
                            writer.flush();
                        } else {
                            writer.write("NOPE\n");
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
                    //NAREST? <HashID>
                    case "NEAREST?": {
                        String hexHashID = parts[1];
                        byte[] keyHashID = hashID.hexStringToByteArray(hexHashID);
                        Map<String, String> closestNodes = findClosestToKey(keyHashID);
                        if(closestNodes != null){
                            writer.write("NODES " + closestNodes.size() + "\n");
                            for(String node : closestNodes.keySet()){
                                writer.write(node + "\n" + closestNodes.get(node) + "\n");
                            }
                            writer.flush();
                        }
                        else{
                            writer.write("NOPE\n");
                            writer.flush();
                        }

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

    // Ensure key and nodeName are appropriately formatted with a newline at the end
    public boolean checkIfCloserNodesExist(byte[] keyHashID) {
        if(networkMap.isEmpty() || keyHashID == null){
            return false; // Return false if the network is empty or keyHashID is null.
        }

        try {
            HashID hashID = new HashID();
            String nodeNameWithNewline = currentNodeName.endsWith("\n") ? currentNodeName : currentNodeName + "\n";
            byte[] currentNodeHash = hashID.computeHashID(nodeNameWithNewline);
            if(currentNodeHash == null) {
                System.out.println("Failed to compute hash for currentNodeName.");
                return false;
            }

            int distance = hashID.calculateDistance(currentNodeHash, keyHashID);
            Map<String, String> nodes = networkMap.get(distance);
            return nodes != null && nodes.size() >= 3;
        } catch (Exception e) {
            System.out.println("Error in checkIfCloserNodesExist: " + e.getMessage());
            return false;
        }
    }



    public void AddToNetworkMap(String name, String address) {
        try {
            HashID hasher = new HashID();
            String nameWithNewline = name.endsWith("\n") ? name : name + "\n";
            byte[] nodeHashID = hasher.computeHashID(nameWithNewline);
            if(nodeHashID == null) {
                System.out.println("Failed to compute hash for node name: " + name);
                return; // Exit if hash computation fails
            }

            String nodeNameWithNewline = nodeName.endsWith("\n") ? nodeName : nodeName + "\n";
            byte[] currentNodeHash = hasher.computeHashID(nodeNameWithNewline);
            if(currentNodeHash == null) {
                System.out.println("Failed to compute hash for current node name.");
                return; // Exit if hash computation fails
            }

            int distance = hasher.calculateDistance(nodeHashID, currentNodeHash);
            networkMap.computeIfAbsent(distance, k -> new HashMap<>()).put(name, address);
        } catch (Exception e) {
            System.out.println("Error adding to network map: " + e.getMessage());
        }
    }


    //Find the 3 closest nodes to that the key's hashID's
    //Returns a map of the closest nodes

    public Map<String, String> findClosestToKey(byte[] keyHashID) {
        try {
            HashID hasher = new HashID();
            if (networkMap.isEmpty()) {
                System.out.println("The network map is empty.");
                return null;
            }

            int keyDistance = hasher.calculateDistance(hasher.computeHashID(nodeName), keyHashID);
            TreeMap<Integer, Map<String, String>> sortedMap = new TreeMap<>();

            // Collect all nodes with their distances, include the current node explicitly if needed
            for (Map.Entry<Integer, Map<String, String>> entry : networkMap.entrySet()) {
                int distance = Math.abs(entry.getKey() - keyDistance);
                sortedMap.put(distance, entry.getValue());
            }

            Map<String, String> closestNodes = new HashMap<>();
            // Ensure the current node is considered if it's one of the closest or if not enough nodes are available
            if (networkMap.containsKey(0)) {
                Map<String, String> zeroDistanceNodes = networkMap.get(0);
                closestNodes.putAll(zeroDistanceNodes);  // Includes current node if present at distance 0
            }

            for (Map.Entry<Integer, Map<String, String>> entry : sortedMap.entrySet()) {
                if (closestNodes.size() >= 3) break; // Stop if we already have 3 closest nodes
                for (Map.Entry<String, String> nodeEntry : entry.getValue().entrySet()) {
                    closestNodes.put(nodeEntry.getKey(), nodeEntry.getValue());
                    if (closestNodes.size() >= 3) break;
                }
            }

            return closestNodes.isEmpty() ? null : closestNodes;
        } catch (Exception e) {
            System.out.println("Error finding closest nodes: " + e.getMessage());
            return null;
        }
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

