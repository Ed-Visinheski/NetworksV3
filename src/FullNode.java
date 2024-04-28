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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private ExecutorService executorService = Executors.newCachedThreadPool();
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
            address = ipAddress + ":" + portNumber;
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
            HashID hashID = new HashID();
            int distance = hashID.calculateDistance(hashID.computeHashID(nodeName + "\n"), hashID.computeHashID(startingNodeName + "\n"));
            networkMap.computeIfAbsent(distance, k -> new HashMap<>()).put(startingNodeName, startingNodeAddress);
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
            connectToStartingNode(startingNodeName ,startingNodeAddress);
            heartbeatConnections();
        } catch (Exception e) {
            System.out.println("Error creating thread.");
        }
    }

    private void heartbeatConnections() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // Delay the start of the heartbeat to give more time for initial connection setup
        scheduler.scheduleAtFixedRate(() -> {
            if (networkMap.isEmpty()) {
                System.out.println("Network map is empty, no heartbeat checks needed.");
                return;
            }
            System.out.println("Heartbeat check started...");
            networkMap.forEach((distance, nodes) -> {
                new HashMap<>(nodes).forEach((nodeName, nodeAddress) -> {
                    // Skip the heartbeat check for the node's own address
                    if (!nodeAddress.equals(this.address)) {
                        executorService.submit(() -> {
                            if (!sendEchoRequest(nodeName, nodeAddress)) {
                                System.out.println("No response from node " + nodeName + " at " + nodeAddress + ". Removing from network map.");
                                // Safe removal
                                nodes.remove(nodeName, nodeAddress);
                            }
                        });
                    }
                    else {
                        System.out.println("Skipping heartbeat check for own address: " + nodeAddress);
                    }
                });
            });
        }, 50, 60, TimeUnit.SECONDS);
    }


    private boolean sendEchoRequest(String nodeName, String nodeAddress) {
        try (Socket socket = new Socket()) {
            String address = nodeAddress.split(":")[0];
            int port = Integer.parseInt(nodeAddress.split(":")[1]);
            socket.connect(new InetSocketAddress(address, port), 5000); // Timeout after 5 seconds
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                writer.write("ECHO?\n");
                writer.flush();
                String response = reader.readLine();
                return "OHCE".equals(response);
            }
        } catch (IOException e) {
            System.out.println("Error sending echo request to node " + nodeName + ": " + e.getMessage());
            return false;
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
            while (socket != null && !socket.isClosed()) {
                String command = reader.readLine();
                if (command == null) {
                    break;
                }
                parts = command.split(" ");
                System.out.println("Received message from Client in format:\n" + command);
                System.out.println("Message parts 0 : " + parts[0]);
                switch (parts[0]){
                    case"NOTIFY":{
                        String name = parts[1];
                        String address = parts[2];
                        if (AddToNetworkMap(name, address)) {
                            writer.write("NOTIFIED\n");
                            writer.flush();
                        } else {
                            writer.write("NOPE\n");
                            writer.flush();
                        }
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
                        System.out.println("Key HashID: " + hashID.bytesToHex(keyHashID));
                        if(!checkIfCloserNodesExist(keyHashID)){
                            keyValueMap.put(hashID.calculateDistance(keyHashID, hashID.computeHashID(nodeName + "\n")), new HashMap<>());
                            keyValueMap.get(hashID.calculateDistance(keyHashID, hashID.computeHashID(nodeName + "\n"))).put(key, value);
                            writer.write("SUCCESS\n");
                        }
                        else{
                            writer.write("NOPE\n");
                        }
                        writer.flush();
                        break;
                    }
                    case "GET?": {
                        int keyLines = Integer.parseInt(parts[1]);
                        StringBuilder keyToGetBuilder = new StringBuilder();
                        for (int i = 0; i < keyLines; i++) {
                            keyToGetBuilder.append(reader.readLine()).append("\n");
                        }
                        String keyToGet = keyToGetBuilder.toString().trim() + "\n"; // Ensuring newline is there
                        byte[] keyHash = hashID.computeHashID(keyToGet);
                        int distance = hashID.calculateDistance(keyHash, hashID.computeHashID(nodeName + "\n"));
                        Map<String, String> distanceMap = keyValueMap.get(distance);
                        if (distanceMap != null && distanceMap.containsKey(keyToGet)) {
                            String value = distanceMap.get(keyToGet);
                            String[] valueSplit = value.split("\n");
                            writer.write("VALUE " + valueSplit.length + "\n" + value);
                            writer.flush();
                        } else {
                            writer.write("NOPE\n");
                            writer.flush();
                        }
                        break;
                    }
                    case "END": {
                        writer.write("END Client Ended Communication\n");
                        writer.flush();
                        try {
                            socket.close();
                        } catch (IOException e) {
                            System.err.println("Error closing socket: " + e.getMessage());
                        }
                        return;
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
                            String nearestResponce = "";
                            for(String node : closestNodes.keySet()){
                                nearestResponce = (node + "\n" + closestNodes.get(node) + "\n");
                            }
                            writer.write(nearestResponce);
                            writer.flush();
                        }
                        else{
                            writer.write("NOPE\n");
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
            socket.close();
    } catch (IOException e) {
            System.out.println("Error handling client.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    // Checks to see if there are 3 nodes at the same distance as the key
    //If there aren't, checks to see if there are any nodes closer to the key
    // Add to a map of the closest nodes until there are 3 nodes
    //Start method by checking if the NetworkMap is empty or if it is smaller than or equal to 3
    //returns false
    public boolean checkIfCloserNodesExist(byte[] keyHashID) throws Exception {
        HashID hasher = new HashID();
        int keyDistance = hasher.calculateDistance(hasher.computeHashID(nodeName + "\n"), keyHashID);
        if(networkMap.isEmpty() || networkMap.size() <= 3){
            return false;
        }
        Map<String, String> closestNodes = findClosestToKey(keyHashID);
        if(closestNodes != null){
            //Checks to see if the current node is in the closest nodes
            if(closestNodes.containsKey(nodeName)){
                return false;
            }
            else if(closestNodes.size() < 3){
                return false;
            }
            else{
                return true;
            }
        }
        else {
            return false;
        }
    }

    public boolean AddToNetworkMap(String name, String address) {
        try {
            HashID hasher = new HashID();
            String nameWithNewline = name.endsWith("\n") ? name : name + "\n";
            byte[] nodeHashID = hasher.computeHashID(nameWithNewline);
            if(nodeHashID == null) {
                System.out.println("Failed to compute hash for node name: " + name);
                return false; // Exit if hash computation fails
            }

            String nodeNameWithNewline = nodeName.endsWith("\n") ? nodeName : nodeName + "\n";
            byte[] currentNodeHash = hasher.computeHashID(nodeNameWithNewline);
            if(currentNodeHash == null) {
                System.out.println("Failed to compute hash for current node name.");
                return false; // Exit if hash computation fails
            }

            int distance = hasher.calculateDistance(nodeHashID, currentNodeHash);
            networkMap.computeIfAbsent(distance, k -> new HashMap<>()).put(name, address);
            return true;
        } catch (Exception e) {
            System.out.println("Error adding to network map: " + e.getMessage());
        }
        return false;
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




    private void connectToStartingNode(String startingNodeName, String nodeAddress) {
        new Thread(() -> {
            String address = nodeAddress.split(":")[0];
            int port = Integer.parseInt(nodeAddress.split(":")[1]);
            try (Socket socket = new Socket(address, port);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
                writer.write("START 1 " + this.nodeName + "\n");
                writer.flush();

                String response;
                while ((response = reader.readLine()) != null) {
                    String[] parts = response.split(" ");
                    if ("END".equals(parts[0])) {
                        System.out.println("Received END command, closing connection.");
                        break;
                    }

                    if ("START".equals(parts[0]) && parts.length == 3) {
                        System.out.println("Received START message from " + parts[2]);
                        HandleServer(socket, reader, writer, startingNodeName, nodeAddress);
                        break; // Assume HandleServer takes over the session handling
                    } else {
                        System.out.println("Received invalid message: " + response);
                        writer.write("END Invalid message\n");
                        writer.flush();
                        break; // Close connection on protocol error
                    }
                }
            } catch (IOException e) {
                System.out.println("Could not connect or communicate with " + nodeAddress + ": " + e.getMessage());
            }
        }).start();
    }


    private void HandleServer(Socket socket, BufferedReader reader, BufferedWriter writer, String startingNodeName, String nodeAddress) {
        try {
            HashID hashID = new HashID();
            writer.write("NOTIFY " + nodeName + " " + address + "\n");
            writer.flush();
            String response;
            while ((response = reader.readLine()) != null && !response.equals("END") && !socket.isClosed()) {
                String[] parts = response.split(" ");
                switch (parts[0]) {
                    case "NOTIFIED": {
                        System.out.println("Notified " + nodeAddress);
                        break;
                    }
                    case "END": {
                        writer.write("END Client Ended Communication\n");
                        writer.flush();
                        try {
                            socket.close();
                        } catch (IOException e) {
                            System.err.println("Error closing socket: " + e.getMessage());
                        }
                        return;
                    }
                    case "NOPE": {
                        System.out.println("Failed to notify " + nodeAddress);
                        break;
                    }
                    case "VALUE": {
                        System.out.println("Received VALUE message from " + nodeAddress + "\n" + response);
                        break;
                    }

                    case "NODES": {
                        System.out.println("Received NODES message from " + nodeAddress + "\n" + response);
                        break;
                    }

                    case "SUCCESS": {
                        System.out.println("Successfully notified " + nodeAddress);
                        break;
                    }
                    case "OHCE": {
                        writer.write("Received ECHO\n");
                        writer.flush();
                        break;
                    }

                    case "ECHO?": {
                        writer.write("OHCE\n");
                        writer.flush();
                        reader.readLine();
                        return;
                    }
                    case "NOFITY?": {
                        String name = reader.readLine();
                        String address = reader.readLine();
                        if (AddToNetworkMap(name, address)) {
                            writer.write("NOTIFIED\n");
                            writer.flush();
                        }
                        break;
                    }
                    case "NEAREST?": {
                        String hexHashID = parts[1];
                        byte[] keyHashID = hashID.hexStringToByteArray(hexHashID);
                        Map<String, String> closestNodes = findClosestToKey(keyHashID);
                        if(closestNodes != null){
                            writer.write("NODES " + closestNodes.size() + "\n");
                            String nearestResponce = "";
                            for(String node : closestNodes.keySet()){
                                nearestResponce = (node + "\n" + closestNodes.get(node) + "\n");
                            }
                            writer.write(nearestResponce);
                            writer.flush();
                        }
                        else{
                            writer.write("NOPE\n");
                            writer.flush();
                        }
                        break;
                    }

                    case "PUT?":{
                        System.out.println("Received PUT? message from " + nodeAddress + "\n" + response);
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
                        System.out.println("Key HashID: " + hashID.bytesToHex(keyHashID));
                        if(!checkIfCloserNodesExist(keyHashID)){
                            keyValueMap.put(hashID.calculateDistance(keyHashID, hashID.computeHashID(nodeName + "\n")), new HashMap<>());
                            keyValueMap.get(hashID.calculateDistance(keyHashID, hashID.computeHashID(nodeName + "\n"))).put(key, value);
                            writer.write("SUCCESS\n");
                        }
                        else{
                            writer.write("NOPE\n");
                        }
                        writer.flush();
                        break;
                    }

                    case "GET?":{
                        int keyLines = Integer.parseInt(parts[1]);
                        StringBuilder keyToGetBuilder = new StringBuilder();
                        for (int i = 0; i < keyLines; i++) {
                            keyToGetBuilder.append(reader.readLine()).append("\n");
                        }
                        String keyToGet = keyToGetBuilder.toString().trim() + "\n"; // Ensuring newline is there
                        byte[] keyHash = hashID.computeHashID(keyToGet);
                        int distance = hashID.calculateDistance(keyHash, hashID.computeHashID(nodeName + "\n"));
                        Map<String, String> distanceMap = keyValueMap.get(distance);
                        if (distanceMap != null && distanceMap.containsKey(keyToGet)) {
                            String value = distanceMap.get(keyToGet);
                            String[] valueSplit = value.split("\n");
                            writer.write("VALUE " + valueSplit.length + "\n" + value);
                            writer.flush();
                        } else {
                            writer.write("NOPE\n");
                            writer.flush();
                        }
                        break;
                    }

                    default:
                        System.out.println("Invalid response from " + nodeAddress + ": " + response);
                        writer.write("END Invalid response\n");
                        writer.flush();
                        socket.close();
                        break;
                }

                Scanner scanner = new Scanner(System.in);
                System.out.println("Enter a command: ");
                String command = scanner.nextLine();
                writer.write(command + "\n");
                writer.flush();
                if("END".contains(command)){
                    reader.readLine();
                    socket.close();
                    return;
                }
            }
        } catch (IOException e) {
            System.out.println("Error handling server: " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
    }
}

