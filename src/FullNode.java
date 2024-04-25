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

    //Network Mapping
    //
    //   A network map is a map from the names of full nodes to their
    //   addresses.  It is used to find the node that is storing the
    //   relevant part of the hash table.  Full nodes MUST build and
    //   maintain a network map.
    //
    //   Each node's network map MUST contain itself.  It MUST contain at
    //   most three nodes at every distance (three nodes at distance 10,
    //   three nodes at distance 11, etc.).  If a node finds more than
    //   three nodes at the same distance it MUST only keep three.  It
    //   SHOULD pick to maximise network robustness and stability.  This MAY
    //   be picking the longest running node.  Nodes MUST remove entries
    //   from their map if they are uncontactable or incorrectly implement
    //   the protocol.
    //
    //   By limiting the number of nodes in the map the memory required is
    //   limited and does not grow with the size of network.  By storing
    //   nodes at all distances, the number of nodes that must be contacted
    //   to find the nearest hashID is minimised.
    //
    //   The map MAY be built by passive mapping; recording the names of
    //   nodes and addresses of full nodes that it has interacted with.
    //   It MAY also be built by active mapping; connecting to other nodes
    //   and querying their directories.
    //
    //   Full nodes MUST use the NOTIFY request to inform other full nodes
    //   of their address.  All nodes MAY use the NOTIFY request to inform
    //   full nodes of addresses of other full nodes.

    // Network map is a map from the distance to a map from the name to the address
    // Integer used for distance, String for the name and string for the address
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

    //6.1 START message
    //
    //   Both nodes MUST send a START message at the start of the
    //   communication.  They MUST NOT send a START message at any other
    //   time.  A START message is a single line consisting of three parts:
    //
    //   START <number> <string>
    //
    //   The number gives the highest protocol version supported by the
    //   sender.  The communication must use the highest protocol version
    //   supported by both the requester and the receiver.  This document
    //   describes protocol version 1.
    //
    //   The string gives the node name of the sender.
    //
    //   For example on connecting to the example node above the first message
    //   received could be:
    //
    //   START 1 martin.brain@city.ac.uk:MyCoolImplementation,1.41,test-node-2

    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
        try {
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
            System.out.println("Sent START message to " + startingNodeName);
            while ((!reader.readLine().contains("END") && !socket.isClosed())) {
                line = reader.readLine();
                System.out.println("Received: " + line);
                parts = line.split(" ");
                if (parts.length < 1) {
                    System.out.println("Invalid message.");
                    writer.write("END Invalid message\n");
                    writer.flush();
                    return;
                }
                String command = parts[0];

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
                if (command.equals("PUT?")) {
                    if(parts.length < 3) {
                        System.out.println("Invalid PUT? message.");
                        writer.write("END Invalid PUT? message\n");
                        writer.flush();
                        stop();
                        return;
                    }
                    int keyLines = Integer.parseInt(parts[1]);
                    int valueLines = Integer.parseInt(parts[2]);
                    String key = "";
                    String value = "";
                    for (int i = 0; i < keyLines; i++) {
                        key += reader.readLine() + "\n";
                    }
                    for (int i = 0; i < valueLines; i++) {
                        value += reader.readLine() + "\n";
                    }
                    //Calculate key hashID
                    byte[] keyHashID = hashID.computeHashID(key);
                    if(calculateStoreDistance(keyHashID)){
                        //Store the key and value
                        keyValueMap.put(key, value);
                        writer.write("SUCCESS\n");
                        writer.flush();
                        System.out.println("Sent SUCCESS message to " + startingNodeName);
                    } else {
                        writer.write("FAILED\n");
                        writer.flush();
                        System.out.println("Sent FAILED message to " + startingNodeName);
                    }
                } else if (command.equals("GET?")) {
                    if (parts.length < 2) {
                        System.out.println("Invalid GET? message.");
                        return;
                    }
                    String key = parts[1];
                    System.out.println("Received GET? message with key " + key);
                    // String value = Get(key);
                    // writer.write("GET! " + key + " " + value + "\n");
                    writer.flush();
                    System.out.println("Sent GET! message to " + startingNodeName);
                } else if (command.equals("NOTIFY")) {
                    if (parts.length < 3) {
                        System.out.println("Invalid NOTIFY message.");
                        return;
                    }
                    String name = parts[1];
                    String address = parts[2];
                    System.out.println("Received NOTIFY message with name " + name + " and address " + address);
                    // Add to network map
                    // AddToNetworkMap(name, address);
                    writer.write("NOTIFY!\n");
                    writer.flush();
                    System.out.println("Sent NOTIFY! message to " + name);
                } else {
                    System.out.println("Unknown command.");
                    return;
                }
        }
    } catch (IOException e) {
            System.out.println("Error handling client.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public boolean calculateStoreDistance(byte[] keyHashID) {
        //Calculate the distance between the keyHashID and the current node
        //Then determine if the key should be stored on this node based on its network size,
        //Check to see if the keyHashID is closer to the current node than the other nodes in the network map
        //send key and value to the closest node
        //if no other closest node calculate the distance between the keyHashID and the current node
        //Keys should be stored based on keyHashID distance <= (256 / number of nodes in network map)
        //if the keyHashID is > (256 / number of nodes in network map) then the key should
        // not be stored on the current node and returns false
        //This is to ensure that the key is stored on the closest node to the keyHashID
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
                distance--;
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


    public static void main(String[] args) {
    }
}

