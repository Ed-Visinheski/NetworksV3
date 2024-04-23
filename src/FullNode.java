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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

// DO NOT EDIT starts
interface FullNodeInterface {
    public boolean listen(String ipAddress, int portNumber);
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress);
}
// DO NOT EDIT ends

public class FullNode implements FullNodeInterface{
    private HashSet<byte[]> networkMap;
    private ServerSocket serverSocket;
    private String nodeName;
    private String address;

    public boolean listen(String ipAddress, int portNumber) {
        System.out.println("Please name this node:");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            nodeName = reader.readLine();
        } catch (IOException e) {
            System.out.println("Error reading the node name.");
            return false;
        }
        try {
            serverSocket = new ServerSocket(portNumber, 50, InetAddress.getByName(ipAddress));
            address = ipAddress + ":" + portNumber;
            System.out.println("FullNode listening on " + address);
            Thread thread = new Thread(() -> {
                while (true) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        handleClient(clientSocket);
                    } catch (IOException e) {
                        System.out.println("Error accepting a client connection.");
                    }
                }
            });
            thread.start();
            return true;
        } catch (IOException e) {
            System.out.println("Could not listen on port: " + portNumber);
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
        String[] parts = startingNodeAddress.split(":");
        try {
            Socket socket = new Socket(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1]));
            System.out.println("FullNode connected to " + startingNodeName + " at " + startingNodeAddress);
            EnterNetworkMap(socket, startingNodeName, startingNodeAddress);
        }
        catch (IOException e) {
            System.out.println("Could not connect to " + startingNodeAddress);
        }
    }

    private void handleClient(Socket clientSocket) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            // Example of reading a message from the client and sending a response
            String clientMessage = in.readLine();
            System.out.println("Received from client: " + clientMessage);
            String[] parts = clientMessage.split(" ");
            switch (parts[0]) {
                case "START":
                    out.println("START 1 " + nodeName);
                    break;
                case "NOTIFY?":
                    String nodeName = in.readLine();
                    String address = in.readLine();
                    System.out.println("Received NOTIFY? from " + nodeName + " at " + address);
                    networkMap.add(address.getBytes());
                    out.println("NOTIFIED");
                    break;


                //6.7. NEAREST? request
                //
                //   The requester MAY send a NEAREST request.  A NEAREST
                //   request is a single line with two parts:
                //
                //   NEAREST? <string>
                //
                //   The string is a hashID written in hex.  The responder MUST look at its
                //   network map and return the three nodes with the closest hashID
                //   to the requested hashID.  It MUST return a NODES response.  A NODES
                //   response is between three and seven lines.  The first line has two parts:
                //
                //   NODES <number>
                //
                //   The number is the number of full node names and addresses that follow.
                //
                //   The requester MUST NOT assume that the responder is aware of all of
                //   the nodes in the network and SHOULD query nodes closer to the key.
                //
                //   For example if a requester sends:
                //
                //   NEAREST? 0f003b106b2ce5e1f95df39fffa34c2341f2141383ca46709269b13b1e6b4832
                //
                //   then the responder might return:
                //
                //   NODES 3
                //   martin.brain@city.ac.uk:MyCoolImplementation,1.41,test-node-1
                //   10.0.0.4:2244
                //   martin.brain@city.ac.uk:MyCoolImplementation,1.67,test-node-7
                //   10.0.0.23:2400
                //   martin.brain@city.ac.uk:MyCoolImplementation,1.67,test-node-9
                //   10.0.0.96:35035

                case "NEAREST?":
                    String hashID = parts[1];
                    byte[] hashIDBytes = hashID.getBytes();
                    System.out.println("Received NEAREST? for " + hashID);
                    // Find the three nodes with the closest hashID to the requested hashID
                    // Return a NODES response


                    break;
            }

            // Close the streams and socket when done
            in.close();
            out.close();
            clientSocket.close();
        } catch (IOException e) {
            System.out.println("Error handling the client.");
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

    public void EnterNetworkMap(Socket socket, String startingNodeName, String startingAddress) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            OutputStreamWriter writer = new OutputStreamWriter(socket.getOutputStream());
            String startMessage = "START 1 " + nodeName + "\n";
            writer.write(startMessage);
            writer.flush();
            String response = reader.readLine();
            String[] responseParts = response.split(" ");
            if(responseParts[0].equals("START")){
                System.out.println(response);
                String message ="NOTIFY?" +"\n" + nodeName + "\n" + address + "\n";
                writer.write(message);
                writer.flush();
                response = reader.readLine();
                if(response.equals("NOTIFIED")){
                    System.out.println(response);
                }
                else{
                    System.out.println("END NOTIFY message not received " + nodeName);
                    System.out.println(response);
                    stop();
                }
            } else {
                System.out.println("END START message not received " + nodeName);
                System.out.println(response);
                stop();
            }
        } catch (IOException e) {
            System.out.println("END Could not resolve " + nodeName);
            stop();
        }

    }


    public static void main(String[] args) {
    }
}

