// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// YOUR_NAME_GOES_HERE
// YOUR_STUDENT_ID_NUMBER_GOES_HERE
// YOUR_EMAIL_GOES_HERE

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;

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


    public boolean start(String startingNodeName, String startingNodeAddress) {
	// Implement this!
	// Return true if the 2D#4 network can be contacted
        contactNodeName = startingNodeName;
        contactNodeAddress = startingNodeAddress;

        try{
            System.out.println("Please name this node:");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            nodeName = reader.readLine();
            String[] parts = startingNodeAddress.split(":");
            String ipAddress = parts[0];
            int port = Integer.parseInt(parts[1]);
            socket = new Socket(InetAddress.getByName(ipAddress), port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new OutputStreamWriter(socket.getOutputStream());
            socket = new Socket(ipAddress, port);
            System.out.println("TemporaryNode connected to " + startingNodeName + " at " + startingNodeAddress);
            String startMessage = "START 1 " + nodeName + "\n";
            writer.write(startMessage);
            writer.flush();
            String response = reader.readLine();
            String[] responceParts = response.split(" ");
            System.out.println("TemporaryNode received: " + response);
            return true;
        } catch (IOException e) {
            System.out.println("Could not resolve " + startingNodeAddress);
            return false;
        }
    }

    public boolean store(String key, String value) {
	    // Implement this!
	    // Return true if the store worke
        HashID hasher = new HashID();

        // Hash the key
        // Send this hashed key to the full node at the starting node address given

        // If the full node responds with a STORED message, return true
        try {
            byte[] keyBytes = hasher.computeHashID(key + '\n');
        } catch (Exception e) {
            System.out.println("Could not hash key");
            return false;
        }


	// Return false if the store failed
	return true;
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

    public String get(String key) {
            // Implement this!
            // Return the value if the key is found
            // Return null if the key is not found
            HashID hasher = new HashID();
            try {
                //Calculate number of lines in the key
                String[] keyLines = key.split("\n");
                String keyMessage = "GET? " + keyLines.length + "\n";
                for (String line : keyLines) {
                    keyMessage += line + "\n";
                }
                writer.write(keyMessage);
                writer.flush();
                String response = reader.readLine();
                String[] responseParts = response.split(" ");
                if(responseParts[0].equals("VALUE")){
                    int valueLines = Integer.parseInt(responseParts[1]);
                    String value = "";
                    for (int i = 0; i < valueLines; i++) {
                        value += reader.readLine() + "\n";
                    }
                    return value;
                } else {
                    return null;
                }
            } catch (Exception e) {
                System.out.println("Could not hash key");
                return null;
            }
    }


}
