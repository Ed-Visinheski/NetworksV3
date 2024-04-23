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
import java.nio.charset.StandardCharsets;

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

        try {
            this.contactNodeName = startingNodeName;
            this.contactNodeAddress = startingNodeAddress;
            System.out.println("Please name this node:");
            BufferedReader nameReader = new BufferedReader(new InputStreamReader(System.in));
            this.nodeName = nameReader.readLine();
            nameReader.close();
            String[] parts = startingNodeAddress.split(":");
            if (parts.length != 2) {
                System.out.println("Invalid address format. Please use IP:Port format.");
                return false;
            }
            String ipAddress = parts[0];
            int port = Integer.parseInt(parts[1]);

            socket = new Socket(ipAddress, port);
            this.reader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
            this.writer = new OutputStreamWriter(this.socket.getOutputStream());

            System.out.println("TemporaryNode connected to " + startingNodeName + " at " + ipAddress + ":" + port);
            String startMessage = "START 1 " + this.nodeName + "\n";
            this.writer.write(startMessage);
            this.writer.flush();
            String response = this.reader.readLine();
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
            for (String line : keyLines) {
                keyMessage += line + "\n";
            }
            for (String line : valueLines) {
                keyMessage += line + "\n";
            }
            writer.write(keyMessage);
            writer.flush();
            String response = reader.readLine();
            if(response.equals("SUCCESS")){
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
                System.out.println("FAILED");
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

    public String get(String key) {
            // Implement this!
            // Return the value if the key is found
            // Return null if the key is not found
        System.out.println("Getting key: " + key);
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
