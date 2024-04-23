// IN2011 Computer Networks
// Coursework 2023/2024
//
// This is an example of how the FullNode object can be used.
// It should work with your submission without any changes.
// This should make your testing easier.

// DO NOT EDIT starts
public class CmdLineFullNode {

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage error!");
            System.err.println("DSTStoreCmdLine startingNodeName startingNodeAddress ipAddress portNumber");
            return;
        } else {
	    // A full node that is running on the network to be a first point of contact
            String startingNodeName = args[0];
            String startingNodeAddress = args[1];

	    // These give the IP Address and port for other nodes to contact this one
	    String ipAddress = args[2];
	    int portNumber;
	    try {
                portNumber = Integer.parseInt(args[3]);
            } catch (Exception e) {
                System.err.println("Exception parsing the port number");
                System.err.println(e);
                return;
            }

	    
            // Use a FullNode object to be a full participant in the 2D#4 network
            FullNode fn = new FullNode();

	    // Full nodes need to be able to accept incoming connections
	    if (fn.listen(ipAddress, portNumber)) {

            // Become part of the network
            fn.handleIncomingConnections(startingNodeName, startingNodeAddress);
		
	    } else {
		System.err.println("Could not listen for incoming connections");
	    }
            return;
        }
    }
}
// DO NOT EDIT ends
