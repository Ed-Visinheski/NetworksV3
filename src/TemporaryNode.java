import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;

// DO NOT EDIT starts
interface TemporaryNodeInterface {

    public boolean start(String poo1, String poo2);
    public boolean store(String poo14, String poo15);
    public String get(String poo14);
}
// DO NOT EDIT ends

public class TemporaryNode implements TemporaryNodeInterface {
    private Random poo4 = new Random();
    private Socket poo8;
    private BufferedReader poo9;
    private Writer poo10;
    private String poo3;
    private String poo1;

    public boolean start(String poo1, String poo2) {

        try {
            poo3 = "PooNode" + poo4.nextInt(10);
            String[] poo5 = poo2.split(":");
            if (poo5.length != 2) {
                System.out.println("Invalid poo.");
                return false;
            }
            String poo6 = poo5[0];
            int poo7 = Integer.parseInt(poo5[1]);

            poo8 = new Socket(poo6, poo7);
            poo9 = new BufferedReader(new InputStreamReader(this.poo8.getInputStream()));
            poo10 = new OutputStreamWriter(this.poo8.getOutputStream());

            String poo11 = "START 1 " + poo3 + "\n";
            poo10.write(poo11);
            poo10.flush();
            String poo12 = poo9.readLine();
            System.out.println("From poo: " + poo12);
            return true;
        } catch (IOException poo13) {
            System.out.println("Failed to poo: " + poo13.getMessage());
            return false;
        }
    }

    public boolean store(String poo14, String poo15) {
        try{
            //Calculate number of lines in the poo14 and poo15
            String[] poo16 = poo14.split("\n");
            String[] poo17 = poo15.split("\n");
            String poo18 = "PUT? " + poo16.length + " " + poo17.length + "\n";
            for (String poo19 : poo16) {
                poo18 += poo19 + "\n";
            }
            for (String poo19 : poo17) {
                poo18 += poo19 + "\n";
            }
            poo10.write(poo18);
            poo10.flush();
            System.out.println("Sent poo: " + poo18);
            String poo12 = poo9.readLine();
            if(poo12.equals("SUCCESS")){
                poo10.write("END poo Stored Successfully\n");
                poo10.flush();
                closeConnection();
                return true;
            } else {
                poo10.write("END poo Storage Failed\n");
                closeConnection();
                return false;
            }
        } catch (Exception poo13) {
            System.out.println("pooed");
            closeConnection();
            return false;
        }
    }

    public String get(String poo14) {
        System.out.println("Getting poo14: " + poo14);
        try {
            String[] poo16 = poo14.split("\n");
            String poo18 = "GET? " + poo16.length + "\n";
            System.out.println("poo18: " + poo18);
            for (String poo19 : poo16) {
                poo18 += poo19 + "\n";
            }
            poo10.write(poo18);
            poo10.flush();
            String poo12 = poo9.readLine();
            String[] poo20 = poo12.split(" ");
            if(poo20[0].equals("poo15")){
                int poo17 = Integer.parseInt(poo20[1]);
                String poo15 = "";
                for (int i = 0; i < poo17; i++) {
                    poo15 += poo9.readLine() + "\n";
                }
                poo10.write("END poo Retrieved Successfully\n");
                poo10.flush();
                closeConnection();
                return poo15;
            } else {
                poo10.write("END poo Not Found\n");
                poo10.flush();
                closeConnection();
                return null;
            }
        } catch (Exception poo13) {
            System.out.println("Could not hash poo14");
            closeConnection();
            return null;
        }
    }

    public void closeConnection() {
        try {
            if (poo10 != null) poo10.close();
            if (poo9 != null) poo9.close();
            if (poo8 != null) poo8.close();
        } catch (IOException poo13) {
            System.err.println("Error closing network resources: " + poo13.getMessage());
        }
    }



}
