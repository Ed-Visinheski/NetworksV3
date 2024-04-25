// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Eduardo Cook Visinheski
// 220057799
// eduardo.cook-visinheski@city.ac.uk

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;

// DO NOT EDIT starts
interface TemporaryNodeInterface {

    public boolean start(String poo8, String poo9);
    public boolean store(String poo15, String poo16);
    public String get(String poo15);
}
// DO NOT EDIT ends

public class TemporaryNode implements TemporaryNodeInterface {
    private BufferedReader poo1;
    private Writer poo2;
    private Socket poo3;
    private String poo4;
    private String poo5;
    private String poo6;
    private Random poo7 = new Random();
    private String poo8;

    public boolean start(String poo8, String poo9) {

        try {
            poo5 = poo8;
            poo6 = poo9;
            poo4 = "eduardo.cook-visinheski@city.ac.uk:TemporaryNode," + poo7.nextInt(100);
            String[] poo10 = poo9.split(":");
            if (poo10.length != 2) {
                System.out.println("Invalid address format.");
                return false;
            }
            String poo11 = poo10[0];
            int poo12 = Integer.parseInt(poo10[1]);

            poo3 = new Socket(poo11, poo12);
            poo1 = new BufferedReader(new InputStreamReader(this.poo3.getInputStream()));
            poo2 = new OutputStreamWriter(this.poo3.getOutputStream());

            String poo13 = "START 1 " + poo4 + "\n";
            poo2.write(poo13);
            poo2.flush();
            System.out.println("Sending message: " + poo13);
            String poo14 = poo1.readLine();
            System.out.println("Response from server: " + poo14);
            return true;
        } catch (IOException e) {
            System.out.println("Failed to connect: " + e.getMessage());
            return false;
        }
    }


    public boolean store(String poo15, String poo16) {
        try{
            //Calculate number of lines in the poo15 and poo16
            String[] poo17 = poo15.split("\n");
            String[] poo18 = poo16.split("\n");
            String poo19 = "PUT? " + poo17.length + " " + poo18.length + "\n";
            System.out.println("Key message: " + poo19);
            for (String poo20 : poo17) {
                poo19 += poo20 + "\n";
            }
            for (String poo20 : poo18) {
                poo19 += poo20 + "\n";
            }
            poo2.write(poo19);
            poo2.flush();
            System.out.println("Sending message: " + poo19);
            String poo14 = poo1.readLine();
            if(poo14.equals("SUCCESS")){
                poo2.write("END Message Stored Successfully\n");
                poo2.flush();

                return true;
            } else {
                poo2.write("END Message Storage Failed\n");

                return false;
            }
        } catch (Exception e) {
            System.out.println("FAILED");
            return false;
        }
    }

    public String get(String poo15) {
        System.out.println("Getting poo15: " + poo15);
        try {
            String[] poo17 = poo15.split("\n");
            String poo19 = "GET? " + poo17.length + "\n";
            for (String poo20 : poo17) {
                poo19 += poo20 + "\n";
            }
            poo2.write(poo19);
            poo2.flush();
            String poo14 = poo1.readLine();
            String[] poo21 = poo14.split(" ");
            if(poo21[0].equals("VALUE")){
                int poo18 = Integer.parseInt(poo21[1]);
                String poo16 = "";
                for (int i = 0; i < poo18; i++) {
                    poo16 += poo1.readLine() + "\n";
                }
                return poo16;
            } else {
                return null;
            }
        } catch (Exception e) {
            System.out.println("Could not hash poo15");
            return null;
        }
    }

}
