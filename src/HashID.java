// IN2011 Computer Networks
// Coursework 2023/2024
//
// Construct the hashID for a string

import java.lang.StringBuilder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashID {

    public static byte [] computeHashID(String line) throws Exception {
		if (line.endsWith("\n")) {
			// What this does and how it works is covered in a later lecture
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(line.getBytes(StandardCharsets.UTF_8));
			return md.digest();
		}
		else {
			// 2D#4 computes hashIDs of lines, i.e. strings ending with '\n'
			throw new Exception("No new line at the end of input to HashID");
		}
	}


	//2. HashIDs
	//
	//   A hashID is the SHA-256 hash of one or more lines of text
	//   (including the new line character at the end).  When used in the
	//   protocol these are formatted as 64 hex digits.  For example the
	//   line of text:
	//
	//   Hello World!
	//
	//   gives the hashID:
	//
	//   03ba204e50d126e4674c005e04d82e84c21366780af1f43bd54a37816b6ab340
	//
	//   The distance between two hashIDs is 256 minus the number of
	//   leading bits that match.  For example, if the two hashIDs are:
	//
	//   0f033be6cea034bd45a0352775a219ef5dc7825ce55d1f7dae9762d80ce64411
	//   0f0139b167bb7b4a416b8f6a7e0daa7e24a08172b9892171e5fdc615bb7f999b
	//
	//   then the first 14 bits match so the distance between the keys is
	//   242.  The distance between a hashID and itself is 0 as all of the bits
	//   match.  The distance from H1 to H2 is the same as the distance from
	//   H2 to H1.  Also the following triangle inequality holds:
	//
	//   distance(H1,H3) <= distance(H1, H2) + distance(H2, H3)

	public static String bytesToHex(byte[] bytes) {
		StringBuilder hexString = new StringBuilder();
		for (byte b : bytes) {
			String hex = Integer.toHexString(0xff & b);
			if (hex.length() == 1) {
				hexString.append('0');
			}
			hexString.append(hex);
		}
		return hexString.toString();
	}

	public static int calculateDistance(byte[] hashID1, byte[] hashID2) {
		int distance = 0;
		for (int i = 0; i < hashID1.length; i++) {
			int xor = hashID1[i] ^ hashID2[i];
			for (int j = 0; j < 8; j++) {
				if ((xor & 1) == 1) {
					return 256 - distance;
				}
				xor = xor >> 1;
				distance++;
			}
		}
		return distance;
	}
}
