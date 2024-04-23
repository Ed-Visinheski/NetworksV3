import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class NetworkMap{
    private String name;
    private String address;
    private byte[] hashId;
    private List<HashMap<Integer, HashMap<String, String>>> networkMap;

    public NetworkMap(String name, String address) throws Exception {
        this.name = name;
        this.address = address;
        this.hashId = HashID.computeHashID(name + "\n");
        this.networkMap = new ArrayList<>();
    }

    public void addNode(String name, String address) throws Exception {
        byte[] hash2 = HashID.computeHashID(name + "\n");
        HashID.bytesToHex(hash2);
        int distance1 = HashID.calculateDistance(this.hashId, hash2);
        HashMap<String, String> nameHash = new HashMap<>();
        nameHash.put(name, address);
        HashMap<Integer, HashMap<String,String>> distanceHash = new HashMap<>();
        distanceHash.put(distance1, nameHash);
        this.networkMap.add(distanceHash);
    }

    public void removeNode(String name){
        this.networkMap.remove(name);
    }

    public List<HashMap<Integer, HashMap<String, String>>> getNetworkMap() {
        return this.networkMap;
    }

    //
    public void viewNetworkMap(){
        for(HashMap<Integer, HashMap<String, String>> distanceHash : this.networkMap){
            for(Integer distance : distanceHash.keySet()){
                System.out.println("Distance: " + distance);
                for(String name : distanceHash.get(distance).keySet()){
                    System.out.println("Name: " + name);
                    System.out.println("Address: " + distanceHash.get(distance).get(name)+"\n");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        NetworkMap map = new NetworkMap("Alice1", "meow");
        map.addNode("1ecilA", "woof");
        map.addNode("1ecilA", "quack");
        map.addNode("1ecilA", "mepw");
        map.addNode("1ecilA", "milk");
        map.viewNetworkMap();
    }













}