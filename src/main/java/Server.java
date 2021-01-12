import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import spread.*;


public class Server{
    //vars de grcp
    private final String filename = "info.json";
    private JsonRepo repo = new JsonRepo(this.filename);
    public static String grcpIP;
    public  static int grcpPort = 5000;
    private StorageService storageService;
    private io.grpc.Server grcpServer;

    //vars do spread
    private String spreadIP = "34.89.68.176";
    private String spreadName = "server";
    private final int spreadPort = 4803;
    private SpreadConnection spreadConn;
    private MessageListener msgHandling;

    public static final String consensusGroup = "Consensus";
    public static final String configGroup = "Config";

    public Server(String[] args){
        if(args.length > 0)
            this.spreadIP = args[0];


         try {
            this.grcpIP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.err.println("Coudn't get GRCP IP ADDRESS");
        }

        this.startServers();
        this.shutdownServers();
    }

    private void startServers(){
        try  {
            spreadConn= new SpreadConnection();
            spreadConn.connect(
                    InetAddress.getByName(this.spreadIP),
                    this.spreadPort,this.spreadName,
                    false, true);


            this.storageService = new StorageService(this.repo, this.spreadConn); //servico do grcp
            this.grcpServer = ServerBuilder
                    .forPort(grcpPort)
                    .addService(this.storageService)
                    .build();

            this.grcpServer.start();

            msgHandling = new MessageListener(this.storageService);
            spreadConn.add(msgHandling);

            joingSpreadGroup(consensusGroup);
            joingSpreadGroup(configGroup);
            //apos entrar ao grupo, enviar msg com dados ip,port ao configServer
            sendSpreadmsgOBJ(MsgType.SENTCONFIG, grcpIP, String.valueOf(grcpPort));

        }
        catch(SpreadException e)  {
            System.err.println("There was an error connecting to the daemon.");
            e.printStackTrace();
            System.exit(1);
        }
        catch(UnknownHostException e) {
            System.err.println("Can't find the daemon " + this.spreadIP);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Can't Start Grcp Server " + this.grcpPort);
            System.exit(1);
        }
    }


    private void joingSpreadGroup(String name){
        try {
            SpreadGroup group = new SpreadGroup();
            group.join(this.spreadConn, name);
        } catch (SpreadException e) {
            e.printStackTrace();
            System.err.println("Failed to join Group, " + name);
        }
    }



    public void shutdownServers(){
        try { // shutdown and quit
            this.grcpServer.awaitTermination();
            this.grcpServer.shutdown();
            this.spreadConn.remove(this.msgHandling);
            this.spreadConn.disconnect();
        } catch (SpreadException | InterruptedException e) {
            System.err.println("error disconnecting spread server ");
            e.printStackTrace();
        }
        System.exit(0);
    }


    private void sendSpreadmsgOBJ(MsgType msgType, String key, String value) {

        try {
            SpreadMessage msg = new SpreadMessage();
            msg.setSafe();
            msg.addGroup(this.configGroup);
            msg.setObject(new MsgData(msgType, key, value));

            this.spreadConn.multicast(msg);

        } catch (SpreadException e) {
            e.printStackTrace();
            System.err.println("Error on Spread Send Message");
        }
    }

    public static void main(String[] args) {


        InetAddress ip;
        String hostname;
        try {
            ip = Inet4Address.getLocalHost();
            hostname = ip.getHostName();

            System.out.println("Your current IP address : " + ip.getHostAddress());
            System.out.println("Your current Hostname : " + hostname);
            System.out.println("Your current Port : " + InetAddress.getLoopbackAddress());

        } catch (UnknownHostException e) {

            e.printStackTrace();
        }
        Server server = new Server(args);
    }
}