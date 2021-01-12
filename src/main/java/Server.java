import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Scanner;

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
    private ArrayList<String> spreadIP = new ArrayList<String>();
    private String spreadName = "server";
    private final int spreadPort = 4803;
    private SpreadConnection spreadConn;
    private MessageListener msgHandling;

    public static final String consensusGroup = "Consensus";
    public static final String configGroup = "Config";

    public Server(String[] args, String autoIP){
        spreadIP.add("34.89.68.176");
        spreadIP.add("35.246.58.5");

        if(args.length > 0 && this.spreadIP.contains(args[0]))
            this.spreadIP.add(args[0]);

        grcpIP = autoIP;
        this.startServers();
        this.shutdownServers();
    }

    private void startServers(){
        for(String ip: spreadIP){
            try  {
                spreadConn= new SpreadConnection();
                spreadConn.connect(
                        Inet4Address.getByName(ip),
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

                break;
            }catch(SpreadException e)  {
                System.err.println("There was an error connecting to the daemon.");
            }catch(UnknownHostException e) {
                System.err.println("Can't find the daemon " + this.spreadIP);
            }catch (IOException e) {
                System.err.println("Can't Start Grcp Server " + this.grcpPort);
            }
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
            Scanner sc = new Scanner(System.in);
            String a = sc.next();
            this.grcpServer.shutdown();
            this.spreadConn.remove(this.msgHandling);
            this.spreadConn.disconnect();


        } catch (SpreadException e) {
            System.err.println("error disconnecting spread server ");
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
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("google.com", 80));
            String ip = socket.getLocalAddress().toString().substring(1);

            Server server = new Server(args, ip);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}