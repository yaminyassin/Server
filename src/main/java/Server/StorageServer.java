package Server;

import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.*;
import java.util.Scanner;

import spread.*;


public class StorageServer {
    //vars de grcp
    private final String filename = "info.json";
    private JsonRepo repo = new JsonRepo(this.filename);
    public static String grcpIP;
    public  static int grcpPort = 5000;
    private StorageService storageService;
    private io.grpc.Server grcpServer;

    //vars do spread
    private String spreadIP = "35.246.58.5";
    private SpreadConnection spreadConn;
    private MessageListener msgHandling;
    private String spreadName = "serv";
    public static final String consensusGroup = "Consensus";
    public static final String configGroup = "Config";

    public StorageServer(String[] args, String autoIP){

        if(args.length > 0 && spreadIP.contains(args[0])){
            spreadName = args[0];
            spreadIP= args[1];
            grcpPort = Integer.parseInt(args[2]);

        }

        grcpIP = autoIP;
        this.startServers();
        this.shutdownServers();
    }

    private void startServers(){
        try  {
            spreadConn= new SpreadConnection();
            spreadConn.connect(
                    Inet4Address.getByName(spreadIP),
                    4803,spreadName,
                    false, true);

            if(spreadConn.isConnected()){
                this.storageService = new StorageService(this.repo, this.spreadConn);

                //servico do grcp
                grcpServer = ServerBuilder
                        .forPort(grcpPort)
                        .addService(this.storageService)
                        .build();

                grcpServer.start();

                msgHandling = new MessageListener(this.storageService);

                spreadConn.add(msgHandling);
                joingSpreadGroup(consensusGroup);
                joingSpreadGroup(configGroup);

                //apos entrar ao grupo, enviar msg com dados ip,port ao configServer
                storageService.sendSpreadMSG(
                        configGroup,
                        MsgType.CONFIG_RES,
                        grcpIP,
                        String.valueOf(grcpPort)
                );
            }else{
                System.err.println("Coudn't Connect to Spread Server.. shutting donw");
                System.exit(1);
            }
        }catch (SpreadException e) {
            System.err.println("Error Connecting to Daemon \n");
            System.exit(1);
        } catch(UnknownHostException e) {
            System.err.println("Can't Find Daemon, Unkown Host " + spreadIP +"\n");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Can't Start Grcp server.Server " + grcpPort + "\n");
            System.exit(1);
        }

    }


    private void joingSpreadGroup(String name){
        try {
            SpreadGroup group = new SpreadGroup();
            group.join(spreadConn, name);
        } catch (SpreadException e) {
            System.err.println("Failed to join Group " + name + "\n");
        }
    }



    public void shutdownServers(){
        try {
            Scanner sc = new Scanner(System.in);
            sc.nextLine();

            grcpServer.shutdown();
            spreadConn.remove(msgHandling);
            spreadConn.disconnect();


        } catch (SpreadException e) {
            System.err.println("Error Disconnecting Spread server.Server \n");
            System.exit(0);
        }
        System.exit(0);
    }


    public static void main(String[] args) {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("google.com", 80));
            String ip = socket.getLocalAddress().toString().substring(1);

            StorageServer server = new StorageServer(args, ip);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}