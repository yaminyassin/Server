package StorageServer;

import io.grpc.stub.StreamObserver;
import rpcstubs.Valor;
import spread.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;


public class MessageListener implements AdvancedMessageListener {

    private SpreadConnection connection;
    private StorageService storageService;
    private JsonRepo storageRepo;
    private HashMap<String, StreamObserver<Valor>> readWaitList;
    private ArrayList<SpreadGroup> serverList = new ArrayList<>();
    private final Random randomPicker = new Random();

    public MessageListener(StorageService storageService) {
        this.storageService = storageService;
        this.connection = storageService.getSpreadConn();
        this.storageRepo = storageService.getStorageRepo();
        this.readWaitList = storageService.getReadWaitList();
    }

    @Override
    public void regularMessageReceived(SpreadMessage spreadMessage) {
        try {
            String msg = spreadMessage.getSender().toString();

            if( ! this.connection.getPrivateGroup().toString().equals(msg) ){

                MsgData obj = (MsgData) spreadMessage.getObject();

                switch (obj.msgType) {
                    case READ_REQ:
                        System.out.println("Recieved READ_REQ From: " + msg + "\n");
                        String value = (String) storageRepo.get(obj.key);

                        if(value != null)
                            this.storageService.sendSpreadMSG(
                                    Server.consensusGroup,
                                    MsgType.READ_RES,
                                    obj.key,
                                    value);
                        break;

                    case READ_RES:
                        System.out.println("Recieved READ_RES With {" + obj.key + ", " + obj.value + "}. \n");

                        if(storageRepo.contains(obj.key)){
                            this.storageService.sendSpreadMSG(
                                    Server.consensusGroup,
                                    MsgType.INVALIDATE,
                                    obj.key,
                                    obj.value);
                        }else{
                            Valor valor = Valor
                                    .newBuilder()
                                    .setValue(obj.value)
                                    .build();

                            StreamObserver<Valor> cliente = readWaitList.get(obj.key);

                            cliente.onNext(valor);
                            cliente.onCompleted();
                            readWaitList.remove(obj.key, cliente);
                        }
                        break;

                    case INVALIDATE:
                        System.out.println("Recieved INVALIDATE for Key: " + obj.key + "\n");
                        storageRepo.rem(obj.key);
                        break;

                    case CONFIG_REQ:
                        System.out.println("Recieved CONFIG_REQ(IP, PORT)" );
                        System.out.println("Sending CONFIG_RES to Sender.. \n");
                        this.storageService.sendSpreadMSG(
                                Server.configGroup,
                                MsgType.CONFIG_RES,
                                Server.grcpIP,
                                String.valueOf(Server.grcpPort));
                        break;

                    case ELECTION_REQ:
                        System.out.println("Recieved ELECTION_REQ.");
                        System.out.println("SENDING ELECTION RES... \n");

                        sendRepository(msg);
                        break;

                    case ELECTION_RES:
                        System.out.println("Recieved ELECTION_RES.");
                        System.out.println("Saving New Repository... \n");
                        getRepository(obj.repo);
                        break;
                }
            }

        } catch (SpreadException e) {
            System.err.println("Error on Recieved SpreadMessage \n");
        }
    }

    @Override
    public void membershipMessageReceived(SpreadMessage spreadMessage) {
        MembershipInfo info = spreadMessage.getMembershipInfo();

        if(! spreadMessage.getSender().toString().equals("Config")){
            if(info.isCausedByJoin() && CheckSameSender(info.getJoined())){
                RequestElection(info);
            }
            else if(info.isCausedByJoin() && ! CheckSameSender(info.getJoined())){

                System.out.println("Added " + info.getJoined() + " to Repo");
                serverList.add(info.getJoined());
            }
            else if(info.isCausedByLeave() && ! CheckSameSender(info.getLeft())){

                System.out.println("Removed " + info.getLeft() + " From Repo");
                serverList.remove(info.getLeft());
            }
            else if(info.isCausedByDisconnect() && ! CheckSameSender(info.getDisconnected())){

                System.out.println("Removed " + info.getDisconnected() + " From Repo");
                serverList.remove(info.getDisconnected());
            }

            System.out.println("Servidores Atuais : ");
            for(SpreadGroup sg : serverList)
                System.out.println(sg);
        }
    }

    private boolean CheckSameSender(SpreadGroup group ){
        return connection.getPrivateGroup().toString().equals(group.toString());
    }

    public void RequestElection(MembershipInfo info){
        ArrayList<SpreadGroup> aux = new ArrayList<>(Arrays.asList(info.getMembers()));

        aux.remove(this.connection.getPrivateGroup());
        serverList = aux;

        if(serverList.size() > 0){

            SpreadGroup picked = serverList.get(randomPicker.nextInt(serverList.size()));

            this.storageService.sendSpreadMSG(
                    picked.toString(),
                    MsgType.ELECTION_REQ,
                    null,
                    null
            );
        }else{
            System.out.println("No Election Process Started..");
        }
    }

    public void sendRepository(String group) {
        try {
            MsgData data = new MsgData();
            data.setMsgType(MsgType.ELECTION_RES);
            data.setRepo(storageRepo);
            data.setKey(this.connection.getPrivateGroup().toString());

            SpreadMessage msg = new SpreadMessage();
            msg.setSafe();
            msg.addGroup(group);
            msg.setObject(data);

            this.connection.multicast(msg);
        } catch (SpreadException e) {
            System.err.println("Error on sendSpreadmsg on server.StorageService \n");
        }
    }

    public void getRepository(JsonRepo new_repo){
        storageRepo = new_repo;
        storageRepo.writeToFile();
    }

}