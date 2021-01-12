import io.grpc.stub.StreamObserver;
import rpcstubs.Valor;
import spread.*;

import java.util.HashMap;


public class MessageListener implements AdvancedMessageListener {

    private SpreadConnection connection;
    private StorageService storageService;
    private JsonRepo storageRepo;
    private HashMap<String, StreamObserver<Valor>> readWaitList;

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
                        System.out.println("Recieved Data Request (IP, PORT) \n" );
                        this.storageService.sendSpreadMSG(
                                Server.configGroup,
                                MsgType.CONFIG_RES,
                                Server.grcpIP,
                                String.valueOf(Server.grcpPort));
                }
            }

        } catch (SpreadException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void membershipMessageReceived(SpreadMessage spreadMessage) {
        PrintMessages.printMembershipInfo(spreadMessage.getMembershipInfo());
    }

}
