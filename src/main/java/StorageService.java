import io.grpc.stub.StreamObserver;
import rpcstubs.Void;
import rpcstubs.*;
import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadMessage;

import java.util.HashMap;


public class StorageService extends StorageServiceGrpc.StorageServiceImplBase {

    private JsonRepo storageRepo;
    private SpreadConnection spreadConn;
    private HashMap<String, StreamObserver<Valor>> readWaitList = new HashMap<>();

    public StorageService(JsonRepo repo, SpreadConnection spreadConn){
        this.storageRepo = repo;
        this.spreadConn = spreadConn;
    }

    /**
     * 1 - Faco write da chave/valor no repositorio;
     * 2 - Envio um READ_REQ ao grupo Consensus;
     * 3 - Se alguem tiver a mesma chave, envio uma msg Invalidate;
     *
     * @param responseObserver
     */
    @Override
    public void write(Par request, StreamObserver<Void> responseObserver) {
        String key = request.getChave().getValue();
        String value = request.getValor().getValue();

        this.storageRepo.set(key, value);
        System.out.println("Written : "+ key + ", " + value);

        this.sendSpreadMSG(
                Server.consensusGroup,
                MsgType.READ_REQ,
                key,
                value
        );

        responseObserver.onNext(Void
                .newBuilder()
                .build()
        );
        responseObserver.onCompleted();
    }

    @Override
    public void read(Chave request, StreamObserver<Valor> responseObserver) {
        String key = request.getValue();

        String value = (String) this.storageRepo.get(key);

        // -------pedir ao grupo Consensus pela chave
        if(value == null){
            sendSpreadMSG(
                    Server.consensusGroup,
                    MsgType.READ_REQ,
                    key,
                    null);

            readWaitList.put(key, responseObserver);
        }

        int contador = 0;

        while(contador <= 5 && readWaitList.containsValue(responseObserver)){
            try {
                Thread.sleep(1000);
                contador +=1;
            } catch (InterruptedException e) {
                System.out.println("erro no threadsleep do read");
            }
        }

        if(readWaitList.containsValue(responseObserver)){
            Valor valor = Valor
                    .newBuilder()
                    .setVoid(Void.newBuilder().build())
                    .build();

            System.out.println("Key Doesn't Exist. \n");

            responseObserver.onNext(valor);
            responseObserver.onCompleted();
            readWaitList.remove(key, responseObserver);
        }
    }

    public void sendSpreadMSG(String group, MsgType msgType, String key, String value) {
        try {
            SpreadMessage msg = new SpreadMessage();
            msg.setSafe();

            msg.addGroup(group);
            msg.setObject(new MsgData(msgType, key, value));
            this.spreadConn.multicast(msg);

        } catch (SpreadException e) {
            System.err.println("Error on sendSpreadmsg on StorageService \n");
        }
    }


    public JsonRepo getStorageRepo() {
        return storageRepo;
    }

    public SpreadConnection getSpreadConn() {
        return spreadConn;
    }

    public HashMap<String, StreamObserver<Valor>> getReadWaitList() {
        return readWaitList;
    }
}