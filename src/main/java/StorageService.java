import io.grpc.stub.StreamObserver;
import rpcstubs.Void;
import rpcstubs.*;
import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadMessage;


public class StorageService extends StorageServiceGrpc.StorageServiceImplBase {

    private JsonRepo repo;
    private SpreadConnection spreadConn;

    public StorageService(JsonRepo repo, SpreadConnection spreadConn){
        this.repo = repo;
        this.spreadConn = spreadConn;
    }

    public JsonRepo getRepo(){
        return this.repo;
    }

    public SpreadConnection getSpreadConn() {
        return spreadConn;
    }

    @Override
    public void write(Par request, StreamObserver<Void> responseObserver) {
        String key = request.getChave().getValue();
        String value = request.getValor().getValue();

        this.sendSpreadMSG(
                Server.consensusGroup,
                MsgType.INVALIDATE,
                key,
                value
        );

        this.repo.set(key, value);
        System.out.println("Written : "+ key + ", " + value);

        responseObserver.onNext(Void
                .newBuilder()
                .build()
        );
        responseObserver.onCompleted();
    }

    @Override
    public void read(Chave request, StreamObserver<Valor> responseObserver) {
        String key = request.getValue();
        int contador = 0;
        String value = (String) this.repo.get(key);

        // -------pedir ao grupo Consensus pela chave
        if(value == null){
            System.out.println("Key Doesn't Exist.. Requesting From Cluster. \n");
            sendSpreadMSG(
                    Server.consensusGroup,
                    MsgType.READ_REQ,
                    key,
                    null);
        }

        // -------verificar se recebeu valor da chave 5x
        while(value == null && contador < 5){
            try {
                Thread.sleep(1000);
                value = (String) this.repo.get(key);
                contador +=1;
            }catch (InterruptedException e) {
                System.err.println("Something Went Wrong on StorageService.read()");
            }
        }

        /*
        se recebeu, envia msg para invalidar chave aos outros servers e
        finalmente, envia de volta o valor da chave ao cliente
        */
        if(value != null){
            Valor valor = Valor
                    .newBuilder()
                    .setValue(value)
                    .build();

            repo.rem(key);

            responseObserver.onNext(valor);
            responseObserver.onCompleted();

        }else{ //se nao existe envia um valor vazio
            Valor valor = Valor
                    .newBuilder()
                    .setVoid(Void.newBuilder().build())
                    .build();

            System.out.println("Key Doesn't Exist. \n");

            responseObserver.onNext(valor);
            responseObserver.onCompleted();
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
            System.err.println("Error on senfSpreadmsg on  \n");
        }
    }


}