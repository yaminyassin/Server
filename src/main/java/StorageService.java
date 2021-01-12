import io.grpc.stub.StreamObserver;
import rpcstubs.Void;
import rpcstubs.*;
import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadGroup;
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

        /*
        Invalidar a chave nos outros servers!!!!!
         */

        this.repo.set(key, value);

        System.out.println("Guardou: "+ key + "," + value);
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
            System.out.println("Valor nao Existe!.. Pedindo ao Grupo");
            sendSpreadmsgOBJ(
                    Server.consensusGroup,
                    MsgType.REQUEST,
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
                System.out.println("Something went wrong on StorageService.read()");
            }
        }

        /*
        se recebeu, envia msg para invalidar chave aos outros servers e
        finalmente, envia de volta o valor da chave ao cliente
        */
        if(value!= null){
            this.sendSpreadmsgOBJ(
                    Server.consensusGroup,
                    MsgType.INVALIDATE,
                    key,
                    value);

            Valor valor = Valor
                    .newBuilder()
                    .setValue(value)
                    .build();

            System.out.println("Leu valor: "+ valor.getValue());

            responseObserver.onNext(valor);
            responseObserver.onCompleted();

        }else{ //se nao existe envia um valor vazio
            Valor valor = Valor
                    .newBuilder()
                    .setVoid(Void.newBuilder().build())
                    .build();
            System.out.println("sending VOID to client");
            responseObserver.onNext(valor);
            responseObserver.onCompleted();
        }
    }

    public void sendSpreadmsgOBJ(String group, MsgType msgType, String key, String value) {
        try {
            SpreadMessage msg = new SpreadMessage();
            msg.setSafe();

            msg.addGroup(group);
            msg.setObject(new MsgData(msgType, key, value));
            this.spreadConn.multicast(msg);

        } catch (SpreadException e) {
            e.printStackTrace();
            System.err.println("Error on Spread Send Message");
        }
    }


}