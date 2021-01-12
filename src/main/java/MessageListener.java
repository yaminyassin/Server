import spread.*;


public class MessageListener implements AdvancedMessageListener {

    private SpreadConnection connection;
    private StorageService storageService;

    public MessageListener(StorageService storageService) {
        this.storageService = storageService;
        this.connection= storageService.getSpreadConn();
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
                        String value = (String) this.storageService.getRepo().get(obj.key);

                        if(value != null)
                            this.storageService.sendSpreadMSG(
                                    Server.consensusGroup,
                                    MsgType.READ_RES,
                                    obj.key,
                                    value);
                        break;

                    case READ_RES:
                        System.out.println("Recieved READ_RES With {" + obj.key + ", " + obj.value + "}. \n");
                        this.storageService.getRepo().set(obj.key, obj.value); //guardar o valor recebido
                        break;

                    case INVALIDATE:
                        System.out.println("Recieved INVALIDATE for Key: " + obj.key + "\n");
                        this.storageService.getRepo().rem(obj.key);
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
