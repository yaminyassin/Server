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

            if(this.connection.getPrivateGroup().toString().equals(msg)){
                System.out.println("Same sender... Ignoring");
            }
            else {
                MsgData obj = (MsgData) spreadMessage.getObject();

                switch (obj.msgType) {
                    case REQUEST:
                        System.out.println("GOT REQUEST FROM -> " + msg);
                        String value = (String) this.storageService.getRepo().get(obj.key);

                        if(value != null)
                            this.storageService.sendSpreadmsgOBJ(
                                    Server.consensusGroup,
                                    MsgType.RESPONSE,
                                    obj.key,
                                    value);
                        break;

                    case RESPONSE:
                        System.out.println("GOT RESPONSE FROM -> " + msg);
                        this.storageService.getRepo().set(obj.key, obj.value); //guardar o valor recebido
                        break;

                    case INVALIDATE:
                        System.out.println("GOT INVALIDATE FROM -> " + msg);
                        this.storageService.getRepo().rem(obj.key);
                        break;

                    case REQCONFIG:
                        System.out.println("REQUESTED INFO " );

                        this.storageService.sendSpreadmsgOBJ(
                                Server.configGroup,
                                MsgType.SENTCONFIG,
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
