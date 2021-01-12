import java.io.Serializable;

public class MsgData implements Serializable {
    MsgType msgType;
    String key;
    String value;

    public MsgData(MsgType msgType, String key, String value ){
        this.value = value;
        this.key = key;
        this.msgType = msgType;
    }
    public MsgData(MsgType msgType, String key){
        this.msgType = msgType;
        this.key = key;
    }
    public MsgData(String key, String value){
        this.value = value;
        this.key = key;
    }
    public MsgData(MsgType msgType) {
        this.msgType = msgType;
    }
    public MsgData(){}
}
