package StorageServer;

import java.io.Serializable;

public class MsgData implements Serializable {
    MsgType msgType;
    String key;
    String value;
    JsonRepo repo;
    public MsgData(){}

    public void setMsgType(MsgType msgType) {
        this.msgType = msgType;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setRepo(JsonRepo repo) {
        this.repo = repo;
    }
}
