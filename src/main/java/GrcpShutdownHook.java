public class GrcpShutdownHook extends Thread{

    private Server server;

    public GrcpShutdownHook(Server server){
        this.server = server;

    }

    public void run(){
        System.out.println("Shutdown Sucessfull");
    }
}
