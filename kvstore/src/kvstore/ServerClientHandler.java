package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class ServerClientHandler implements NetworkHandler {

    private KVServer kvServer;
    private ThreadPool threadPool;

    /**
     * Constructs a ServerClientHandler with ThreadPool of a single thread.
     *
     * @param kvServer KVServer to carry out requests
     */
    public ServerClientHandler(KVServer kvServer) {
        this(kvServer, 1);
    }

    /**
     * Constructs a ServerClientHandler with ThreadPool of thread equal to
     * the number passed in as connections.
     *
     * @param kvServer KVServer to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public ServerClientHandler(KVServer kvServer, int connections) {
        this.kvServer = kvServer;
        threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request for a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        ClientHandler handler = new ClientHandler(client);
        try {
            threadPool.addJob(handler);
        }
        catch(InterruptedException ie) {
            //ignore the error
        }
    }

    /**
     * Runnable class with routine to service a request from the client.
     */
    private class ClientHandler implements Runnable {

        private Socket client;

        /**
         * Construct a ClientHandler.
         *
         * @param client Socket connected to client with the request
         */
        public ClientHandler(Socket client) {
            this.client = client;
        }

        /**
         * Processes request from client and sends back a response with the
         * result. The delivery of the response is best-effort. If we are
         * unable to return a response, there is nothing else we can do.
         */
        @Override
        public void run() {
            KVMessage response = new KVMessage(KVConstants.RESP);
            try {
                KVMessage message = new KVMessage(client);
                if(message.getMsgType().equals(KVConstants.PUT_REQ)) {
                    kvServer.put(message.getKey(), message.getValue());
                    response.setMessage(KVConstants.SUCCESS);
                }
                else if(message.getMsgType().equals(KVConstants.GET_REQ)) {
                    String value = kvServer.get(message.getKey());
                    response.setValue(value);
                    response.setKey(message.getKey());
                }
                else if(message.getMsgType().equals(KVConstants.DEL_REQ)) {
                    kvServer.del(message.getKey());
                    response.setMessage(KVConstants.SUCCESS);
                }
                else throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
            }
            catch(KVException e)
            {
//            	System.out.println("e.message = " + e.getKVMessage().getMessage());
                response.setKey(null);
                response.setValue(null);
                response.setMessage(e.getKVMessage().getMessage());
//                System.out.println("KVException: " + response.getMessage());
            }
            finally
            {
            	try {
                response.sendMessage(client);
            	}
            	catch (Exception e){
            		
            	}
            }
            try{
            	client.close();
            } catch (Exception e){
            	
            }
            
        }
    }

}
