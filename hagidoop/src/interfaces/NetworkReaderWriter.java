package interfaces;

public class NetworkReaderWriter implements ReaderWriter {
	private String fname;
    private int index;
    private boolean closed;

	public KV read() {
        return null;
    }

    public void write(KV kv) {

    }

    public void openServer() {

    }

	public void openClient() {

    }

	public NetworkReaderWriter accept() {
        return null;
    }
	public void closeServer() {

    }

	public void closeClient() {

    }
}
