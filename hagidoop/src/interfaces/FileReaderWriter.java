package interfaces;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class FileReaderWriter implements ReaderWriter {
	public static final int FMT_TXT = 0;
	public static final int FMT_KV = 1;
	private String fname;
    private int index;
    private boolean closed;
    private String mode;

    public FileReaderWriter(String name) {
        this.fname = name;
    }

    @Override
    public KV read() {
        KV kv = new KV();
        if (this.closed) {
            System.out.println("File is closed");
            return kv;
        }

        if (this.mode.equals("r")) {
            try (BufferedReader reader = new BufferedReader(new FileReader(this.fname))) {
                String line = null;

                while ((line = reader.readLine()) != null) {
                    kv.k = String.valueOf(index);
                    kv.v = line;

                    this.index ++;
                }
                
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            } 
        } else if (this.mode.equals("w")) {
            kv = null;
            System.out.println("File is opened in write mode");
        } else {
            kv = null;
            System.out.println("Unknown mode: " + this.mode);
        }
        return kv;
    }

    @Override
    public void write(KV record) {
        if (this.closed) {
            System.out.println("File is closed");
            return;
        }

        if (this.mode.equals("w")) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.fname, true))) {
                writer.write(record.k + " " + record.v);
                index ++;
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (this.mode.equals("r")) {
            System.out.println("File is opened in read mode");
        } else {
            System.out.println("Unknown mode: " + this.mode);
        }
    }

    public void open(String mode) {
        this.mode = mode;
        this.index = 0;
        this.closed = false;
    }

    public void close() {
        this.closed = true;
    }

    public long getIndex() {
        return this.index;
    }

    public String getFname() {
        return this.fname;
    }

    public void setFname(String fname) {
        this.fname = fname;
    }
}
