package interfaces;

import java.io.*;

public class FileReaderWriterImpl implements FileReaderWriter {
    private String fname;
    private int index;
    private boolean closed;
    private String mode;

    public FileReaderWriterImpl(String name) {
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
                int currentLineNumber = 1;

                while ((line = reader.readLine()) != null) {
                    if (currentLineNumber == this.index) { break;}
                    currentLineNumber++;
                }

                kv.k = line;
                kv.v = String.valueOf(index);

                this.index ++;
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            } 
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
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void open(String mode) {
        this.mode = mode;
        this.index = 0;
        this.closed = false;
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public long getIndex() {
        return this.index;
    }

    @Override
    public String getFname() {
        return this.fname;
    }

    @Override
    public void setFname(String fname) {
        this.fname = fname;
    }
    
}
