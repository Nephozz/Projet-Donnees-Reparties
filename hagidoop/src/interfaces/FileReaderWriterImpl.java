package interfaces;

import java.io.*;

//à modif
public class FileReaderWriterImpl implements FileReaderWriter {
    private String fname;
    private int index;
    private boolean closed;
    private String mode;
    private int fmt;

    public FileReaderWriterImpl(String name, int fmt) {
        this.fname = name;
        this.index = 1;
        this.fmt = fmt;
    }

    @Override
    public KV read() {
        KV kv = new KV();
        if (this.closed) {
            System.out.println("File is closed");
            return kv;
        }
        if (this.mode.equals("r")) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(this.fname));
                String line = null;

                if (this.fmt == FileReaderWriter.FMT_TXT) {
                    while ((line = reader.readLine()) != null) {
                        kv.k = String.valueOf(index);
                        kv.v = line;
                        this.index ++;
                    }
                } else if (this.fmt == FileReaderWriter.FMT_KV) {
                    while ((line = reader.readLine()) != null) {
                        kv.k = line.split(KV.SEPARATOR)[0];
                        kv.v = line.split(KV.SEPARATOR)[1];
                        this.index ++;
                    }
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
