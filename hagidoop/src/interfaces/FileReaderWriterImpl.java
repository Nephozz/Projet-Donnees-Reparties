package interfaces;

import java.io.*;

//à modif
public class FileReaderWriterImpl implements FileReaderWriter {
    private String fname;
    private int index;
    private boolean closed;
    private String mode;
    private int fmt;

    private BufferedWriter writer;
    private BufferedReader reader;

    public FileReaderWriterImpl(String name, int fmt) {
        this.fname = name;
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
                String line = null;

                if (this.fmt == FileReaderWriter.FMT_TXT) {
                    if ((line = reader.readLine()) != null) {
                        kv.k = String.valueOf(index);
                        kv.v = line;
                        this.index ++;
                    } else {
                        kv = null;
                    }
                } else if (this.fmt == FileReaderWriter.FMT_KV) {
                    if ((line = reader.readLine()) != null) {
                        kv.k = line.split(KV.SEPARATOR)[0];
                        kv.v = line.split(KV.SEPARATOR)[1];
                        this.index ++;
                    } else {
                        kv = null;
                    }
                }
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
            try {
                if (record != null) {
                    writer.write(record.k + KV.SEPARATOR + record.v+"\n");
                    this.index ++;
                }
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
        try {
            if (mode.equals("r")) {
                reader = new BufferedReader(new FileReader(this.fname));;
            } else if (mode.equals("w")) {
                writer = new BufferedWriter(new FileWriter(this.fname, true));
            } else {
                throw new IllegalArgumentException("Invalid mode: " + mode);
            }
            index = 0;
        } catch (IOException e) {
            System.err.println("Error opening file: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        this.closed = true;
        try {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing file: " + e.getMessage());
        }
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
