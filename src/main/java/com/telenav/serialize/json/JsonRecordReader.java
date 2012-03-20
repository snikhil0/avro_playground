package com.telenav.serialize.json;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class JsonRecordReader implements RecordReader<LongWritable, Text> {

    private long start;
    private long end;
    private FSDataInputStream fsin;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    public static final byte LEFT_BRACE = 123;
    public static final byte RIGHT_BRACE = 125;
    public static final byte END_OF_LINE = 10;
    public static final byte END_OF_FILE = -1;

    public JsonRecordReader(FileSplit split, JobConf jobConf) throws IOException {
        start = split.getStart();
        end = start + split.getLength();
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(jobConf);
        fsin = fs.open(split.getPath());
        fsin.seek(start);
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    public long getPos() throws IOException {
        return fsin.getPos();
    }

    public void close() throws IOException {
        fsin.close();
    }

    public float getProgress() throws IOException {
        return ((float) (fsin.getPos() - start)) / ((float) (end - start));
    }

    public boolean next(LongWritable key, Text value) throws IOException {
        if (fsin.getPos() < end) {
            try {
                int count = 0;
                while ((count = readLine(count)) != 0) {
                }
                key.set(fsin.getPos());
                value.set(buffer.getData(), 0, buffer.getLength());
                return true;
            } finally {
                buffer.reset();
            }
        }
        return false;
    }

    private int readLine(int count) throws IOException {
        while (true) {
            int b = fsin.read();
            if (b == END_OF_FILE) {
                return count;
            }
            buffer.write(b);

            if (b == LEFT_BRACE)
                count++;
            else if (b == RIGHT_BRACE)
                count--;
            else if (b == END_OF_LINE)
                return count; 
        }
    }

}
