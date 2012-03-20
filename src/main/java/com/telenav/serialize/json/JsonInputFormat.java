/**
 * Copyright(c) 2008 Business.com. All rights reserved.
Ê*
Ê* CloudBase is free software: you can redistribute it and/or modify
Ê* it under the terms of the GNU General Public License as published by
Ê* the Free Software Foundation, version 2. By downloading CloudBase you
 * hereby acknowledge and agree to the terms and conditions of the GNU
 * General Public License.
 * 
 * CloudBase is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THEREFORE,
 * EXCEPT AS SPECIFIED INÊWRITING THE SOFTWAREÊISÊMADE AVAILABEÊON AN 
 * AS IS BASIS AND SUBJECT TO THE RESTRICTIONS AND CONDITIONS OF THE 
 * GNU GENERAL PUBLIC LICENSE,ÊBUSINESS.COM SHALL HAVE NO LIABILITY 
 * FOR THE SOFTWARE PROVIDED IN FURTHERANCE OF THIS AGREEMENT;Ê
 * BUSINESS.COMÊMAKES ANDÊLICENSEE RECEIVES NO WARRANTIES, EXPRESS, 
 * IMPLIED, STATUTORY, OR IN ANY OTHER PROVISIONÊOR ANY OTHER COMMUNICATION;
 * ANDÊBUSINESS.COM SPECIFICALLY DISCLAIMS ANY WARRANTY OF MERCHANTABILITY 
 * AND FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with CloudBase.Ê If not, see <http://www.gnu.org/licenses/>.
 */
/**
 *
 * @author yru
 */
package com.telenav.serialize.json;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;

public class JsonInputFormat extends FileInputFormat<LongWritable, Text>
        implements JobConfigurable {

	@Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit,
            JobConf job,
            Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new JsonRecordReader((FileSplit) genericSplit, job);
    }

   @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
        for (FileStatus status : listStatus(job)) {
            Path fileName = status.getPath();
            if (status.isDir()) {
                throw new IOException("Not a file: " + fileName);
            }
            FileSystem fs = fileName.getFileSystem(job);
            LineReader lr = null;
            try {
                FSDataInputStream in = fs.open(fileName);
                lr = new LineReader(in, job);
                Text line = new Text();
                int count = 0;
                long begin = 0;
                long length = 0;
                int num = -1;
                while ((num = lr.readLine(line)) > 0) {
                    length += num;
                    int newCount = countBraces(line);
                                        
                    if (count == 0 && newCount == Integer.MAX_VALUE) {
                        // ignore this line
                        begin += length;
                        length = 0;
                    } else {
                        count += (newCount == Integer.MAX_VALUE) ? 0 : newCount;
                    }
                    if (count == 0 && length > 0) {
                        splits.add(new FileSplit(fileName, begin, length, new String[]{}));
                        begin += length;
                        length = 0;
                    }
                }
            } finally {
                if (lr != null) {
                    lr.close();
                }
            }
        }
        return splits.toArray(new FileSplit[splits.size()]);
    }

    // return 0, if the text contains equal number of left and right braces
    // return Integer.MAX_VALUE, if the text doesn't contain any brace
    
    private int countBraces(Text text) {
        boolean hasBrace = false;
        int count = 0;
        try {
            byte[] bytes = text.toString().getBytes("utf-8");
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] == JsonRecordReader.LEFT_BRACE) {
                    hasBrace = true;
                    count++;
                } else if (bytes[i] == JsonRecordReader.RIGHT_BRACE) {
                    hasBrace = true;
                    count--;
                }
            }
        } catch (IOException ioe) {
        }
        return hasBrace ? count : Integer.MAX_VALUE;
    }

    public void configure(JobConf conf) {
    }
}
