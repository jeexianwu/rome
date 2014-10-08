package com.duomai.bigdata.textmining;


import java.io.OutputStreamWriter;
import java.io.Writer;


import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;



public class ReaderSequenceFile {

	public static void main(String[] args) throws Exception {
		
		Path[] pathArr;
	    Configuration conf = new Configuration();
	    Path input = new Path(args[0]);
	    
	    FileSystem fs = input.getFileSystem(conf);
	    if (fs.getFileStatus(input).isDir()) {
	      pathArr = FileUtil.stat2Paths(fs.listStatus(input, PathFilters.logsCRCFilter()));
	    } else {
	      pathArr = new Path[1];
	      pathArr[0] = input;
	    }


	    Writer writer;
	    writer = new OutputStreamWriter(System.out, Charsets.UTF_8);
	    
	    try {
	      for (Path path : pathArr) {
	    	  writer.append("Input Path: ").append(String.valueOf(path)).append('\n');
	          @SuppressWarnings("resource")
	          SequenceFileIterator<?, ?> iterator = new SequenceFileIterator<Writable, Writable>(path, true, conf);
	          writer.append("Key class: ").append(iterator.getKeyClass().toString());
	          writer.append(" Value Class: ").append(iterator.getValueClass().toString()).append('\n');
	          
	          long count = 0;
	          while (iterator.hasNext()) {
	            Pair<?, ?> record = iterator.next();
	            String key = record.getFirst().toString();
	            writer.append("Key: ").append(key);
	            String str = record.getSecond().toString();
	            writer.append(": Value: ").append(str);
	            writer.write('\n');
	            count += 1;
	          }
	            writer.append("Count: ").append(String.valueOf(count)).append('\n');
	        }
	      writer.flush();

	    } finally {
	    	System.out.println("FINISHED");
	    }

	}
}
