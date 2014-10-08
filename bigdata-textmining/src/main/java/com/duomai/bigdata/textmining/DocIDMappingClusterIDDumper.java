package com.duomai.bigdata.textmining;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DocIDMappingClusterIDDumper extends Configured implements Tool,
Mapper<IntWritable, WeightedVectorWritable, Text, Text> {

private transient static Logger log = LoggerFactory
    .getLogger(DocIDMappingClusterIDDumper.class);

public static void main(String[] args) {
	
	log.info("START MAPPING DOCID WITH CLUSTERID");
int res;
try {
    res = ToolRunner.run(new Configuration(),
            new DocIDMappingClusterIDDumper(), args);
} catch (Exception e) {
    res = -1;
}
System.exit(res);
}

public int run(String[] args) throws Exception {

Options options = new Options();
// automatically generate the help statement
HelpFormatter formatter = new HelpFormatter();
// create the parser
CommandLineParser parser = new GnuParser();

options.addOption("h", "help", false, "print this message");
options.addOption("i", "input", true, "input clusteredPoints");
options.addOption("o", "output", true, "output doc cluster IDs");

// parse the command line arguments
CommandLine line = null;
try {
    line = parser.parse(options, args);
    if (line.hasOption("help")) {
        formatter.printHelp("DocIDMappingClusterIDDumper", options);
        return 0;
    }
    if (!line.hasOption("o") | !line.hasOption("i")) {
        formatter.printHelp("DocIDMappingClusterIDDumper", options);
        return -1;
    }
} catch (ParseException e) {
    formatter.printHelp("DocIDMappingClusterIDDumper", options);
}

Path inPath = new Path(line.getOptionValue("i"));
Path outPath = new Path(line.getOptionValue("o"));

// extracts the string representations from the vectors
int retVal = extract(inPath, outPath);
if (retVal != 0) {
    HadoopUtil.delete(getConf(), outPath);
    return retVal;
}

return 0;
}

public int extract(Path input, Path output) throws IOException {
JobConf job = new JobConf(getConf());
// job.setJobName(this.getClass().getName());
job.setJarByClass(this.getClass());
FileInputFormat.addInputPath(job, input);
job.setInputFormat(SequenceFileInputFormat.class);
job.setNumReduceTasks(0);
job.setMapperClass(DocIDMappingClusterIDDumper.class);
FileOutputFormat.setOutputPath(job, output);
job.setOutputFormat(TextOutputFormat.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);

RunningJob rj = JobClient.runJob(job);

if (rj.isSuccessful() == false)
    return -1;
return 0;
}

public void configure(JobConf conf) {
setConf(conf);
}

public void close() throws IOException {
}

public void map(IntWritable key, WeightedVectorWritable value,
    OutputCollector<Text, Text> output, Reporter reporter)
    throws IOException {
Vector v = value.getVector();
if (v instanceof NamedVector) {
    String name = ((NamedVector) v).getName();
    if (name != null & name.length() > 2)
        output.collect(new Text(name), new Text(key.toString()));
    else
        reporter.incrCounter("DocIDMappingClusterIDDumper", "Missing name", 1);
} else
    reporter.incrCounter("DocIDMappingClusterIDDumper", "Unnamed vector", 1);
}

}