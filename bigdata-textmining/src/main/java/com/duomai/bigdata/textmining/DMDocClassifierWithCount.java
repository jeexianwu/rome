package com.duomai.bigdata.textmining;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Using the model trained by CNB to classify a document to a belonging cluster 
 * 
 */
public class DMDocClassifierWithCount extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(DMDocClassifierWithCount.class);

  public static final String COMPLEMENTARY = "class"; //b for bayes, c for complementary
  //private static final Pattern SLASH = Pattern.compile("/");

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new DMDocClassifierWithCount(), args);
  }

  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(addOption(DefaultOptionCreator.overwriteOption().create()));
    addOption("model", "m", "The path to the model built during training", true);
    addOption(buildOption("testComplementary", "c", "test complementary?", false, false, String.valueOf(false)));
    addOption(buildOption("runSequential", "seq", "run sequential?", false, false, String.valueOf(false)));
    addOption("labelIndex", "l", "The path to the location of the label index", true);
    Map<String, List<String>> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      HadoopUtil.delete(getConf(), getOutputPath());
    }
    
    boolean complementary = hasOption("testComplementary");
    //boolean sequential = hasOption("runSequential");
    
    //load the labels
    //Map<Integer, String> labelMap = BayesUtils.readLabelIndex(getConf(), new Path(getOption("labelIndex")));
    
    getConf().set("LABEL_INDEX", getOption("labelIndex"));
    getConf().set("model", getOption("model"));
    getConf().set("class", String.valueOf(complementary));
    
    
    log.info("Select the mapreduce approach,Starting");
    
    @SuppressWarnings("deprecation")
	Job classifierJob = new Job(getConf());
    classifierJob.setJobName("Classify a doc by using cbayesian model, the input:"
            + getInputPath());
    classifierJob.setJarByClass(DMDocClassifierWithCount.class);
	
    classifierJob.setInputFormatClass(SequenceFileInputFormat.class);
    classifierJob.setOutputFormatClass(TextOutputFormat.class);
	
	//set Map&Reduce
    classifierJob.setMapperClass(DMDocClassifierWithCountMapper.class);
    classifierJob.setReducerClass(DMDocClassifierWithCountReducer.class);
	
    classifierJob.setOutputKeyClass(NullWritable.class);
    classifierJob.setOutputValueClass(Text.class);
    classifierJob.setMapOutputKeyClass(Text.class);
    classifierJob.setMapOutputValueClass(IntWritable.class);
	
	FileInputFormat.setInputPaths(classifierJob, getInputPath());
	FileOutputFormat.setOutputPath(classifierJob, getOutputPath());
	
	if (classifierJob.waitForCompletion(true)) {
	    return 0;
	}
	return -1;
    
  }


}
