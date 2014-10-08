package com.duomai.bigdata.textmining;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.ComplementaryNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Using the model trained by CNB to classify a document to a belonging cluster 
 * 
 */
public class DocClassifier extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(DocClassifier.class);

  public static final String COMPLEMENTARY = "class"; //b for bayes, c for complementary
  //private static final Pattern SLASH = Pattern.compile("/");

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new DocClassifier(), args);
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
    boolean sequential = hasOption("runSequential");
    
    //load the labels
    Map<Integer, String> labelMap = BayesUtils.readLabelIndex(getConf(), new Path(getOption("labelIndex")));
    
    if (sequential) {
    	log.info("Select the sequential approach,Starting");
      FileSystem fs = FileSystem.get(getConf());
      NaiveBayesModel model = NaiveBayesModel.materialize(new Path(getOption("model")), getConf());
      AbstractNaiveBayesClassifier classifier;
      if (complementary) {
        classifier = new ComplementaryNaiveBayesClassifier(model);
      } else {
        classifier = new StandardNaiveBayesClassifier(model);
      }
      @SuppressWarnings("deprecation")
	  SequenceFile.Writer writer =
          new SequenceFile.Writer(fs, getConf(), getOutputPath(), Text.class, Text.class);
      @SuppressWarnings("deprecation")
	  Reader reader = new Reader(fs, getInputPath(), getConf());
      
      Text key = new Text();
      VectorWritable vw = new VectorWritable();
      while (reader.next(key, vw)) {
    	  Vector scoreVector = classifier.classifyFull(vw.get());
    	  Integer bestIdx = Integer.valueOf(scoreVector.maxValueIndex());
        writer.append(new Text(key.toString()),new Text(labelMap.get(bestIdx)));
      }
      writer.close();
      reader.close();
    } else {
    	log.info("Select the mapreduce approach,Starting");
      boolean succeeded = runMapReduce(parsedArgs);
      if (!succeeded) {
        return -1;
      }
    }
    
    return 0;
  }

  private boolean runMapReduce(Map<String, List<String>> parsedArgs) throws IOException,
      InterruptedException, ClassNotFoundException {
	  
    Path model = new Path(getOption("model"));
    HadoopUtil.cacheFiles(model, getConf());
    /*Path lableindex = new Path(getOption("labelIndex"));
    HadoopUtil.cacheFiles(lableindex, getConf());*/
    
    
    //the output key is the expected value, the output value are the scores for all the labels
    Job testJob = prepareJob(getInputPath(), getOutputPath(), SequenceFileInputFormat.class, DMBayesTestMapper.class,
            Text.class, Text.class, TextOutputFormat.class);
    testJob.getConfiguration().set("LABEL_INDEX", getOption("--labels"));
    //testJob.getConfiguration().set("LABELINDEX_MAP", labelMap);
    boolean complementary = parsedArgs.containsKey("testComplementary");
    testJob.getConfiguration().set(COMPLEMENTARY, String.valueOf(complementary));
    return testJob.waitForCompletion(true);
  }

 
}
