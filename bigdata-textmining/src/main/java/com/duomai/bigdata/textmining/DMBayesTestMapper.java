package com.duomai.bigdata.textmining;


import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.ComplementaryNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Map;

/**
 * Run the input through the model and see if it matches.
 * <p/>
 * The output value is the generated label, and the matched clusterid:
 */
public class DMBayesTestMapper extends Mapper<Text, VectorWritable, Text, Text> {

  private AbstractNaiveBayesClassifier classifier;
  
  //load the labels
  private Map<Integer, String> labelMap;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    Path modelPath = HadoopUtil.getSingleCachedFile(conf);
    NaiveBayesModel model = NaiveBayesModel.materialize(modelPath, conf);
    boolean isComplementary = Boolean.parseBoolean(conf.get(DocClassifier.COMPLEMENTARY));
    
    Path lableIndex = new Path(conf.get("LABEL_INDEX"));
    //labelMap = BayesUtils.readLabelIndex(conf, lableIndex);
    
    // ensure that if we are testing in complementary mode, the model has been
    // trained complementary. a complementarty model will work for standard classification
    // a standard model will not work for complementary classification
    if (isComplementary) {
      Preconditions.checkArgument((model.isComplemtary() == isComplementary),
          "Complementary mode in model is different than test mode");
    }
    
    if (isComplementary) {
      classifier = new ComplementaryNaiveBayesClassifier(model);
    } else {
      classifier = new StandardNaiveBayesClassifier(model);
    }
  }

  @Override
  protected void map(Text key, VectorWritable value, Context context) throws IOException, InterruptedException {
    Vector result = classifier.classifyFull(value.get());
    //the key is the expected value
    //select the best clusterid
    Integer bestIdx = result.maxValueIndex();
    String clusterName = labelMap.get(bestIdx);
    context.write(key, new Text(clusterName));
  }
}
