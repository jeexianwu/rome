package com.duomai.bigdata.textmining;

import com.google.common.io.Closeables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.common.lucene.IteratorTokenStream;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

/**
 * Converts a document in to a sparse vector
 */
public class DMTFPartialVectorReducer extends Reducer<Text, StringTuple, Text, VectorWritable> {
	
	private static final Logger log = LoggerFactory.getLogger(DMTFPartialVectorReducer.class);

  private final OpenObjectIntHashMap<String> dictionary = new OpenObjectIntHashMap<String>();

  private int dimension;
  private boolean sequentialAccess;
  private boolean namedVector;
  private int maxNGramSize = 1;

  @Override
  protected void reduce(Text key, Iterable<StringTuple> values, Context context)
    throws IOException, InterruptedException {
    Iterator<StringTuple> it = values.iterator();
    if (!it.hasNext()) {
      return;
    }
    StringTuple value = it.next();

    Vector vector = new RandomAccessSparseVector(dimension, value.length()); // guess at initial size

    if (maxNGramSize >= 2) {
      ShingleFilter sf = new ShingleFilter(new IteratorTokenStream(value.getEntries().iterator()), maxNGramSize);
      sf.reset();
      try {
        do {
          String term = sf.getAttribute(CharTermAttribute.class).toString();
          log.info(term);
          if (!term.isEmpty() && dictionary.containsKey(term)) { // ngram
            int termId = dictionary.get(term);
            vector.setQuick(termId, vector.getQuick(termId) + 1);
          }
        } while (sf.incrementToken());

        sf.end();
      } finally {
        Closeables.close(sf, true);
      }
    } else {
      for (String term : value.getEntries()) {
        if (!term.isEmpty() && dictionary.containsKey(term)) { // unigram
          int termId = dictionary.get(term);
          vector.setQuick(termId, vector.getQuick(termId) + 1);
        }
      }
    }
    if (sequentialAccess) {
      vector = new SequentialAccessSparseVector(vector);
    }

    if (namedVector) {
      vector = new NamedVector(vector, key.toString());
    }

    // if the vector has no nonZero entries (nothing in the dictionary), let's not waste space sending it to disk.
    if (vector.getNumNondefaultElements() > 0) {
      VectorWritable vectorWritable = new VectorWritable(vector);
      context.write(key, vectorWritable);
    } else {
      context.getCounter("DMTFPartialVectorReducer", "emptyVectorCount").increment(1);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();

    dimension = conf.getInt(PartialVectorMerger.DIMENSION, Integer.MAX_VALUE);
    sequentialAccess = conf.getBoolean(PartialVectorMerger.SEQUENTIAL_ACCESS, false);
    namedVector = conf.getBoolean(PartialVectorMerger.NAMED_VECTOR, false);
    maxNGramSize = conf.getInt(DictionaryVectorizer.MAX_NGRAMS, maxNGramSize);

    URI[] localFiles = DistributedCache.getCacheFiles(conf);
    Path dictionaryFile = HadoopUtil.findInCacheByPartOfFilename(DictionaryVectorizer.DICTIONARY_FILE, localFiles);
    log.info(dictionaryFile.toUri().toString());
    // key is word value is id
    for (Pair<Writable, IntWritable> record
            : new SequenceFileIterable<Writable, IntWritable>(dictionaryFile, true, conf)) {
      dictionary.put(record.getFirst().toString(), record.getSecond().get());
      log.info(record.getFirst().toString()+"---"+record.getSecond().get());
    }
  }

}
