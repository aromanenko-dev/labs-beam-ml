package com.talend.labs.beam.classification;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.talend.labs.beam.classification.ClassificationPipeline.ClassificationPipelineOptions;
import java.util.Random;
import org.apache.beam.runners.core.construction.External;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transform that wraps the call to the expansion service to execute the transform in the python
 * side.
 */
public class GenreClassifier
    extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
  private static final String URN = "genreclassifier:python:v1";

  private ClassificationPipelineOptions options;

  public GenreClassifier withClassificationPipelineOptions(ClassificationPipelineOptions options) {
    this.options = options;
    return this;
  }

  public static GenreClassifier create() {
    return new GenreClassifier();
  }

  @Override
  public PCollection<KV<String, String>> expand(PCollection<String> input) {
    checkArgument(this.options != null, "You must set the ClassificationPipelineOptions");
    if (options.isUseExternal()) {
      // Apply external Python classifier
      PCollection<KV<String, String>> output =
          input.apply(
              "ExternalRandomGenreClassifier",
              External.of(URN, new byte[] {}, options.getExpansionServiceURL())
                  .<KV<String, String>>withOutputType());
      return output;
    } else {
      // Apply Java classifier
      return input.apply(ParDo.of(new RandomGenreClassifierFn()));
    }
  }

  // Java version of classifier
  private static class RandomGenreClassifierFn extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element String name, OutputReceiver<KV<String, String>> out) {
      Random rand = new Random();
      int randomClassifier = rand.nextInt(10);
      if (name.length() >= randomClassifier) {
        out.output(KV.of("Sci-Fi", name));
      } else {
        out.output(KV.of("Drama", name));
      }
    }
  }
}
