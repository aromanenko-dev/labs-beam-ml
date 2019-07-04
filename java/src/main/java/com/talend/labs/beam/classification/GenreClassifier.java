package com.talend.labs.beam.classification;

import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Transform that wraps the call to the expansion service to execute the transform in the python
 * side.
 */
public class GenreClassifier
    extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {

  public static GenreClassifier of() {
    return new GenreClassifier();
  }

  private static class RandomGenreClassifierFn extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element String name, OutputReceiver<KV<String, String>> out) {
      Random rand = new Random();
      int randomClassifier = rand.nextInt(10);
      if (name.length() >= randomClassifier) {
        out.output(KV.of("GenreA", name));
      } else {
        out.output(KV.of("GenreB", name));
      }
    }
  }

  @Override
  public PCollection<KV<String, String>> expand(PCollection<String> input) {
    // TODO Replace this code with the code that invoke the python transform
    return input.apply("RandomGenreClassifier", ParDo.of(new RandomGenreClassifierFn()));
  }
}
