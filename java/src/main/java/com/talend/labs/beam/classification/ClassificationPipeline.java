package com.talend.labs.beam.classification;

import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ClassificationPipeline {

  /**
   * Transform that wraps the call to the expansion service to execute the transform in the python
   * side.
   */
  private static class GenreClassifierTransform
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> input) {
      // TODO Replace this code with the code that invoke the python transform
      return input.apply(
          ParDo.of(
              new DoFn<String, KV<String, String>>() {
                @ProcessElement
                public void processElement(
                    @Element String name, OutputReceiver<KV<String, String>> out) {
                  Random rand = new Random();
                  int randomClassifier = rand.nextInt(10);
                  if (name.length() >= randomClassifier) {
                    out.output(KV.of("GenreA", name));
                  } else {
                    out.output(KV.of("GenreB", name));
                  }
                }
              }));
    }
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    PCollection<String> names = p.apply(Create.of("Maria", "John", "Xavier", "Erika"));
    PCollection<KV<String, String>> genres = names.apply(new GenreClassifierTransform());
    genres.apply(
        ParDo.of(
            new DoFn<KV<String, String>, KV<String, String>>() {
              @ProcessElement
              public void processElement(@Element KV<String, String> genreTuple, OutputReceiver<KV<String, String>> out) {
                System.out.println(genreTuple);
                out.output(genreTuple);
              }
            }));

    p.run();
  }
}
