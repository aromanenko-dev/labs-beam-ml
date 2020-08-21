package com.talend.labs.beam.classification;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassificationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(GenreClassifier.class);

  private static class PrintFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> out) {
      System.out.println("JAVA OUTPUT: " + element);
      out.output(element);
    }
  }

  /** Specific pipeline options. */
  public interface ClassificationPipelineOptions extends PipelineOptions {
    @Description("Expansion Service URL")
    @Default.String("localhost:8097")
    String getExpansionServiceURL();

    void setExpansionServiceURL(String url);

    @Description("External Translation")
    boolean isUseExternal();

    void setUseExternal(boolean isUseExternal);
  }

  public static void main(String[] args) {
    ClassificationPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(ClassificationPipelineOptions.class);
    LOG.info("PipelineOptions: " + options);

    // Create pipeline
    Pipeline p = Pipeline.create(options);

    // Simple input dataset
    PCollection<String> movies =
        p.apply(Create.of("Matrix", "Star Wars", "Forrest Gump", "Terminator"));

    // Classifier PTransform
    PCollection<KV<String, String>> genres =
        movies.apply(GenreClassifier.create().withClassificationPipelineOptions(options));

    // Output results
    genres.apply(ParDo.of(new PrintFn()));

    // Run pipeline
    p.run().waitUntilFinish();
  }
}
