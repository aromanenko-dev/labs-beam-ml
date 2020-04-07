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

public class ClassificationPipeline {
  private static class PrintFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> out) {
      System.out.println("JAVA, element: " + element);
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
    System.out.println("ClassificationPipelineOptions: " + options);

    Pipeline p = Pipeline.create(options);
    PCollection<String> names = p.apply(Create.of("Maria", "John", "Xavier", "Erika"));
    PCollection<KV<String, String>> genres =
        names.apply(GenreClassifier.create().withClassificationPipelineOptions(options));
    genres.apply(ParDo.of(new PrintFn()));

    p.run().waitUntilFinish();
  }
}
