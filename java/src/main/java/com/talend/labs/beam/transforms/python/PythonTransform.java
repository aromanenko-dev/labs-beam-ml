package com.talend.labs.beam.transforms.python;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class PythonTransform extends PTransform<PCollection<String>, PCollection<String>> {

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    return input.apply(ParDo.of(new InvokeDoFn()));
  }

  private PythonTransform() {}

  public static PythonTransform of() { return new PythonTransform(); }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> names =
        p.apply(Create.of("Maria")).apply(new PythonTransform()); //, "John", "Xavier", "Erika"
    p.run().waitUntilFinish();
    //    InvokeDoFn invokeDoFn = new InvokeDoFn();
    //    invokeDoFn.setup();
    //    invokeDoFn.processElement("test", null);
  }

  private static class PrintFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> out) {
      System.out.println("JAVA OUTPUT: " + element);
      out.output(element);
    }
  }
}
