package com.talend.labs.beam.transforms.python;

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import avro.shaded.com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.WireCoderSetting;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.ImmutableExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.core.construction.graph.UserStateReference;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Value;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Inspired by Flink's BeamPythonStatelessFunctionRunner and Beam's SparkExecutableStageFunction */
class InvokeDoFn extends DoFn<String, String> {
  private static final String INPUT_ID = "input";
  private static final String OUTPUT_ID = "output";
  private static final String TRANSFORM_ID = "transform";

  private static final String MAIN_INPUT_NAME = "input";
  private static final String MAIN_OUTPUT_NAME = "output";

  private static final String INPUT_CODER_ID = "input_coder";
  private static final String OUTPUT_CODER_ID = "output_coder";
  private static final String WINDOW_CODER_ID = "window_coder";

  private static final String WINDOW_STRATEGY = "windowing_strategy";

  private static ExecutableStage createExecutableStage() throws Exception {
    // TODO correct name
    String functionUrn = "com.talend.beam.python3";
    String functionBody = "bla";
    ByteString functionPayload = ByteString.copyFrom(functionBody, "UTF-8");

    RunnerApi.Components components =
        RunnerApi.Components.newBuilder()
            .putPcollections(
                INPUT_ID,
                RunnerApi.PCollection.newBuilder()
                    .setWindowingStrategyId(WINDOW_STRATEGY)
                    .setCoderId(INPUT_CODER_ID)
                    .build())
            .putPcollections(
                OUTPUT_ID,
                RunnerApi.PCollection.newBuilder()
                    .setWindowingStrategyId(WINDOW_STRATEGY)
                    .setCoderId(OUTPUT_CODER_ID)
                    .build())
            .putTransforms(
                TRANSFORM_ID,
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName(TRANSFORM_ID)
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn(functionUrn)
                            .setPayload(
                                org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString
                                    .copyFrom(getUserDefinedFunctionsProtoBytes()))
                            .build())
                    .putInputs(MAIN_INPUT_NAME, INPUT_ID)
                    .putOutputs(MAIN_OUTPUT_NAME, OUTPUT_ID)
                    .build())
            .putWindowingStrategies(
                WINDOW_STRATEGY,
                RunnerApi.WindowingStrategy.newBuilder().setWindowCoderId(WINDOW_CODER_ID).build())
            .putCoders(INPUT_CODER_ID, getInputCoderProto())
            .putCoders(OUTPUT_CODER_ID, getOutputCoderProto())
            .putCoders(
                WINDOW_CODER_ID,
                RunnerApi.Coder.newBuilder()
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn(ModelCoders.GLOBAL_WINDOW_CODER_URN)
                            .build())
                    .build())
            .build();

    // Create python environment
    //    String command = "echo";
    String command =
        //        "source /home/ismael/.virtualenvs/python3/beam-2.24.0/bin/activate; "
        //        "/home/ismael/.virtualenvs/python3/beam-2.24.0/bin/activate && "
        "/home/ismael/workspace/beam4/sdks/python/container/py38/build/target/launcher/linux_amd64/boot";
    //    run apache/beam_python3.8_sdk:2.24.0
    Map<String, String> env = Collections.emptyMap();
    Environment environment = Environments.createProcessEnvironment("", "", command, env);

    PipelineNode.PCollectionNode input =
        PipelineNode.pCollection(INPUT_ID, components.getPcollectionsOrThrow(INPUT_ID));
    List<SideInputReference> sideInputs = Collections.EMPTY_LIST;
    List<UserStateReference> userStates = Collections.EMPTY_LIST;
    List<TimerReference> timers = Collections.EMPTY_LIST;
    List<PipelineNode.PTransformNode> transforms =
        Collections.singletonList(
            PipelineNode.pTransform(TRANSFORM_ID, components.getTransformsOrThrow(TRANSFORM_ID)));
    List<PipelineNode.PCollectionNode> outputs =
        Collections.singletonList(
            PipelineNode.pCollection(OUTPUT_ID, components.getPcollectionsOrThrow(OUTPUT_ID)));
    return ImmutableExecutableStage.of(
        components,
        environment,
        input,
        sideInputs,
        userStates,
        timers,
        transforms,
        outputs,
        createValueOnlyWireCoderSetting());
  }

  private static byte[] getUserDefinedFunctionsProtoBytes() {
    //      return this.userDefinedFunctions.toByteArray();
    String code = "import apache_beam as beam" + System.lineSeparator() +
    "class _RandomGenreClassifierFn(beam.DoFn):" + System.lineSeparator() +
    "\t" + "def process(self, element):" + System.lineSeparator() +
//    "print(element)
    "\t\t" + "return element" + System.lineSeparator() +
    System.lineSeparator() +
    "class GenreClassifier(beam.PTransform):" + System.lineSeparator() +
    "\t" + "def __init__(self):" + System.lineSeparator() +
    "\t\t" + "super(GenreClassifier, self).__init__()" + System.lineSeparator() +
    "\t" + "def expand(self, p):" + System.lineSeparator() +
    "\t\t" + "return p | \"RandomGenreClassifier\" >> beam.ParDo(_RandomGenreClassifierFn())"
            ;

    return code.getBytes(StandardCharsets.UTF_8);
  }

  private static RunnerApi.Coder getInputCoderProto() {
    return RunnerApi.Coder.newBuilder()
        .setSpec(
            RunnerApi.FunctionSpec.newBuilder().setUrn(ModelCoders.STRING_UTF8_CODER_URN).build())
        .build();
  }

  private static RunnerApi.Coder getOutputCoderProto() {
    return getInputCoderProto();
  }

  private static Collection<WireCoderSetting> createValueOnlyWireCoderSetting() throws IOException {
    WindowedValue<byte[]> value = WindowedValue.valueInGlobalWindow(new byte[0]);
    Coder<? extends BoundedWindow> windowCoder = GlobalWindow.Coder.INSTANCE;
    WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder =
        WindowedValue.FullWindowedValueCoder.of(ByteArrayCoder.of(), windowCoder);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    windowedValueCoder.encode(value, baos);

    return Arrays.asList(
        RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
            .setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
            .setPayload(
                org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString.copyFrom(
                    baos.toByteArray()))
            .setInputOrOutputId(INPUT_ID)
            .build(),
        RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
            .setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
            .setPayload(
                org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString.copyFrom(
                    baos.toByteArray()))
            .setInputOrOutputId(OUTPUT_ID)
            .build());
  }

  /** The Python function execution result receiver. */
  private transient LinkedBlockingQueue<byte[]> resultBuffer;
  /** The receiver which forwards the input elements to a remote environment for processing. */
  protected transient FnDataReceiver<WindowedValue<byte[]>> mainInputReceiver;
  /** Handler for state requests. */
  // TODO Unsupported
  private final transient StateRequestHandler stateRequestHandler =
      StateRequestHandler.unsupported();
  /**
   * A bundle handler for handling input elements by forwarding them to a remote environment for
   * processing. It holds a collection of {@link FnDataReceiver}s which actually perform the data
   * forwarding work.
   *
   * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote
   * resources, and throw an exception if bundle processing has failed.
   */
  private transient RemoteBundle remoteBundle;

  @Setup
  public void setup() {
    try {
      ExecutableStage executableStage = createExecutableStage();
      System.out.println(executableStage.toString());

      // TODO
      String taskName = "taskName";
      String retrievalToken = "retrievalToken";
      PortablePipelineOptions portableOptions =
          PipelineOptionsFactory.as(PortablePipelineOptions.class);
      portableOptions.setSdkWorkerParallelism(1);
      portableOptions.setFilesToStage(Lists.newArrayList("/tmp/beamtostage/file1"));

      Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);
      // TODO hack around BEAM-XXXXX
      Struct pipelineOptions2 =
          pipelineOptions.toBuilder()
              .putFields(
                  "beam:option:save_main_session", Value.newBuilder().setBoolValue(true).build())
              .build();
      System.out.println(pipelineOptions2);
      // one operator has one Python SDK harness
      JobBundleFactory jobBundleFactory =
          DefaultJobBundleFactory.create(
              JobInfo.create(taskName, taskName, retrievalToken, pipelineOptions2));
      StageBundleFactory stageBundleFactory = jobBundleFactory.forStage(executableStage);
      // TODO this is the one who deals with metrics
      BundleProgressHandler progressHandler = BundleProgressHandler.ignored();
      // TODO this should be done per bundle?
      this.remoteBundle =
          stageBundleFactory.getBundle(
              createOutputReceiverFactory(), stateRequestHandler, progressHandler);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private OutputReceiverFactory createOutputReceiverFactory() {
    return new OutputReceiverFactory() {

      // the input value type is always byte array
      @SuppressWarnings("unchecked")
      @Override
      public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
        return input -> resultBuffer.add(input.getValue());
      }
    };
  }

  @StartBundle
  public void startBundle() {
    //    remoteBundle = stageBundleFactory.getBundle(createOutputReceiverFactory(),
    // stateRequestHandler, progressHandler);

    mainInputReceiver =
        checkNotNull(
            Iterables.getOnlyElement(remoteBundle.getInputReceivers().values()),
            "Failed to retrieve main input receiver.");
  }

  @ProcessElement
  public void processElement(@Element String record, OutputReceiver<String> outputReceiver) {
    byte[] data = record.getBytes();
    try {
      mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(data));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @FinishBundle
  public void finishBundle() {
    try {
      remoteBundle.close();
    } catch (Throwable t) {
      throw new RuntimeException("Failed to close remote bundle", t);
    } finally {
      remoteBundle = null;
    }
  }

  @Teardown
  public void teardown() {}
}
