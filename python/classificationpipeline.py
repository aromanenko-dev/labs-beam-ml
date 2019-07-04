import apache_beam as beam
import genreclassifier as genreclassifier
import random

from apache_beam.options.pipeline_options import PipelineOptions


class PrintFn(beam.DoFn):
    def process(self, element):
        print(element)


if __name__ == "__main__":
    p = beam.Pipeline(options=PipelineOptions())
    # names = p | 'ReadMyFile' >> beam.io.ReadFromText('gs://some/inputData.txt')
    names = p | beam.Create(["Maria", "John", "Xavier", "Erika"])

    genres = names | genreclassifier.GenreClassifier()
    genres | beam.ParDo(PrintFn())

    p.run()
