import apache_beam as beam
import random

from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())
# names = p | 'ReadMyFile' >> beam.io.ReadFromText('gs://some/inputData.txt')
names = p | beam.Create(["Maria", "John", "Xavier", "Erika"])

class RandomGenreClassifierFn(beam.DoFn):
    def process(self, element):
        random_classifier = random.randint(0, 10)
        if len(element) >= random_classifier:
            return ("GenreA", element)
        else:
            return ("GenreB", element)


class PrintFn(beam.DoFn):
    def process(self, element):
        print(element)

genres = names | beam.ParDo(RandomGenreClassifierFn())
genres | beam.ParDo(PrintFn())

p.run()
