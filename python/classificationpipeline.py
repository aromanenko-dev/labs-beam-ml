import apache_beam as beam
import talend.labs.beam.ml.genreclassifier as genreclassifier
import random
import sys

from apache_beam.options.pipeline_options import PipelineOptions


class _PrintFn(beam.DoFn):
    def process(self, element):
        print(element)
        return element


if __name__ == "__main__":
    options = PipelineOptions(flags=sys.argv)
    p = beam.Pipeline(options=options)

    names = p | beam.Create(["Maria", "John", "Xavier", "Erika"])
    genres = names | genreclassifier.GenreClassifier()
    genres | beam.ParDo(_PrintFn())

    p.run().wait_until_finish()

