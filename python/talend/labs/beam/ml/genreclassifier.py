import apache_beam as beam
import random


class _RandomGenreClassifierFn(beam.DoFn):
    def process(self, element):
        random_classifier = random.randint(0, 10)
        if len(element) >= random_classifier:
            return ("GenreA", element)
        else:
            return ("GenreB", element)


class GenreClassifier(beam.PTransform):
    def __init__(self):
        super(GenreClassifier, self).__init__()

    def expand(self, p):
        return p | "RandomGenreClassifier" >> beam.ParDo(_RandomGenreClassifierFn())
