from __future__ import absolute_import

import argparse
import logging
import signal
import sys
import random

import grpc

import apache_beam as beam
from apache_beam.pipeline import PipelineOptions
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.runners.portability import expansion_service
from apache_beam.transforms import ptransform
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor

from random import randint

_LOGGER = logging.getLogger(__name__)


class _RandomGenreClassifierFn(beam.DoFn):
    def process(self, element):
        if len(element) % 2:
            return [("Sci-Fi", element)]
        else:
            return [("Drama", element)]

@ptransform.PTransform.register_urn('genreclassifier:python:v1', None)
class GenreClassifier(ptransform.PTransform):
    def __init__(self):
        super(GenreClassifier, self).__init__()

    def expand(self, pcoll):
        return pcoll | "RandomGenreClassifier" >> beam.ParDo(_RandomGenreClassifierFn())

    def to_runner_api_parameter(self, unused_context):
        return 'genreclassifier:python:v1', None

    @staticmethod
    def from_runner_api_parameter(unused_ptransform, unused_parameter, unused_context):
        return GenreClassifier()


server = None


def cleanup(unused_signum, unused_frame):
    _LOGGER.info('Shutting down expansion service.')
    server.stop(None)

def main(unused_argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--port', type=int, help='port on which to serve the job api')
    options = parser.parse_args()
    global server
    server = grpc.server(UnboundedThreadPoolExecutor())

    # DOCKER SDK Harness
    beam_expansion_api_pb2_grpc.add_ExpansionServiceServicer_to_server(
        expansion_service.ExpansionServiceServicer(
            PipelineOptions(
                ["--experiments", "beam_fn_api",
                 "--sdk_location", "container"])),
        server)

    # PROCESS SDK Harness
    # beam_expansion_api_pb2_grpc.add_ExpansionServiceServicer_to_server(
    #     expansion_service.ExpansionServiceServicer(
    #         PipelineOptions.from_dictionary({
    #             'environment_type': 'PROCESS',
    #             'environment_config': '{"command": "sdks/python/container/build/target/launcher/darwin_amd64/boot"}',
    #             'experiments': 'beam_fn_api',
    #             'sdk_location': 'container',
    #         })
    #     ), server
    # )

    server.add_insecure_port('localhost:{}'.format(options.port))
    server.start()
    _LOGGER.info('Listening for expansion requests at %d', options.port)

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    # blocking main thread forever.
    signal.pause()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main(sys.argv)
