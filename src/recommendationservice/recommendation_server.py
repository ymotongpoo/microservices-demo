#!/usr/bin/python
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import grpc
from concurrent import futures
import time
import traceback
import random
import os
import logging
import googleclouddebugger

import demo_pb2
import demo_pb2_grpc
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from logger import JSONStreamHandler

log = logging.getLogger('recommendationservice')
log.setLevel(logging.INFO)
log.addHandler(JSONStreamHandler())

# TODO(morganmclean,ahmetb) tracing currently disabled due to memory leak (see TODO below)
# from opencensus.trace.ext.grpc import server_interceptor
# from opencensus.trace.samplers import always_on
# from opencensus.trace.exporters import stackdriver_exporter
# from opencensus.trace.exporters import print_exporter

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
        max_responses = 5
        # fetch list of products from product catalog stub
        cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty())
        product_ids = [x.id for x in cat_response.products]
        filtered_products = list(set(product_ids)-set(request.product_ids))
        num_products = len(filtered_products)
        num_return = min(max_responses, num_products)
        # sample list of indicies to return
        indices = random.sample(range(num_products), num_return)
        # fetch product ids from indices
        prod_list = [filtered_products[i] for i in indices]
        log.info("[Recv ListRecommendations] product_ids={}".format(prod_list))
        # build and return response
        response = demo_pb2.ListRecommendationsResponse()
        response.product_ids.extend(prod_list)
        return response

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)


if __name__ == "__main__":
    log.info("initializing recommendationservice")

    # TODO(morganmclean,ahmetb) enabling the tracing interceptor/sampler below
    # causes an unbounded memory leak eventually OOMing the container.
    # ----
    # try:
    #     sampler = always_on.AlwaysOnSampler()
    #     exporter = stackdriver_exporter.StackdriverExporter()
    #     tracer_interceptor = server_interceptor.OpenCensusServerInterceptor(sampler, exporter)
    # except:
    #     tracer_interceptor = server_interceptor.OpenCensusServerInterceptor()

    try:
        googleclouddebugger.enable(
            module='recommendationserver',
            version='1.0.0'
        )
    except Exception, err:
        log.error("could not enable debugger")
        log.error(traceback.print_exc())
        pass

    port = os.environ.get('PORT', "8080")
    catalog_addr = os.environ.get('PRODUCT_CATALOG_SERVICE_ADDR', '')
    if catalog_addr == "":
        raise Exception('PRODUCT_CATALOG_SERVICE_ADDR environment variable not set')
    log.info("product catalog address: " + catalog_addr)
    channel = grpc.insecure_channel(catalog_addr)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(channel)

    # create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) # ,interceptors=(tracer_interceptor,))

    # add class to gRPC server
    service = RecommendationService()
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(service, server)
    health_pb2_grpc.add_HealthServicer_to_server(service, server)

    # start server
    log.info("listening on port: " + port)
    server.add_insecure_port('[::]:'+port)
    server.start()

    # keep alive
    try:
         while True:
            time.sleep(10000)
    except KeyboardInterrupt:
            server.stop(0)
