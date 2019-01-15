// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/profiler"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/shippingservice/genproto"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultPort = "50051"

	projectID = "yoshifumi-cloud-demo/shipping"
)

var logger *logging.Logger

func main() {
	ctx := context.Background()
	client, err := logging.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create Stackdriver Logging client: %v", err)
	}
	logger = client.Logger("shipping-logger")

	go initTracing()
	go initProfiling("shippingservice", "1.0.0")

	port := defaultPort
	if value, ok := os.LookupEnv("APP_PORT"); ok {
		port = value
	}
	port = fmt.Sprintf(":%s", port)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Log(logging.Entry{
			Severity: logging.Critical,
			Payload:  fmt.Sprintf("failed to listen: %v", err),
		})
	}
	srv := grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{}))
	svc := &server{}
	pb.RegisterShippingServiceServer(srv, svc)
	healthpb.RegisterHealthServer(srv, svc)
	logger.Log(logging.Entry{
		Severity: logging.Info,
		Payload:  fmt.Sprintf("Shipping Service listening on port %s", port),
	})

	// Register reflection service on gRPC server.
	reflection.Register(srv)
	if err := srv.Serve(lis); err != nil {
		logger.Log(logging.Entry{
			Severity: logging.Critical,
			Payload:  fmt.Sprintf("failed to serve: %v", err),
		})
	}
}

// server controls RPC service responses.
type server struct{}

// Check is for health checking.
func (s *server) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// GetQuote produces a shipping quote (cost) in USD.
func (s *server) GetQuote(ctx context.Context, in *pb.GetQuoteRequest) (*pb.GetQuoteResponse, error) {
	logger.Log(logging.Entry{
		Severity: logging.Info,
		Payload:  "[GetQuote] received request",
	})
	defer logger.Log(logging.Entry{
		Severity: logging.Info,
		Payload:  "[GetQuote] completed request",
	})

	// 1. Our quote system requires the total number of items to be shipped.
	count := 0
	for _, item := range in.Items {
		count += int(item.Quantity)
	}

	// 2. Generate a quote based on the total number of items to be shipped.
	quote := CreateQuoteFromCount(count)

	// 3. Generate a response.
	return &pb.GetQuoteResponse{
		CostUsd: &pb.Money{
			CurrencyCode: "USD",
			Units:        int64(quote.Dollars),
			Nanos:        int32(quote.Cents * 10000000)},
	}, nil

}

// ShipOrder mocks that the requested items will be shipped.
// It supplies a tracking ID for notional lookup of shipment delivery status.
func (s *server) ShipOrder(ctx context.Context, in *pb.ShipOrderRequest) (*pb.ShipOrderResponse, error) {
	logger.Log(logging.Entry{
		Severity: logging.Info,
		Payload:  "[ShipOrder] received request",
	})
	defer logger.Log(logging.Entry{
		Severity: logging.Info,
		Payload:  "[ShipOrder] completed request",
	})
	// 1. Create a Tracking ID
	baseAddress := fmt.Sprintf("%s, %s, %s", in.Address.StreetAddress, in.Address.City, in.Address.State)
	id := CreateTrackingId(baseAddress)

	// 2. Generate a response.
	return &pb.ShipOrderResponse{
		TrackingId: id,
	}, nil
}

func initStats(exporter *stackdriver.Exporter) {
	view.SetReportingPeriod(60 * time.Second)
	view.RegisterExporter(exporter)
	if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
		logger.Log(logging.Entry{
			Severity: logging.Warning,
			Payload:  "Error registering default server views",
		})
	} else {
		logger.Log(logging.Entry{
			Severity: logging.Info,
			Payload:  "Registered default server views",
		})
	}
}

func initTracing() {
	// TODO(ahmetb) this method is duplicated in other microservices using Go
	// since they are not sharing packages.
	for i := 1; i <= 3; i++ {
		exporter, err := stackdriver.NewExporter(stackdriver.Options{})
		if err != nil {
			logger.Log(logging.Entry{
				Severity: logging.Warning,
				Payload:  fmt.Sprintf("failed to initialize stackdriver exporter: %+v", err),
			})
		} else {
			trace.RegisterExporter(exporter)
			trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
			logger.Log(logging.Entry{
				Severity: logging.Info,
				Payload:  "registered stackdriver tracing",
			})

			// Register the views to collect server stats.
			initStats(exporter)
			return
		}
		d := time.Second * 10 * time.Duration(i)
		logger.Log(logging.Entry{
			Severity: logging.Info,
			Payload:  fmt.Sprintf("sleeping %v to retry initializing stackdriver exporter", d),
		})
		time.Sleep(d)
	}
	logger.Log(logging.Entry{
		Severity: logging.Warning,
		Payload:  "could not initialize stackdriver exporter after retrying, giving up",
	})
}

func initProfiling(service, version string) {
	// TODO(ahmetb) this method is duplicated in other microservices using Go
	// since they are not sharing packages.
	for i := 1; i <= 3; i++ {
		if err := profiler.Start(profiler.Config{
			Service:        service,
			ServiceVersion: version,
			// ProjectID must be set if not running on GCP.
			// ProjectID: "my-project",
		}); err != nil {
			logger.Log(logging.Entry{
				Severity: logging.Warning,
				Payload:  fmt.Sprintf("failed to start profiler: %+v", err),
			})
		} else {
			logger.Log(logging.Entry{
				Severity: logging.Info,
				Payload:  "started stackdriver profiler",
			})
			return
		}
		d := time.Second * 10 * time.Duration(i)
		logger.Log(logging.Entry{
			Severity: logging.Info,
			Payload:  fmt.Sprintf("sleeping %v to retry initializing stackdriver profiler", d),
		})
		time.Sleep(d)
	}
	logger.Log(logging.Entry{
		Severity: logging.Warning,
		Payload:  "could not initialize stackdriver profiler after retrying, giving up",
	})
}
