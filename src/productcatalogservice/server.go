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
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
        "log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/profiler"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"github.com/golang/protobuf/jsonpb"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const projectID = "yoshifumi-cloud-demo/productcatalog"

var (
	cat          pb.ListProductsResponse
	catalogMutex *sync.Mutex
	logger       *logging.Logger

	port = flag.Int("port", 3550, "port to listen at")

	reloadCatalog bool
)

func main() {
	ctx := context.Background()
	client, err := logging.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create Stackdriver Logging client: %v", err)
	}
	logger = client.Logger("productcatalog-logger")
	catalogMutex = &sync.Mutex{}
	err = readCatalogFile(&cat)
	if err != nil {
		logger.Log(logging.Entry{
			Severity: logging.Warning,
			Payload:  "could not parse product catalog",
		})
	}

	go initTracing()
	go initProfiling("productcatalogservice", "1.0.0")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for {
			sig := <-sigs
			logger.Log(logging.Entry{
				Severity: logging.Info,
				Payload:  fmt.Sprintf("Received signal: %s", sig),
			})
			if sig == syscall.SIGUSR1 {
				reloadCatalog = true
				logger.Log(logging.Entry{
					Severity: logging.Info,
					Payload:  "Enable catalog reloading",
				})
			} else {
				reloadCatalog = false
				logger.Log(logging.Entry{
					Severity: logging.Info,
					Payload:  "Disable catalog reloading",
				})
			}
		}
	}()

	logger.Log(logging.Entry{
		Severity: logging.Info,
		Payload:  fmt.Sprintf("starting grpc server at :%d", *port),
	})
	run(*port)
	select {}
}

func run(port int) string {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Log(logging.Entry{
			Severity: logging.Critical,
			Payload:  err.Error(),
		})
	}
	srv := grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{}))
	svc := &productCatalog{}
	pb.RegisterProductCatalogServiceServer(srv, svc)
	healthpb.RegisterHealthServer(srv, svc)
	go srv.Serve(l)
	return l.Addr().String()
}

func initStats(exporter *stackdriver.Exporter) {
	view.SetReportingPeriod(60 * time.Second)
	view.RegisterExporter(exporter)
	if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
		logger.Log(logging.Entry{
			Severity: logging.Info,
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
	}
	logger.Log(logging.Entry{
		Severity: logging.Warning,
		Payload:  "could not initialize stackdriver profiler after retrying, giving up",
	})
}

type productCatalog struct{}

func readCatalogFile(catalog *pb.ListProductsResponse) error {
	catalogMutex.Lock()
	defer catalogMutex.Unlock()
	catalogJSON, err := ioutil.ReadFile("products.json")
	if err != nil {
		logger.Log(logging.Entry{
			Severity: logging.Critical,
			Payload:  fmt.Sprintf("failed to open product catalog json file: %v", err),
		})
		return err
	}
	if err := jsonpb.Unmarshal(bytes.NewReader(catalogJSON), catalog); err != nil {
		logger.Log(logging.Entry{
			Severity: logging.Warning,
			Payload:  fmt.Sprintf("failed to parse the catalog JSON: %v", err),
		})
		return err
	}
	logger.Log(logging.Entry{
		Severity: logging.Info,
		Payload:  "successfully parsed product catalog json",
	})
	return nil
}

func parseCatalog() []*pb.Product {
	if reloadCatalog || len(cat.Products) == 0 {
		err := readCatalogFile(&cat)
		if err != nil {
			return []*pb.Product{}
		}
	}
	return cat.Products
}

func (p *productCatalog) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (p *productCatalog) ListProducts(context.Context, *pb.Empty) (*pb.ListProductsResponse, error) {
	return &pb.ListProductsResponse{Products: parseCatalog()}, nil
}

func (p *productCatalog) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	var found *pb.Product
	for i := 0; i < len(parseCatalog()); i++ {
		if req.Id == parseCatalog()[i].Id {
			found = parseCatalog()[i]
		}
	}
	if found == nil {
		return nil, status.Errorf(codes.NotFound, "no product with ID %s", req.Id)
	}
	return found, nil
}

func (p *productCatalog) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	// Intepret query as a substring match in name or description.
	var ps []*pb.Product
	for _, p := range parseCatalog() {
		if strings.Contains(strings.ToLower(p.Name), strings.ToLower(req.Query)) ||
			strings.Contains(strings.ToLower(p.Description), strings.ToLower(req.Query)) {
			ps = append(ps, p)
		}
	}
	return &pb.SearchProductsResponse{Results: ps}, nil
}
