package main

import (
	"fmt"
	pb "github.com/clawio/service-localfs-prop/proto/propagator"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"os"
	"runtime"
	"strconv"
)

const (
	serviceID              = "CLAWIO_LOCALFS_PROP"
	dsnEnvar               = serviceID + "_DSN"
	portEnvar              = serviceID + "_PORT"
	maxSqlIdleEnvar        = serviceID + "_MAXSQLIDLE"
	maxSqlConcurrencyEnvar = serviceID + "_MAXSQLCONCURRENCY"
	sharedSecretEnvar      = "CLAWIO_SHAREDSECRET"
)

type environ struct {
	dsn               string
	port              int
	maxSqlIdle        int
	maxSqlConcurrency int
	sharedSecret      string
}

func getEnviron() (*environ, error) {
	e := &environ{}
	e.dsn = os.Getenv(dsnEnvar)
	port, err := strconv.Atoi(os.Getenv(portEnvar))
	if err != nil {
		return nil, err
	}
	e.port = port

	maxSqlIdle, err := strconv.Atoi(os.Getenv(maxSqlIdleEnvar))
	if err != nil {
		return nil, err
	}
	e.maxSqlIdle = maxSqlIdle

	maxSqlConcurrency, err := strconv.Atoi(os.Getenv(maxSqlConcurrencyEnvar))
	if err != nil {
		return nil, err
	}
	e.maxSqlConcurrency = maxSqlConcurrency

	e.sharedSecret = os.Getenv(sharedSecretEnvar)
	return e, nil
}
func printEnviron(e *environ) {
	log.Infof("%s=%s", dsnEnvar, e.dsn)
	log.Infof("%s=%d", portEnvar, e.port)
	log.Infof("%s=%d", maxSqlIdleEnvar, e.maxSqlIdle)
	log.Infof("%s=%d", maxSqlConcurrencyEnvar, e.maxSqlConcurrency)
	log.Infof("%s=%d", portEnvar, e.port)
	log.Infof("%s=%s", sharedSecretEnvar, "******")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Infof("Service %s started", serviceID)

	env, err := getEnviron()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	printEnviron(env)

	p := &newServerParams{}
	p.dsn = env.dsn
	p.sharedSecret = env.sharedSecret
	p.maxSqlIdle = env.maxSqlIdle
	p.maxSqlConcurrency = env.maxSqlConcurrency

	srv, err := newServer(p)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", env.port))
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPropServer(grpcServer, srv)
	grpcServer.Serve(lis)
}
