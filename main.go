package main

import (
	"fmt"
	"github.com/clawio/grpcxlog"
	pb "github.com/clawio/service.localstore.prop/proto"
	"github.com/rs/xlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"net"
	"os"
	"strconv"
)

var log xlog.Logger

const (
	serviceID         = "CLAWIO_LOCALSTOREPROP"
	dsnEnvar          = serviceID + "_DSN"
	portEnvar         = serviceID + "_PORT"
	sharedSecretEnvar = "CLAWIO_SHAREDSECRET"
)

type environ struct {
	dsn          string
	port         int
	sharedSecret string
}

func getEnviron() (*environ, error) {
	e := &environ{}
	e.dsn = os.Getenv(dsnEnvar)
	port, err := strconv.Atoi(os.Getenv(portEnvar))
	if err != nil {
		return nil, err
	}
	e.port = port
	e.sharedSecret = os.Getenv(sharedSecretEnvar)
	return e, nil
}
func printEnviron(e *environ) {
	log.Infof("%s=%s", dsnEnvar, e.dsn)
	log.Infof("%s=%d", portEnvar, e.port)
	log.Infof("%s=%s", sharedSecretEnvar, "******")
}

func setupLog() {

	host, _ := os.Hostname()
	conf := xlog.Config{
		// Log info level and higher
		Level: xlog.LevelDebug,
		// Set some global env fields
		Fields: xlog.F{
			"svc":  serviceID,
			"host": host,
		},
		// Output everything on console
		Output: xlog.NewOutputChannel(xlog.NewConsoleOutput()),
	}

	log = xlog.New(conf)

	// Plug the xlog handler's input to Go's default logger
	grpclog.SetLogger(grpcxlog.Log{log})

}

func main() {

	setupLog()

	log.Infof("Service %s started", serviceID)

	env, err := getEnviron()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	printEnviron(env)

	p := &newServerParams{}
	p.dsn = env.dsn
	p.sharedSecret = env.sharedSecret

	srv, err := newServer(p)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", env.port))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPropServer(grpcServer, srv)
	grpcServer.Serve(lis)
}
