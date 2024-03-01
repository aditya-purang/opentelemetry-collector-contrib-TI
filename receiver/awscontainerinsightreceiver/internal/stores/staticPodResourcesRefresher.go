package stores

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	"net"
	"os"
	"time"
)

const (
	socketPath        = "/var/lib/kubelet/pod-resources/kubelet.sock"
	connectionTimeout = 10 * time.Second
)

func StartScraping(logger *zap.Logger) {
	conn, cleanup, err := connectToServer(socketPath)
	if err != nil {
		fmt.Printf("error connecting to socket: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()

	client := podresourcesapi.NewPodResourcesListerClient(conn)
	for {
		list(&client, logger)
		time.Sleep(30 * time.Second)
	}
}

func list(client *podresourcesapi.PodResourcesListerClient, logger *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	logger.Info("static_pod_resources calling ListPodResources")
	resp, err := (*client).List(ctx, &podresourcesapi.ListPodResourcesRequest{})
	if err != nil {
		logger.Info("static_pod_resources error list resources: " + err.Error())
	} else {
		logger.Info("static_pod_resources response: " + resp.String())
	}
}

func connectToServer(socket string) (*grpc.ClientConn, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx,
		socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			d := net.Dialer{}
			return d.DialContext(ctx, "unix", addr)
		}),
	)

	if err != nil {
		return nil, func() {}, fmt.Errorf("failure connecting to %s: %v", socket, err)
	}

	return conn, func() { conn.Close() }, nil
}
