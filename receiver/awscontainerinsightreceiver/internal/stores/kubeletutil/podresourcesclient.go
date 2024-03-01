// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kubeletutil // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores/kubeletutil"

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
)

const (
	socketPath        = "/var/lib/kubelet/pod-resources/kubelet.sock"
	connectionTimeout = 10 * time.Second
)

type PodResourcesClient struct {
	delegateClient podresourcesapi.PodResourcesListerClient
	conn           *grpc.ClientConn
	logger         *zap.Logger
}

func NewPodResourcesClient(logger *zap.Logger) (*PodResourcesClient, error) {
	podResourcesClient := &PodResourcesClient{}
	podResourcesClient.logger = logger

	conn, err := podResourcesClient.connectToServer(socketPath)
	podResourcesClient.conn = conn

	logger.Info("PodResources conn state: " + conn.GetState().String())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	podResourcesClient.delegateClient = podresourcesapi.NewPodResourcesListerClient(conn)
	logger.Info("PodResources delegate: ", zap.Any("delegate client", podResourcesClient.delegateClient))

	return podResourcesClient, nil
}

func (p *PodResourcesClient) connectToServer(socket string) (*grpc.ClientConn, error) {
	_, err := os.Stat(socket)
	if err != nil {
		p.logger.Info("PodResources socket error: " + err.Error())
	}

	if os.IsNotExist(err) {
		return nil, fmt.Errorf("socket path does not exist: %s", socket)
	} else if err != nil {
		return nil, fmt.Errorf("failed to check socket path: %w", err)
	}

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
		p.logger.Info("PodResources connection error: " + err.Error())
	}
	if err != nil {
		return nil, fmt.Errorf("failure connecting to '%s': %w", socket, err)
	}

	return conn, nil
}

func (p *PodResourcesClient) ListPods() (*podresourcesapi.ListPodResourcesResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	resp, err := p.delegateClient.List(ctx, &podresourcesapi.ListPodResourcesRequest{})
	if err != nil {
		p.logger.Info("PodResources ListPods error: " + err.Error())
	}
	if err != nil {
		return nil, fmt.Errorf("failure getting pod resources: %w", err)
	}

	return resp, nil
}

func (p *PodResourcesClient) Shutdown() {
	err := p.conn.Close()
	if err != nil {
		p.logger.Info("PodResources shutdown error: " + err.Error())
	}
	if err != nil {
		return
	}
}
