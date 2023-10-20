// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsmiddleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	awsv1 "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/awstesting"
	s3v1 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testUserAgent = "user/agent"
	testLatency   = time.Millisecond
)

type testHandler struct {
	id             string
	position       HandlerPosition
	handleRequest  func(id string, r *http.Request)
	handleResponse func(id string, r *http.Response)
	start          time.Time
	end            time.Time
	requestIDs     []string
	responseIDs    []string
}

var _ RequestHandler = (*testHandler)(nil)
var _ ResponseHandler = (*testHandler)(nil)

func (t *testHandler) ID() string {
	return t.id
}

func (t *testHandler) Position() HandlerPosition {
	return t.position
}

func (t *testHandler) HandleRequest(id string, r *http.Request) {
	t.start = time.Now()
	t.requestIDs = append(t.requestIDs, id)
	if t.handleRequest != nil {
		t.handleRequest(id, r)
	}
}

func (t *testHandler) HandleResponse(id string, r *http.Response) {
	t.end = time.Now()
	t.responseIDs = append(t.responseIDs, id)
	if t.handleResponse != nil {
		t.handleResponse(id, r)
	}
}

func (t *testHandler) Latency() time.Duration {
	return t.end.Sub(t.start)
}

type recordOrder struct {
	order []string
}

func (ro *recordOrder) Handle(id string) func(string, *http.Request) {
	return func(string, *http.Request) {
		ro.order = append(ro.order, id)
	}
}

func TestHandlerPosition(t *testing.T) {
	testCases := []struct {
		position HandlerPosition
		str      string
	}{
		{position: After, str: "after"},
		{position: Before, str: "before"},
	}
	for _, testCase := range testCases {
		position := testCase.position
		got, err := position.MarshalText()
		assert.NoError(t, err)
		assert.EqualValues(t, testCase.str, got)
		assert.NoError(t, position.UnmarshalText(got))
		assert.Equal(t, position, testCase.position)
	}
}

func TestInvalidHandlerPosition(t *testing.T) {
	position := HandlerPosition(-1)
	got, err := position.MarshalText()
	assert.Error(t, err)
	assert.ErrorIs(t, err, errUnsupportedPosition)
	assert.Nil(t, got)
	err = position.UnmarshalText([]byte("HandlerPosition(-1)"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, errUnsupportedPosition)
}

func TestInvalidHandlers(t *testing.T) {
	handler := new(MockHandler)
	handler.On("ID").Return("invalid handler")
	handler.On("Position").Return(HandlerPosition(-1))
	middleware := new(MockMiddlewareExtension)
	middleware.On("Handlers").Return([]RequestHandler{handler}, []ResponseHandler{handler})
	c := newConfigurer(middleware.Handlers())
	// v1
	client := awstesting.NewClient()
	err := c.ConfigureSDKv1(&client.Handlers)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errInvalidHandler))
	assert.True(t, errors.Is(err, errUnsupportedPosition))
	// v2
	err = c.ConfigureSDKv2(&awsv2.Config{})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errInvalidHandler))
	assert.True(t, errors.Is(err, errUnsupportedPosition))
	handler.AssertNotCalled(t, "HandleRequest", mock.Anything, mock.Anything)
	handler.AssertNotCalled(t, "HandleResponse", mock.Anything, mock.Anything)
}

func TestAppendOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	testCases := map[string]struct {
		requestHandlers []*testHandler
		wantOrder       []string
	}{
		"WithBothBefore": {
			requestHandlers: []*testHandler{
				{id: "1", position: Before},
				{id: "2", position: Before},
			},
			wantOrder: []string{"2", "1"},
		},
		"WithBothAfter": {
			requestHandlers: []*testHandler{
				{id: "1", position: After},
				{id: "2", position: After},
			},
			wantOrder: []string{"1", "2"},
		},
		"WithBeforeAfter": {
			requestHandlers: []*testHandler{
				{id: "1", position: Before},
				{id: "2", position: After},
			},
			wantOrder: []string{"1", "2"},
		},
		"WithAfterBefore": {
			requestHandlers: []*testHandler{
				{id: "1", position: After},
				{id: "2", position: Before},
			},
			wantOrder: []string{"2", "1"},
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			recorder := &recordOrder{}
			var requestHandlers []RequestHandler
			for _, handler := range testCase.requestHandlers {
				handler.handleRequest = recorder.Handle(handler.id)
				requestHandlers = append(requestHandlers, handler)
			}
			handler := new(MockHandler)
			handler.On("ID").Return("mock")
			handler.On("Position").Return(After)
			handler.On("HandleRequest", mock.Anything, mock.Anything)
			handler.On("HandleResponse", mock.Anything, mock.Anything)
			requestHandlers = append(requestHandlers, handler)
			middleware := new(MockMiddlewareExtension)
			middleware.On("Handlers").Return(
				requestHandlers,
				[]ResponseHandler{handler},
			)
			c := newConfigurer(middleware.Handlers())
			// v1
			client := awstesting.NewClient(&awsv1.Config{
				Region:     awsv1.String("mock-region"),
				DisableSSL: awsv1.Bool(true),
				Endpoint:   awsv1.String(server.URL),
			})
			assert.NoError(t, c.ConfigureSDKv1(&client.Handlers))
			s3v1Client := &s3v1.S3{Client: client}
			_, err := s3v1Client.ListBuckets(&s3v1.ListBucketsInput{})
			require.NoError(t, err)
			assert.Equal(t, testCase.wantOrder, recorder.order)
			recorder.order = nil
			// v2
			cfg := awsv2.Config{Region: "us-east-1"}
			assert.NoError(t, c.ConfigureSDKv2(&cfg))
			s3v2Client := s3v2.NewFromConfig(cfg, func(options *s3v2.Options) {
				options.BaseEndpoint = awsv2.String(server.URL)
			})
			_, err = s3v2Client.ListBuckets(context.Background(), &s3v2.ListBucketsInput{})
			require.NoError(t, err)
			assert.Equal(t, testCase.wantOrder, recorder.order)
		})
	}
}

func TestConfigureSDKv1(t *testing.T) {
	middleware, recorder, server := setup(t)
	defer server.Close()
	client := awstesting.NewClient(&awsv1.Config{
		Region:     awsv1.String("mock-region"),
		DisableSSL: awsv1.Bool(true),
		Endpoint:   awsv1.String(server.URL),
		MaxRetries: awsv1.Int(0),
	})
	require.Equal(t, 3, client.Handlers.Build.Len())
	require.Equal(t, 1, client.Handlers.ValidateResponse.Len())
	assert.NoError(t, newConfigurer(middleware.Handlers()).ConfigureSDKv1(&client.Handlers))
	assert.Equal(t, 5, client.Handlers.Build.Len())
	assert.Equal(t, 2, client.Handlers.ValidateResponse.Len())
	s3Client := &s3v1.S3{Client: client}
	output, err := s3Client.ListBuckets(&s3v1.ListBucketsInput{})
	require.NoError(t, err)
	assert.NotNil(t, output)
	assert.GreaterOrEqual(t, recorder.Latency(), testLatency)
	assert.Equal(t, recorder.requestIDs, recorder.responseIDs)
}

func TestConfigureSDKv2(t *testing.T) {
	middleware, recorder, server := setup(t)
	defer server.Close()
	cfg := awsv2.Config{Region: "us-east-1", RetryMaxAttempts: 0}
	assert.NoError(t, newConfigurer(middleware.Handlers()).ConfigureSDKv2(&cfg))
	s3Client := s3v2.NewFromConfig(cfg, func(options *s3v2.Options) {
		options.BaseEndpoint = awsv2.String(server.URL)
	})
	output, err := s3Client.ListBuckets(context.Background(), &s3v2.ListBucketsInput{})
	require.NoError(t, err)
	assert.NotNil(t, output)
	assert.GreaterOrEqual(t, recorder.Latency(), testLatency)
	assert.Equal(t, recorder.requestIDs, recorder.responseIDs)
}

func userAgentHandler() RequestHandler {
	return &testHandler{
		id:       "test.UserAgent",
		position: Before,
		handleRequest: func(_ string, r *http.Request) {
			r.Header.Set("User-Agent", testUserAgent)
		},
	}
}

func setup(t *testing.T) (Middleware, *testHandler, *httptest.Server) {
	t.Helper()
	recorder := &testHandler{id: "test.Latency", position: After}
	middleware := new(MockMiddlewareExtension)
	middleware.On("Handlers").Return(
		[]RequestHandler{userAgentHandler(), recorder},
		[]ResponseHandler{recorder},
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUserAgent := r.Header.Get("User-Agent")
		assert.Contains(t, gotUserAgent, testUserAgent)
		time.Sleep(testLatency)
		w.WriteHeader(http.StatusOK)
	}))
	return middleware, recorder, server
}
