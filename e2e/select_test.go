// go:build external
package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gotest.tools/v3/icmd"
)

func getSequentialFile(n int, form string) (string, map[int]compareFunc) {
	sb := strings.Builder{}
	expectedLines := make(map[int]compareFunc)

	for i := 0; i < n; i++ {
		var line string
		switch form {
		case "json":
			line = fmt.Sprintf(`{ "line": "%d", "id": "i%d", data: "some event %d" }`, i, i, i)
		case "csv":
			if i == 0 {
				line = "line,id,data"
			} else {
				line = fmt.Sprintf(`%d,i%d,"some event %d"`, i, i, i)
			}
		}
		sb.WriteString(line)
		sb.WriteString("\n")

		expectedLines[i] = equals(line)
	}

	return sb.String(), expectedLines
}

func startMinio(t *testing.T, ctx context.Context) (testcontainers.Container, string, error) {
	t.Helper()
	port, err := nat.NewPort("", "9000")
	if err != nil {
		t.Errorf("error while building port\n")
		return nil, "", err
	}

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio",
		ExposedPorts: []string{string(port)},
		Cmd:          []string{"server", "/data"},
		WaitingFor:   wait.ForListeningPort(port),
	}

	minioC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}
	host, err := minioC.Host(ctx)
	if err != nil {
		return nil, "", err
	}

	mappedPort, err := minioC.MappedPort(ctx, port)
	if err != nil {
		return nil, "", err
	}
	address := fmt.Sprintf("http://%s:%d", host, mappedPort.Int())
	return minioC, address, nil
}

func TestSelectCommand(t *testing.T) {
	ctx := context.Background()
	container, address, err := startMinio(t, ctx)
	if err != nil {
		t.Fatalf("Could not start the container.Error:%v\n", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Errorf("Could not terminate the container. Error:%v\n", err)
		}
	})
	region := "us-east-1"
	accessKeyId := "minioadmin"
	secretKey := "minioadmin"

	s3client, s5cmd := setup(t, withEndpointURL(address), withRegion(region), withAccessKeyId(accessKeyId), withSecretKey(secretKey))

	createBucket(t, s3client, "bucket")
	putFile(t, s3client, "bucket", "filename.txt", "content")

	c := s5cmd("ls", "s3://bucket/")
	result := icmd.RunCmd(c, withEnv("AWS_ACCESS_KEY_ID", accessKeyId), withEnv("AWS_SECRET_ACCESS_KEY", secretKey))

	fmt.Println(result.Stdout())
}
