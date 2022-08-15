package e2e

import (
	"testing"

	"gotest.tools/v3/icmd"
)

func TestBucketVersioning(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)
	s3client, s5cmd, cleanup := setup(t, withS3Backend("mem"))
	defer cleanup()

	createBucket(t, s3client, bucket)

	// check that when bucket is created, it is unversioned
	cmd := s5cmd("bucket-version", "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("%q is an unversioned bucket", bucket),
	})

	testcases := []struct {
		name             string
		versioningStatus string
	}{
		{
			name:             "Enable Bucket Versioning",
			versioningStatus: "Enabled",
		},
		{
			name:             "Suspend Bucket Versioning",
			versioningStatus: "Suspended",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// set bucket versioning to and check if the change succeeded
			cmd = s5cmd("bucket-version", "--set", tc.versioningStatus, "s3://"+bucket)
			result = icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)

			assertLines(t, result.Stdout(), map[int]compareFunc{
				0: equals("Bucket versioning for %q is set to %q", bucket, tc.versioningStatus),
			})

			cmd = s5cmd("bucket-version", "s3://"+bucket)
			result = icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)

			assertLines(t, result.Stdout(), map[int]compareFunc{
				0: equals("Bucket versioning for %q is %q", bucket, tc.versioningStatus),
			})

		})
	}
}
