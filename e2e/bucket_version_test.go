package e2e

import (
	"testing"

	"gotest.tools/v3/icmd"
)

func TestBucketVersioning(t *testing.T) {
	skipTestIfGCS(t, "versioning is not supported in GCS")

	t.Parallel()

	bucket := s3BucketFromTestName(t)
	s3client, s5cmd := setup(t, withS3Backend("mem"))

	createBucket(t, s3client, bucket)

	// check that when bucket is created, it is unversioned
	cmd := s5cmd("bucket-version", "s3://"+bucket)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals("%q is an unversioned bucket", bucket),
	})

	testcases := []struct {
		name                     string
		versioningStatus         string
		expectedVersioningStatus string
	}{
		{
			name:                     "Enable Bucket Versioning",
			versioningStatus:         "Enabled",
			expectedVersioningStatus: "Enabled",
		},
		{
			name:                     "Suspend Bucket Versioning",
			versioningStatus:         "Suspended",
			expectedVersioningStatus: "Suspended",
		},
		{
			name:                     "Enable Bucket Versioning Case Insensitive",
			versioningStatus:         "eNaBlEd",
			expectedVersioningStatus: "Enabled",
		},
		{
			name:                     "Suspend Bucket Versioning Case Insensitive",
			versioningStatus:         "sUsPenDeD",
			expectedVersioningStatus: "Suspended",
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
				0: equals("Bucket versioning for %q is set to %q", bucket, tc.expectedVersioningStatus),
			})

			cmd = s5cmd("bucket-version", "s3://"+bucket)
			result = icmd.RunCmd(cmd)

			result.Assert(t, icmd.Success)

			assertLines(t, result.Stdout(), map[int]compareFunc{
				0: equals("Bucket versioning for %q is %q", bucket, tc.expectedVersioningStatus),
			})

		})
	}
}
