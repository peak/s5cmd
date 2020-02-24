package core

import (
	"path/filepath"
	"testing"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
)

func TestParseUnchecked(t *testing.T) {
	t.Run("opt.Unchecked", func(t *testing.T) {
		input := "testStr"
		assertParse(t, opt.Unchecked, input, input, false, false, "", "", nil)
	})
	t.Run("opt.UncheckedOneOrMore", func(t *testing.T) {
		input := "testStr1"
		assertParse(t, opt.UncheckedOneOrMore, input, input, false, false, "", "", nil)
	})
	t.Run("opt.UncheckedOneOrMore", func(t *testing.T) {
		input := "testStr1 testStr2"
		assertParse(t, opt.UncheckedOneOrMore, input, input, false, false, "", "", nil)
	})
}

func TestParseS3Obj(t *testing.T) {
	typ := opt.S3Obj
	t.Run("path/to/obj: valid key", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"
		input := "s3://" + inputBucket + "/" + inputKey

		assertParse(t, typ, input, input, false, true, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj/: s3 key should not end with slash", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"
		input := "s3://" + inputBucket + "/" + inputKey
		expectError := true

		assertParse(t, typ, input, "", expectError, true, "", "", nil)
	})
	t.Run("path/to/obj*: s3 key cannot contain glob character", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"
		input := "s3://" + inputBucket + "/" + inputKey
		expectError := true

		assertParse(t, typ, input, "", expectError, true, "", "", nil)
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		expectError := true

		assertParse(t, typ, inputKey, "", expectError, false, "", "", nil)
	})
}

func TestParseS3Dir(t *testing.T) {
	typ := opt.S3Dir
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"
		input := "s3://" + inputBucket + "/" + inputKey

		assertParse(t, typ, input, input, false, true, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj: s3 dir should end with slash", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"
		input := "s3://" + inputBucket + "/" + inputKey
		expectError := true

		assertParse(t, typ, input, "", expectError, true, "", "", nil)
	})
	t.Run("path/to/obj*: s3 dir should end with slash", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"
		input := "s3://" + inputBucket + "/" + inputKey
		expectError := true

		assertParse(t, typ, input, "", expectError, true, "", "", nil)
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		expectError := true

		assertParse(t, typ, inputKey, "", expectError, true, "", "", nil)
	})
	t.Run("missing-key", func(t *testing.T) {
		inputBucket := "bucket"
		input := "s3://" + inputBucket

		assertParse(t, typ, input, input, false, true, inputBucket, "", nil)
	})
}

func TestParseS3WildObj(t *testing.T) {
	typ := opt.S3WildObj
	t.Run("path/to/wild/*obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/wild/*obj"
		input := "s3://" + inputBucket + "/" + inputKey

		assertParse(t, typ, input, input, false, true, inputBucket, inputKey, nil)
	})
	t.Run("path/to/wild/*obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/wild/*obj/"
		input := "s3://" + inputBucket + "/" + inputKey

		assertParse(t, typ, input, input, false, true, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj: s3 key should contain glob characters", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"
		input := "s3://" + inputBucket + "/" + inputKey
		expectError := true

		assertParse(t, typ, input, "", expectError, false, "", "", nil)
	})
	t.Run("path/to/obj/: s3 key should contain glob characters", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"
		input := "s3://" + inputBucket + "/" + inputKey
		expectError := true

		assertParse(t, typ, input, "", expectError, true, "", "", nil)
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		assertParse(t, typ, inputKey, "", true, false, "", "", nil)
	})
	t.Run("missing-key", func(t *testing.T) {
		inputBucket := "s3://bucket"
		assertParse(t, typ, inputBucket, "", true, true, "", "", nil)
	})
}

func TestParseS3ObjOrDir(t *testing.T) {
	typ := opt.S3ObjOrDir
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"
		input := "s3://" + inputBucket + "/" + inputKey

		assertParse(t, typ, input, input, false, true, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"
		input := "s3://" + inputBucket + "/" + inputKey

		assertParse(t, typ, input, input, false, true, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj+file", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"
		input := "s3://" + inputBucket + "/" + inputKey

		assertParse(t, typ, input, input, false, true, inputBucket, inputKey, newURL("file"))
	})
	t.Run("path/to/obj/+file", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"
		input := "s3://" + inputBucket + "/" + inputKey

		assertParse(t, typ, input, input+"file", false, true, inputBucket, inputKey+"file", newURL("file"))
	})
	t.Run("missing-key", func(t *testing.T) {
		inputBucket := "bucket"
		input := "s3://" + inputBucket

		assertParse(t, typ, input, input, false, true, inputBucket, "", nil)
	})
	t.Run("missing-key+file", func(t *testing.T) {
		inputBucket := "bucket"
		input := "s3://" + inputBucket

		assertParse(t, typ, input, input+"/file", false, true, inputBucket, "file", newURL("file"))
	})
	t.Run("missing-key-with-slash+file", func(t *testing.T) {
		inputBucket := "bucket"
		input := "s3://" + inputBucket + "/"

		assertParse(t, typ, input, input+"file", false, true, inputBucket, "file", newURL("file"))
	})
	t.Run("path/to/obj*: s3 key cannot contain glob characters", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"
		input := "s3://" + inputBucket + "/" + inputKey
		expectError := true

		assertParse(t, typ, input, "", expectError, true, "", "", nil)
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		assertParse(t, typ, inputKey, "", true, false, "", "", nil)
	})
}

func TestParseFileObj(t *testing.T) {
	typ := opt.FileObj
	t.Run("path/to/obj", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj")

		assertParse(t, typ, input, input, false, false, "", "", nil)
	})
	t.Run("path/to/obj/: FileObj should not end with slash", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj") + string(filepath.Separator)
		expectError := true

		assertParse(t, typ, input, "", expectError, false, "", "", nil)
	})
	t.Run("path/to/obj*: FileObj cannot contain glob characters", func(t *testing.T) {
		input := "path/to/obj*"
		expectError := true

		assertParse(t, typ, input, "", expectError, false, "", "", nil)
	})
	t.Run("s3://bucket/path: expect local file, got remote file", func(t *testing.T) {
		input := "s3://bucket/path"

		assertParse(t, typ, input, "", true, true, "", "", nil)
	})
}

func TestParseFileDir(t *testing.T) {
	typ := opt.Dir
	t.Run("path/to/obj/", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj") + string(filepath.Separator)

		assertParse(t, typ, input, input, false, false, "", "", nil)
	})
	t.Run("existing-dir", func(t *testing.T) {
		assertParse(t, typ, "vendor", "vendor/", false, false, "", "", nil)
	})
	t.Run("path/to/obj", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj")
		expectedOutput := input + string(filepath.Separator)

		assertParse(t, typ, input, expectedOutput, false, false, "", "", nil)
	})
	t.Run("path/to/obj*: Dir cannot contain glob characters", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj*")
		expectError := true

		assertParse(t, typ, input, "", expectError, false, "", "", nil)
	})
	t.Run("s3://bucket/path: expected local file, got remote", func(t *testing.T) {
		input := "s3://bucket/path"
		expectError := true

		assertParse(t, typ, input, "", expectError, true, "", "", nil)
	})
}

func TestParseFileOrDir(t *testing.T) {
	typ := opt.FileOrDir
	t.Run("path/to/obj", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj")

		assertParse(t, typ, input, input, false, false, "", "", nil)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj") + string(filepath.Separator)

		assertParse(t, typ, input, input, false, false, "", "", nil)
	})
	t.Run("Existing-dir-without-slash", func(t *testing.T) {
		input := "../vendor"
		expectedOutput := "../vendor" + string(filepath.Separator)

		assertParse(t, typ, input, expectedOutput, false, false, "", "", nil)
	})
	t.Run("path/to/obj*: FileOrDir cannot contain glob characters", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj*")

		assertParse(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("s3://bucket/path: expected local file, got remote", func(t *testing.T) {
		input := "s3://bucket/path"
		assertParse(t, typ, input, "", true, true, "", "", nil)
	})
}

func TestParseGlob(t *testing.T) {
	typ := opt.Glob
	t.Run("path/to/obj*", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj*")
		assertParse(t, typ, input, input, false, false, "", "", nil)
	})
	t.Run("path/to/obj: Glob should contain glob characters", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj")
		assertParse(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("s3://bucket/path: expected local file, got remote", func(t *testing.T) {
		input := "s3://bucket/path"
		assertParse(t, typ, input, "", true, true, "", "", nil)
	})
}

func assertParse(
	t *testing.T,
	typ opt.ParamType,
	input string,
	expectedOutArg string,
	expectError bool,
	expectRemoteURL bool,
	expectedS3bucket string,
	expectedS3key string,
	fnObj *objurl.ObjectURL,
) {
	t.Helper()

	a, err := parseArgumentByType(input, typ, fnObj)
	if expectError {
		if err == nil {
			t.Fatal("Expected err")
		}
	} else {
		if err != nil {
			t.Fatalf("Unexpected err: %v", err)
		}
	}

	if expectError {
		return // Success
	}

	if a.Absolute() != expectedOutArg {
		t.Errorf(`"Expected a.arg was "%s" but got "%s"`, expectedOutArg, a.Absolute())
	}

	if expectRemoteURL {
		if !a.IsRemote() {
			t.Fatalf("Expected remote file, got local: %q", a.Path)
		}
	} else {
		if a.IsRemote() {
			t.Fatalf("Expected local file, got: %q", a.Path)
		}
	}

	if !expectRemoteURL {
		return // Success
	}

	if a.Bucket != expectedS3bucket {
		t.Errorf(`"Expected a.s3.bucket was "%s" but got "%s"`, expectedS3bucket, a.Bucket)
	}
	if a.Path != expectedS3key {
		t.Errorf(`"Expected a.s3.key was "%s" but got "%s"`, expectedS3key, a.Path)
	}
}
