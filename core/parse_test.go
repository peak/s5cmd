package core

import (
	"path/filepath"
	"testing"

	"github.com/peak/s5cmd/opt"
)

func TestParseUnchecked(t *testing.T) {
	t.Run("opt.Unchecked", func(t *testing.T) {
		input := "testStr"
		testParseGeneral(t, opt.Unchecked, input, input, false, true, "", "", nil)
	})
	t.Run("opt.UncheckedOneOrMore", func(t *testing.T) {
		input := "testStr1"
		testParseGeneral(t, opt.UncheckedOneOrMore, input, input, false, true, "", "", nil)
	})
	t.Run("opt.UncheckedOneOrMore", func(t *testing.T) {
		input := "testStr1 testStr2"
		testParseGeneral(t, opt.UncheckedOneOrMore, input, input, false, true, "", "", nil)
	})
}

func testParseGeneral(t *testing.T, typ opt.ParamType, input, expectedOutArg string, expectError, expectNilS3 bool, expectedS3bucket, expectedS3key string, fnObj *JobArgument) {
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

	if a.arg != expectedOutArg {
		t.Errorf(`"Expected a.arg was "%s" but got "%s"`, expectedOutArg, a.arg)
	}

	if expectNilS3 {
		if a.s3 != nil {
			t.Fatal("Expected nil a.s3")
		}
	} else {
		if a.s3 == nil {
			t.Fatal("Unexpected nil a.s3")
		}
	}
	if expectNilS3 {
		return // Success
	}

	if a.s3.Bucket != expectedS3bucket {
		t.Errorf(`"Expected a.s3.bucket was "%s" but got "%s"`, expectedS3bucket, a.s3.Bucket)
	}
	if a.s3.Key != expectedS3key {
		t.Errorf(`"Expected a.s3.key was "%s" but got "%s"`, expectedS3key, a.s3.Key)
	}
}

func TestParseS3Obj(t *testing.T) {
	typ := opt.S3Obj
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		testParseGeneral(t, typ, inputKey, "", true, false, "", "", nil)
	})
}

func TestParseS3Dir(t *testing.T) {
	typ := opt.S3Dir
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		testParseGeneral(t, typ, inputKey, "", true, false, "", "", nil)
	})
	t.Run("missing-key", func(t *testing.T) {
		inputBucket := "bucket"

		input := "s3://" + inputBucket
		testParseGeneral(t, typ, input, input, false, false, inputBucket, "", nil)
	})
}

func TestParseS3WildObj(t *testing.T) {
	typ := opt.S3WildObj
	t.Run("path/to/wild/*obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/wild/*obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey, nil)
	})
	t.Run("path/to/wild/*obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/wild/*obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		testParseGeneral(t, typ, inputKey, "", true, false, "", "", nil)
	})
	t.Run("missing-key", func(t *testing.T) {
		inputBucket := "s3://bucket"
		testParseGeneral(t, typ, inputBucket, "", true, false, "", "", nil)
	})
}

func TestParseS3ObjOrDir(t *testing.T) {
	typ := opt.S3ObjOrDir
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey, nil)
	})
	t.Run("path/to/obj+file", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey, &JobArgument{arg: "file", s3: nil})
	})
	t.Run("path/to/obj/+file", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input+"file", false, false, inputBucket, inputKey+"file", &JobArgument{arg: "file", s3: nil})
	})
	t.Run("missing-key", func(t *testing.T) {
		inputBucket := "bucket"

		input := "s3://" + inputBucket
		testParseGeneral(t, typ, input, input, false, false, inputBucket, "", nil)
	})
	t.Run("missing-key+file", func(t *testing.T) {
		inputBucket := "bucket"
		input := "s3://" + inputBucket
		testParseGeneral(t, typ, input, input+"/file", false, false, inputBucket, "file", &JobArgument{arg: "file", s3: nil})
	})
	t.Run("missing-key-with-slash+file", func(t *testing.T) {
		inputBucket := "bucket"
		input := "s3://" + inputBucket + "/"
		testParseGeneral(t, typ, input, input+"file", false, false, inputBucket, "file", &JobArgument{arg: "file", s3: nil})
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "", nil)
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		testParseGeneral(t, typ, inputKey, "", true, false, "", "", nil)
	})
}

func TestParseFileObj(t *testing.T) {
	typ := opt.FileObj
	t.Run("path/to/obj", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj")
		testParseGeneral(t, typ, input, input, false, true, "", "", nil)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj") + string(filepath.Separator)
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		input := "path/to/obj*"
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
	t.Run("s3://bucket/path", func(t *testing.T) {
		input := "s3://bucket/path"
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
}

func TestParseFileDir(t *testing.T) {
	typ := opt.Dir
	t.Run("path/to/obj/", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj") + string(filepath.Separator)
		testParseGeneral(t, typ, input, input, false, true, "", "", nil)
	})
	t.Run("existing-dir", func(t *testing.T) {
		testParseGeneral(t, typ, "vendor", "vendor/", false, true, "", "", nil)
	})
	t.Run("path/to/obj", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj")
		testParseGeneral(t, typ, input, input+string(filepath.Separator), false, true, "", "", nil)
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj*")
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
	t.Run("s3://bucket/path", func(t *testing.T) {
		input := "s3://bucket/path"
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
}

func TestParseFileOrDir(t *testing.T) {
	typ := opt.FileOrDir
	t.Run("path/to/obj", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj")
		testParseGeneral(t, typ, input, input, false, true, "", "", nil)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj") + string(filepath.Separator)
		testParseGeneral(t, typ, input, input, false, true, "", "", nil)
	})
	t.Run("Existing-dir-without-slash", func(t *testing.T) {
		testParseGeneral(t, typ, "../vendor", "../vendor"+string(filepath.Separator), false, true, "", "", nil)
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj*")
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
	t.Run("s3://bucket/path", func(t *testing.T) {
		input := "s3://bucket/path"
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
}

func TestParseGlob(t *testing.T) {
	typ := opt.Glob
	t.Run("path/to/obj*", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj*")
		testParseGeneral(t, typ, input, input, false, true, "", "", nil)
	})
	t.Run("path/to/obj", func(t *testing.T) {
		input := filepath.Join("path", "to", "obj")
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
	t.Run("s3://bucket/path", func(t *testing.T) {
		input := "s3://bucket/path"
		testParseGeneral(t, typ, input, "", true, true, "", "", nil)
	})
}
