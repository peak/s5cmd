package s5cmd

import "testing"

func TestParseUnchecked(t *testing.T) {
	t.Run("PARAM_UNCHECKED", func(t *testing.T) {
		input := "testStr"
		testParseGeneral(t, PARAM_UNCHECKED, input, input, false, true, "", "")
	})
	t.Run("PARAM_UNCHECKED_ONE_OR_MORE", func(t *testing.T) {
		input := "testStr1"
		testParseGeneral(t, PARAM_UNCHECKED_ONE_OR_MORE, input, input, false, true, "", "")
	})
	t.Run("PARAM_UNCHECKED_ONE_OR_MORE", func(t *testing.T) {
		input := "testStr1 testStr2"
		testParseGeneral(t, PARAM_UNCHECKED_ONE_OR_MORE, input, input, false, true, "", "")
	})
}

func testParseGeneral(t *testing.T, typ ParamType, input, expectedOutArg string, expectError, expectNilS3 bool, expectedS3bucket, expectedS3key string) {
	a, err := parseArgumentByType(input, typ, nil)

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

	if a.s3.bucket != expectedS3bucket {
		t.Errorf(`"Expected a.s3.bucket was "%s" but got "%s"`, expectedS3bucket, a.s3.bucket)
	}
	if a.s3.key != expectedS3key {
		t.Errorf(`"Expected a.s3.key was "%s" but got "%s"`, expectedS3key, a.s3.key)
	}
}

func TestParseS3Obj(t *testing.T) {
	typ := PARAM_S3OBJ
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "")
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "")
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		testParseGeneral(t, typ, inputKey, "", true, false, "", "")
	})
}

func TestParseS3Dir(t *testing.T) {
	typ := PARAM_S3DIR
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "")
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "")
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		testParseGeneral(t, typ, inputKey, "", true, false, "", "")
	})
	t.Run("missing-key", func(t *testing.T) {
		inputBucket := "s3://bucket"
		testParseGeneral(t, typ, inputBucket, "", true, false, "", "")
	})
}

func TestParseS3WildObj(t *testing.T) {
	typ := PARAM_S3WILDOBJ
	t.Run("path/to/wild/*obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/wild/*obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey)
	})
	t.Run("path/to/wild/*obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/wild/*obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey)
	})
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "")
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "")
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		testParseGeneral(t, typ, inputKey, "", true, false, "", "")
	})
	t.Run("missing-key", func(t *testing.T) {
		inputBucket := "s3://bucket"
		testParseGeneral(t, typ, inputBucket, "", true, false, "", "")
	})
}

func TestParseS3ObjOrDir(t *testing.T) {
	typ := PARAM_S3OBJORDIR
	t.Run("path/to/obj", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj/"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey)
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, input, false, false, inputBucket, inputKey)
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		inputBucket := "bucket"
		inputKey := "path/to/obj*"

		input := "s3://" + inputBucket + "/" + inputKey
		testParseGeneral(t, typ, input, "", true, false, "", "")
	})
	t.Run("missing-bucket", func(t *testing.T) {
		inputKey := "path/to/obj*"
		testParseGeneral(t, typ, inputKey, "", true, false, "", "")
	})
}

func TestParseFileObj(t *testing.T) {
	typ := PARAM_FILEOBJ
	t.Run("path/to/obj", func(t *testing.T) {
		input := "path/to/obj"
		testParseGeneral(t, typ, input, input, false, true, "", "")
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		input := "path/to/obj/"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		input := "path/to/obj*"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
	t.Run("s3://bucket/path", func(t *testing.T) {
		input := "s3://bucket/path"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
}

func TestParseFileDir(t *testing.T) {
	typ := PARAM_DIR
	t.Run("path/to/obj/", func(t *testing.T) {
		input := "path/to/obj/"
		testParseGeneral(t, typ, input, input, false, true, "", "")
	})
	t.Run("cmd", func(t *testing.T) {
		testParseGeneral(t, typ, "cmd", "cmd/", false, true, "", "")
	})
	t.Run("path/to/obj", func(t *testing.T) {
		input := "path/to/obj"
		testParseGeneral(t, typ, input, input+"/", false, true, "", "")
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		input := "path/to/obj*"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
	t.Run("s3://bucket/path", func(t *testing.T) {
		input := "s3://bucket/path"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
}

func TestParseFileOrDir(t *testing.T) {
	typ := PARAM_FILEORDIR
	t.Run("path/to/obj", func(t *testing.T) {
		input := "path/to/obj"
		testParseGeneral(t, typ, input, input, false, true, "", "")
	})
	t.Run("path/to/obj/", func(t *testing.T) {
		input := "path/to/obj/"
		testParseGeneral(t, typ, input, input, false, true, "", "")
	})
	t.Run("cmd", func(t *testing.T) {
		testParseGeneral(t, typ, "cmd", "cmd/", false, true, "", "")
	})
	t.Run("path/to/obj*", func(t *testing.T) {
		input := "path/to/obj*"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
	t.Run("s3://bucket/path", func(t *testing.T) {
		input := "s3://bucket/path"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
}

func TestParseGlob(t *testing.T) {
	typ := PARAM_GLOB
	t.Run("path/to/obj*", func(t *testing.T) {
		input := "path/to/obj*"
		testParseGeneral(t, typ, input, input, false, true, "", "")
	})
	t.Run("path/to/obj", func(t *testing.T) {
		input := "path/to/obj"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
	t.Run("s3://bucket/path", func(t *testing.T) {
		input := "s3://bucket/path"
		testParseGeneral(t, typ, input, "", true, true, "", "")
	})
}
