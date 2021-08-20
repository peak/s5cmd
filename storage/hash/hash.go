package hash

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/peak/s5cmd/storage"
)

// ErrorMultipartUpload states object uploaded using multipart.
var ErrorMultipartUpload error = errors.New("object uploaded multipart, hash comparison is not available for multipart uploaded objects")

// ObjectHash defines hash related properties of storage.Object structure
type ObjectHash struct {
	object    *storage.Object
	multipart int
	isLocal   bool
}

// NewObjectHash returns a new ObjectHash object.
func NewObjectHash(object *storage.Object) *ObjectHash {
	return &ObjectHash{
		object:    object,
		multipart: checkMultipart(object.Etag),
		isLocal:   !object.URL.IsRemote(),
	}
}

// different checks is given objecthash is different than source hash.
func (o *ObjectHash) different(target *ObjectHash) error {
	if o.isLocal { // local -> remote
		// remote is multipart uploaded.
		if target.multipart != 0 {
			return ErrorMultipartUpload
		} else {
			localHash, err := fileHash(o.object.URL.Path)
			if err != nil {
				return err
			}
			if localHash == target.object.Etag {
				return hashSameError(o.object.URL.Path, target.object.URL.Path)
			} else {
				return nil
			}
		}
	} else {
		if o.multipart != 0 {
			return ErrorMultipartUpload
		}

		if target.isLocal { // remote -> local
			localHash, err := fileHash(o.object.URL.Path)
			if err != nil {
				return err
			}
			if o.object.Etag == localHash {
				return hashSameError(o.object.URL.Path, target.object.URL.Path)
			} else {
				return nil
			}
		} else {
			if o.object.Etag == target.object.Etag {
				return hashSameError(o.object.URL.Path, target.object.URL.Path)
			} else {
				return nil
			}
		}
	}
}

// checkMultipart checks if given object is uploaded using multipart.
func checkMultipart(hashValue string) int {
	splits := strings.Split(hashValue, "-")
	if len(splits) != 2 {
		return 0
	}
	multipart, _ := strconv.Atoi(splits[1])
	return multipart
}

// fileHash computes hash of local file.
func fileHash(path string) (string, error) {
	file, err := os.Open(path)

	if err != nil {
		return "", err
	}
	defer file.Close()
	return fileToHash(file)
}

// fileToHash converts file to hash.
func fileToHash(r io.Reader) (string, error) {
	var MD5String string
	hash := md5.New()

	if _, err := io.Copy(hash, r); err != nil {
		return MD5String, err
	}

	hashInBytes := hash.Sum(nil)[:16]
	MD5String = hex.EncodeToString(hashInBytes)

	return MD5String, nil
}

// hashSameError returns an error which states source and target hash are same.
func hashSameError(source, target string) error {
	return fmt.Errorf("%s hash and %s hash are same. Content is not changed.", source, target)
}
