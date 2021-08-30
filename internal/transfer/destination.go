package transfer

import (
	"context"
	"strings"

	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

func (m *Manager) PrepareRemoteDestination(srcurl, dsturl *url.URL) *url.URL {
	objname := srcurl.Base()
	if m.isBatch && !m.flatten {
		objname = srcurl.Relative()
	}

	if dsturl.IsPrefix() || dsturl.IsBucket() {
		dsturl = dsturl.Join(objname)
	}
	return dsturl
}

func (m *Manager) PrepareLocalDestination(ctx context.Context, srcurl, dsturl *url.URL) (*url.URL, error) {
	objname := srcurl.Base()
	if m.isBatch && !m.flatten {
		objname = srcurl.Relative()
	}

	client := storage.NewLocalClient(m.storageOptions)

	if m.isBatch {
		err := client.MkdirAll(dsturl.Absolute())
		if err != nil {
			return nil, err
		}
	}

	obj, err := client.Stat(ctx, dsturl)
	if err != nil && err != storage.ErrGivenObjectNotFound {
		return nil, err
	}

	if m.isBatch && !m.flatten {
		dsturl = dsturl.Join(objname)
		err := client.MkdirAll(dsturl.Dir())
		if err != nil {
			return nil, err
		}
	}

	if err == storage.ErrGivenObjectNotFound {
		err := client.MkdirAll(dsturl.Dir())
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(dsturl.Absolute(), "/") {
			dsturl = dsturl.Join(objname)
		}
	} else {
		if obj.Type.IsDir() {
			dsturl = obj.URL.Join(objname)
		}
	}

	return dsturl, nil
}
