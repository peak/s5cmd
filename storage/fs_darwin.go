//go:build darwin

package storage

import (
	"os"
	"syscall"
	"time"
)

func GetFileTime(filename string) (time.Time, time.Time, time.Time, error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}

	stat := fi.Sys().(*syscall.Stat_t)
	cTime := time.Unix(int64(stat.Ctimespec.Sec), int64(stat.Ctimespec.Nsec))
	aTime := time.Unix(int64(stat.Atimespec.Sec), int64(stat.Atimespec.Nsec))

	mTime := fi.ModTime()

	return aTime, mTime, cTime, nil
}

func SetFileTime(filename string, accessTime, modificationTime, creationTime time.Time) error {
	var err error
	if accessTime.IsZero() && modificationTime.IsZero() {
		// Nothing recorded in s3. Return fast.
		return nil
	} else if accessTime.IsZero() {
		accessTime, _, _, err = GetFileTime(filename)
		if err != nil {
			return err
		}
	} else if modificationTime.IsZero() {
		_, modificationTime, _, err = GetFileTime(filename)
		if err != nil {
			return err
		}
	}
	err = os.Chtimes(filename, accessTime, modificationTime)
	if err != nil {
		return err
	}
	return nil
}
