package command

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"gotest.tools/v3/fs"

	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

func TestExpandSource_Follow_Link_To_Single_File(t *testing.T) {
	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("f1.txt", ""),
		),
		fs.WithDir(
			"b",
		),
		fs.WithSymlink("b/my_link", "a/f1.txt"),
	}

	workdir := fs.NewDir(t, "expandsourcetest", folderLayout...)
	defer workdir.Remove()

	ctx := context.Background()
	workdirUrl, _ := url.New(workdir.Join("b/my_link"))

	//follow symbolic links
	ch, _ := expandSource(ctx, true, workdirUrl, storage.Options{})
	var expected []string
	for obj := range ch {
		expected = append(expected, obj.URL.Absolute())
	}
	workdirJoin := filepath.ToSlash(workdir.Join("b/my_link"))
	assert.Equal(t, []string{workdirJoin}, expected)
}

func TestExpandSource_Do_Not_Follow_Link_To_Single_File(t *testing.T) {
	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("f1.txt", ""),
		),
		fs.WithDir(
			"b",
		),
		fs.WithSymlink("b/my_link", "a/f1.txt"),
	}

	workdir := fs.NewDir(t, "expandsourcetest", folderLayout...)
	defer workdir.Remove()

	ctx := context.Background()
	workdirUrl, _ := url.New(workdir.Join("b/my_link"))

	//do not follow symbolic links
	ch, _ := expandSource(ctx, false, workdirUrl, storage.Options{})
	var expected []string
	for obj := range ch {
		expected = append(expected, obj.URL.Absolute())
	}
	assert.Empty(t, expected)
}

func TestExpandSource_Follow_Link_To_Directory(t *testing.T) {
	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("f1.txt", ""),
			fs.WithFile("f2.txt", ""),
			fs.WithDir("b",
				fs.WithFile("f3.txt", "")),
		),
		fs.WithDir(
			"c",
		),
		fs.WithSymlink("c/my_link", "a"),
	}

	workdir := fs.NewDir(t, "expandsourcetest", folderLayout...)
	defer workdir.Remove()

	ctx := context.Background()
	workdirUrl, _ := url.New(workdir.Join("c/my_link"))

	//follow symbolic links
	ch, _ := expandSource(ctx, true, workdirUrl, storage.Options{})
	var expected []string
	for obj := range ch {
		expected = append(expected, obj.URL.Absolute())
	}
	sort.Strings(expected)
	assert.Equal(t, []string{
		filepath.ToSlash(workdir.Join("c/my_link/b/f3.txt")),
		filepath.ToSlash(workdir.Join("c/my_link/f1.txt")),
		filepath.ToSlash(workdir.Join("c/my_link/f2.txt")),
	}, expected)
}

func TestExpandSource_Do_Not_Follow_Link_To_Directory(t *testing.T) {
	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("f1.txt", ""),
			fs.WithFile("f2.txt", ""),
			fs.WithDir("b",
				fs.WithFile("f3.txt", "")),
		),
		fs.WithDir(
			"c",
		),
		fs.WithSymlink("c/my_link", "a"),
	}

	workdir := fs.NewDir(t, "expandsourcetest", folderLayout...)
	defer workdir.Remove()

	ctx := context.Background()
	workdirUrl, _ := url.New(workdir.Join("c/my_link"))

	//do not follow symbolic links
	ch, _ := expandSource(ctx, false, workdirUrl, storage.Options{})
	var expected []string
	for obj := range ch {
		expected = append(expected, obj.URL.Absolute())
	}
	assert.Empty(t, expected)
}

func TestExpandSource_Do_Not_Follow_Symlinks(t *testing.T) {
	ctx := context.Background()
	fileContent := "CAFEBABE"
	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("f1.txt", fileContent),
		),
		fs.WithDir("b"),
		fs.WithDir("c"),
		fs.WithSymlink("b/link1", "a/f1.txt"),
		fs.WithSymlink("c/link2", "b/link1"),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	workdirUrl, _ := url.New(workdir.Path())

	//do not follow symbolic links
	ch, _ := expandSource(ctx, false, workdirUrl, storage.Options{})
	var expected []string
	for obj := range ch {
		expected = append(expected, obj.URL.Absolute())
	}
	workdirJoin := filepath.ToSlash(workdir.Join("a/f1.txt"))
	assert.Equal(t, []string{workdirJoin}, expected)
}
