package main

import (
	"bytes"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

func get(args []string, quiet bool) {
	if len(args) == 0 || len(args) > 2 {
		fatalWithUsage()
	}

	sources, nn, err := normalizePaths(args[0:1])
	if err != nil {
		fatal(err)
	}

	source := sources[0]
	var dest string
	if len(args) == 2 {
		dest = args[1]
	} else {
		cwd, err := os.Getwd()
		if err != nil {
			fatal(err)
		}

		_, name := path.Split(source)
		dest = filepath.Join(cwd, name)
	}

	client, err := getClient(nn)
	if err != nil {
		fatal(err)
	}

	err = client.Walk(source, func(p string, fi os.FileInfo, err error) error {
		fullDest := filepath.Join(dest, strings.TrimPrefix(p, source))

		if err != nil {
			fatal(err)
		}

		if fi.IsDir() {
			err = os.Mkdir(fullDest, 0755)
			if err != nil {
				fatal(err)
			}
		} else {
			if !quiet && term.IsTerminal(int(os.Stdout.Fd())) {
				local, err := os.Create(fullDest)
				if err != nil {
					return err
				}
				defer local.Close()
				stat, _ := client.Stat(p)
				remote, err := client.Open(p)
				if err != nil {
					return err
				}
				bar := progressbar.DefaultBytes(stat.Size(), filepath.Base(fullDest))
				_, err = io.Copy(io.MultiWriter(local, bar), remote)
				if err != nil {
					remote.Close()
					return err
				}
				err = remote.Close()
			} else {
				err = client.CopyToLocal(p, fullDest)
			}
			if pathErr, ok := err.(*os.PathError); ok {
				fatal(pathErr)
			} else if err != nil {
				fatal(err)
			}
		}
		return nil
	})

	if err != nil {
		fatal(err)
	}
}

func getmerge(args []string, addNewlines bool) {
	if len(args) != 2 {
		fatalWithUsage()
	}

	dest := args[1]
	sources, nn, err := normalizePaths(args[0:1])
	if err != nil {
		fatal(err)
	}

	client, err := getClient(nn)
	if err != nil {
		fatal(err)
	}

	local, err := os.Create(dest)
	if err != nil {
		fatal(err)
	}

	source := sources[0]
	children, err := client.ReadDir(source)
	if err != nil {
		fatal(err)
	}

	readers := make([]io.Reader, 0, len(children))
	for _, child := range children {
		if child.IsDir() {
			continue
		}

		childPath := path.Join(source, child.Name())
		file, err := client.Open(childPath)
		if err != nil {
			fatal(err)
		}

		readers = append(readers, file)
		if addNewlines {
			readers = append(readers, bytes.NewBufferString("\n"))
		}
	}

	_, err = io.Copy(local, io.MultiReader(readers...))
	if err != nil {
		fatal(err)
	}
}
