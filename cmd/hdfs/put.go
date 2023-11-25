package main

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

func put(args []string, force, preserve, notemp bool) {
	if len(args) != 2 {
		fatalWithUsage()
	}

	dests, nn, err := normalizePaths(args[1:])
	if err != nil {
		fatal(err)
	}

	dest := dests[0]
	source := args[0]

	if source != "-" {
		source, err = filepath.Abs(source)
		if err != nil {
			fatal(err)
		}
	}

	var sourceStat fs.FileInfo
	if source != "-" {
		sourceStat, err = os.Stat(source)
		if err != nil {
			fatal(err)
		}
	}

	client, err := getClient(nn)
	if err != nil {
		fatal(err)
	}

	existing, err := client.Stat(dest)
	if err == nil {
		if existing.IsDir() {
			dest = path.Join(dest, filepath.Base(source))
		} else {
			if !force {
				fatal(&os.PathError{"put", dest, os.ErrExist})
			}
		}
	} else if !os.IsNotExist(err) {
		fatal(err)
	}

	putFunc := func(src, fullDest string) error {
		fullDestTmp := fullDest + "._COPYING_"
		if notemp {
			fullDestTmp = fullDest
		}

		_, err = client.Stat(fullDest)
		if err == nil {
			if force {
				client.Remove(fullDest)
			} else {
				return errors.New("file already exists")
			}
		}

		writer, err := client.Create(fullDestTmp)
		if err != nil {
			return err
		}

		var reader *os.File
		if source == "-" {
			reader = os.Stdin
		} else {
			reader, err = os.Open(src)
			if err != nil {
				return err
			}
		}

		if term.IsTerminal(int(os.Stdout.Fd())) && source != "-" {
			bar := progressbar.DefaultBytes(sourceStat.Size(), filepath.Base(fullDest))
			_, err = io.Copy(io.MultiWriter(writer, bar), reader)
		} else {
			_, err = io.Copy(writer, reader)
		}
		if err != nil {
			return err
		}

		reader.Close()
		writer.Close()

		hdfsStat, err := client.Stat(fullDestTmp)
		if err != nil {
			return err
		}

		if source != "-" && sourceStat.Size() != hdfsStat.Size() {
			os.Remove(fullDestTmp)
			return errors.New("sizes are different")
		}

		if source != "-" && preserve {
			err = client.Chtimes(fullDestTmp, sourceStat.ModTime(), sourceStat.ModTime())
			if err != nil {
				os.Remove(fullDestTmp)
				return errors.New("change mtime error")
			}
		}

		if fullDestTmp != fullDest {
			err = client.Rename(fullDestTmp, fullDest)
			if err != nil {
				return err
			}
		}

		return nil
	}

	if source == "-" {
		err = putFunc(source, dest)
	} else {
		mode := 0755 | os.ModeDir
		err = filepath.Walk(source, func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			rel, err := filepath.Rel(source, p)
			if err != nil {
				return err
			}

			fullDest := path.Join(dest, rel)
			if fi.IsDir() {
				client.Mkdir(fullDest, mode)
			} else {
				return putFunc(p, fullDest)
			}

			return nil
		})
	}

	if err != nil {
		fatal(err)
	}
}
