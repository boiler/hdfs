package main

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

func cp(args []string, quiet, force, preserve, notemp bool) {
	sources, srcns, err := normalizePaths(args[0:1])
	if err != nil {
		fatal(err)
	}
	src := sources[0]

	dests, dstns, err := normalizePaths(args[1:])
	if err != nil {
		fatal(err)
	}
	dst := dests[0]

	dstTmp := dst + "._COPYING_"
	if notemp {
		dstTmp = dst
	}

	srcClient, err := getClient(srcns)
	if err != nil {
		fatal(err)
	}

	dstClient, err := getClient(dstns)
	if err != nil {
		fatal(err)
	}

	srcStat, err := srcClient.Stat(src)
	if err != nil {
		fatal(err)
	}

	dstStat, err := dstClient.Stat(dst)
	if err == nil {
		if force {
			dstClient.Remove(dst)
		} else {
			fatal(errors.New("file already exists"))
		}
	}

	if force {
		dstClient.Remove(dstTmp)
	}

	reader, err := srcClient.Open(src)
	if err != nil {
		fatal(err)
	}
	bufReader := bufio.NewReader(reader)

	writer, err := dstClient.Create(dstTmp)
	if err != nil {
		fatal(err)
	}
	bufWriter := bufio.NewWriter(writer)

	if !quiet && term.IsTerminal(int(os.Stdout.Fd())) {
		bar := progressbar.DefaultBytes(srcStat.Size(), filepath.Base(dst))
		_, err = io.Copy(io.MultiWriter(bufWriter, bar), bufReader)
	} else {
		_, err = io.Copy(bufWriter, bufReader)
	}
	if err != nil {
		fatal(err)
	}

	reader.Close()
	bufWriter.Flush()
	writer.Close()

	dstStat, err = dstClient.Stat(dstTmp)
	if err != nil {
		fatal(err)
	}

	if srcStat.Size() != dstStat.Size() {
		dstClient.Remove(dstTmp)
		fatal(errors.New("sizes are different"))
	}

	if preserve {
		err = dstClient.Chtimes(dstTmp, srcStat.ModTime(), srcStat.ModTime())
		if err != nil {
			dstClient.Remove(dstTmp)
			fatal(errors.New("change mtime error"))
		}
	}

	if dstTmp != dst {
		err = dstClient.Rename(dstTmp, dst)
		if err != nil {
			fatal(err)
		}
	}

}
