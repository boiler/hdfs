package main

import (
	"fmt"
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
)

func getfacl(paths []string, recursive bool) {

	var AclEntryTypeProto = map[hdfs.AclEntryProto_AclEntryTypeProto]string{
		0: "user",
		1: "group",
		2: "mask",
		3: "other",
	}
	var FsActionProto = map[hdfs.AclEntryProto_FsActionProto]string{
		0: "---",
		1: "--x",
		2: "-w-",
		3: "-wx",
		4: "r--",
		5: "r-x",
		6: "rw-",
		7: "rwx",
	}

	paths, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	visit := func(p string, fi os.FileInfo, err error) error {

		facl, err := client.Getfacl(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			status = 1
		}

		fmt.Printf("# file: %s\n", p)
		fmt.Printf("# owner: %s\n", facl.Owner())
		fmt.Printf("# group: %s\n", facl.OwnerGroup())
		fmt.Printf("# perm: %s\n", fi.Mode().String())

		if facl.Sticky() == true {
			if fi.Mode()&0001 != 0 {
				fmt.Printf("# flags: --t\n")
			} else {
				fmt.Printf("# flags: --T\n")
			}
		}
		for _, e := range facl.Entries() {
			if e.GetScope() == hdfs.AclEntryProto_DEFAULT {
				fmt.Printf("default:%s:%s:%s\n", AclEntryTypeProto[e.GetType()], e.GetName(), FsActionProto[e.GetPermissions()])
			} else {
				if len(e.GetName()) > 0 {
					fmt.Printf("%s:%s:%s\n", AclEntryTypeProto[e.GetType()], e.GetName(), FsActionProto[e.GetPermissions()])
				}
			}
		}

		return nil
	}

	for _, p := range paths {
		if recursive {
			err = client.Walk(p, visit)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				status = 1
			}
		} else {
			info, err := client.Stat(p)
			if err != nil {
				fatal(err)
			}
			visit(p, info, nil)
		}
	}

}
