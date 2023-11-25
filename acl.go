package hdfs

import (
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/v2/internal/rpc"
	"github.com/golang/protobuf/proto"
)

// AclStatus represents a set of ACL information about a file or directory in
// HDFS. It's provided directly by the namenode, and has no unix filesystem
// analogue.
type AclStatus struct {
	name      string
	aclStatus *hdfs.AclStatusProto
}

// Displays the Access Control Lists (ACLs) of files and directories. If a
// directory has a default ACL, then getfacl also displays the default ACL.
func (c *Client) Getfacl(name string) (*AclStatus, error) {
	as, err := c.getAclStatus(name)
	if err != nil {
		err = &os.PathError{"getfacl", name, err}
	}

	return as, err
}

func (c *Client) getAclStatus(name string) (*AclStatus, error) {
	req := &hdfs.GetAclStatusRequestProto{Src: proto.String(name)}
	resp := &hdfs.GetAclStatusResponseProto{}

	err := c.namenode.Execute("getAclStatus", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = nnErr
		}

		return nil, err
	}

	return &AclStatus{name, resp.GetResult()}, nil
}

// Owner returns the name of the user that owns the file or directory.
func (as *AclStatus) Owner() string {
	return as.aclStatus.GetOwner()
}

// OwnerGroup returns the name of the group that owns the file or directory.
func (as *AclStatus) OwnerGroup() string {
	return as.aclStatus.GetGroup()
}

// Sticky returns the bool flag Sticky bits of the file or directory.
func (as *AclStatus) Sticky() bool {
	return as.aclStatus.GetSticky()
}

// File or Directory permision - same spec as posix
func (as *AclStatus) Permission() *hdfs.FsPermissionProto {
	return as.aclStatus.GetPermission()
}

// Displays the Access Control Lists (ACLs) of files and directories.
func (as *AclStatus) Entries() []*hdfs.AclEntryProto {
	return as.aclStatus.GetEntries()
}
