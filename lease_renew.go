package hdfs

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/v2/internal/rpc"
	"github.com/golang/protobuf/proto"
)

const leaseRenewInterval = 30 * time.Second

type leaseRenewer struct {
	ctx        context.Context
	Cancel     context.CancelFunc
	errCh      chan error
	wg         sync.WaitGroup
	filesWOpen uint64
}

func (c *Client) leaseRenew() error {
	if atomic.LoadUint64(&c.filesWOpen) == 0 {
		return nil
	}
	req := &hdfs.RenewLeaseRequestProto{
		ClientName: proto.String(c.namenode.ClientName),
	}
	resp := &hdfs.RenewLeaseResponseProto{}

	if err := c.namenode.Execute("renewLease", req, resp); err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr)
		}

		return err
	}

	return nil
}

func (c *Client) leaseRenewerRun() {
	defer c.wg.Done()
	ticker := time.NewTicker(leaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.leaseRenew(); err != nil {
				fmt.Fprintf(os.Stderr, "hdfs lease renew error: %+v\n", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}
