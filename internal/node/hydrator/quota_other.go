//go:build !linux

package hydrator

import "context"

func (d *GVisorSandboxDriver) applyHostQuota(context.Context, string, int64, int64) error {
	return nil
}

func (d *GVisorSandboxDriver) releaseHostQuota(context.Context, string) error {
	return nil
}
