/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: CurveAdm
 * Created Date: 2023-03-13
 * Author: Lijin Xiong (lijin.xiong@zstack.io)
 */

package checker

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/opencurve/curveadm/cli/cli"
	comm "github.com/opencurve/curveadm/internal/common"
	"github.com/opencurve/curveadm/internal/configure/disks"
	"github.com/opencurve/curveadm/internal/configure/topology"
	"github.com/opencurve/curveadm/internal/errno"
	"github.com/opencurve/curveadm/internal/storage"
	"github.com/opencurve/curveadm/internal/task/context"
	"github.com/opencurve/curveadm/internal/task/step"
	"github.com/opencurve/curveadm/internal/task/task"
)

const (
	COPYSETS_STATUS_CMD  = "curve_ops_tool copysets-status"
	COPYSETS_NOT_HEALTHY = "Copysets not healthy"
)

type checkDiskInUse struct {
	host          string
	newDiskDevice string
	oldDisk       storage.Disk
	curveadm      *cli.CurveAdm
}

type checkDiskTheSame struct {
	newDiskId     string
	newDiskDevice string
	host          string
	oldDisk       storage.Disk
	curveadm      *cli.CurveAdm
}

type checkDiskSize struct {
	host          string
	newDiskDevice string
	newDiskSize   string
	oldDisk       storage.Disk
	curveadm      *cli.CurveAdm
}

type checkDiskEmpty struct {
	host          string
	newDiskDevice string
	fsType        string
	curveadm      *cli.CurveAdm
}

type checkCopysetsHealty struct {
	host          string
	chunkserverId string
	curveadm      *cli.CurveAdm
}

func (s *checkDiskSize) Execute(ctx *context.Context) error {
	var success bool
	step := &step.ListBlockDevice{ // disk device size
		Device:      []string{s.newDiskDevice},
		Format:      "SIZE -b",
		NoHeadings:  true,
		Success:     &success,
		Out:         &s.newDiskSize,
		ExecOptions: s.curveadm.ExecOptions(),
	}

	if err := step.Execute(ctx); err != nil {
		return err
	}

	if !success {
		return errno.ERR_LIST_BLOCK_DEVICES_FAILED.
			F("Get disk device[%s:%s] szie failed", s.host, s.newDiskDevice)
	}

	newDiskSize, err := strconv.ParseInt(s.newDiskSize, 10, 64)
	if err != nil {
		return err
	}

	oldDiskSize, err := strconv.ParseInt(s.oldDisk.Size, 10, 64)
	if err != nil {
		return err
	}

	if newDiskSize < oldDiskSize {
		return errno.ERR_REPLACE_DISK_SMALLER_SIZE.
			F("Disk device[%s] size[%d] in host[%s] is smaller than the former disk[%s] size[%d]",
				s.newDiskDevice, newDiskSize, s.host, s.oldDisk.Device, oldDiskSize)
	}
	return nil
}

func (s *checkDiskInUse) Execute(ctx *context.Context) error {
	oldDisk := s.oldDisk
	diskRecords, err := s.curveadm.Storage().GetDisk(
		comm.DISK_FILTER_DEVICE, s.host, s.newDiskDevice)
	if err != nil {
		return err
	}
	if len(diskRecords) > 0 {
		newDisk := diskRecords[0]
		if newDisk.ChunkServerID != oldDisk.ChunkServerID {
			return errno.ERR_REPLACE_DISK_USED_BY_OTHER_CHUNKSERVER.
				F("The disk[%s:%s] is being used by chunkserver %s",
					s.host, s.newDiskDevice, newDisk.ChunkServerID)
		}
	}
	return nil
}

func (s *checkDiskTheSame) Execute(ctx *context.Context) error {
	var success bool
	step := &step.ListBlockDevice{ // disk device uuid
		Device:      []string{s.newDiskDevice},
		Format:      "UUID",
		NoHeadings:  true,
		Success:     &success,
		Out:         &s.newDiskId,
		ExecOptions: s.curveadm.ExecOptions(),
	}

	if err := step.Execute(ctx); err != nil {
		return err
	}
	if !success {
		return errno.ERR_LIST_BLOCK_DEVICES_FAILED.
			F("Get disk device[%s:%s] uuid failed", s.host, s.newDiskDevice)
	}
	oldDiskId, _, err := disks.GetDiskId(s.oldDisk)
	if err != nil {
		return err
	}
	if s.newDiskId == oldDiskId {
		return errno.ERR_REPLACE_THE_SAME_PHYSICAL_DISK.
			F("The disk device[%s:%s] has not been physically replaced from the slot",
				s.host, s.newDiskDevice)
	}

	return nil
}

func (s *checkDiskEmpty) Execute(ctx *context.Context) error {
	var success bool
	step := &step.ListBlockDevice{ // disk device filesystem
		Device:      []string{s.newDiskDevice},
		Format:      "FSTYPE",
		NoHeadings:  true,
		Success:     &success,
		Out:         &s.fsType,
		ExecOptions: s.curveadm.ExecOptions(),
	}

	if err := step.Execute(ctx); err != nil {
		return err
	}
	if !success {
		return errno.ERR_LIST_BLOCK_DEVICES_FAILED.
			F("Get disk device[%s:%s] filesystem type failed", s.host, s.newDiskDevice)
	}

	if s.fsType != "" {
		return errno.ERR_DISK_NOT_EMPTY.
			F("The disk[%s:%s] has %s filesystem",
				s.host, s.newDiskDevice, s.fsType)
	}
	return nil
}

func (s *checkCopysetsHealty) Execute(ctx *context.Context) error {

	diskOfHost, err := s.curveadm.Storage().GetDisk(comm.DISK_FILTER_HOST, s.host)
	if err != nil {
		return err
	}

	cmdExecFailureCount := 0
	for _, cs := range diskOfHost {
		if cs.ChunkServerID == s.chunkserverId {
			continue
		}
		containerId, err := s.curveadm.Storage().GetContainerId(cs.ChunkServerID)
		if err != nil {
			return err
		}
		dockerCli := ctx.Module().DockerCli().ContainerExec(containerId, COPYSETS_STATUS_CMD)
		out, err := dockerCli.Execute(s.curveadm.ExecOptions())
		if err != nil {
			cmdExecFailureCount++
		}
		if strings.Contains(out, COPYSETS_NOT_HEALTHY) {
			return errno.ERR_REPLACE_DISK_CLUSTER_NOT_HEALTHY
		}
	}
	if cmdExecFailureCount > 0 {
		return errno.ERR_REPLACE_DISK_CLUSTER_HEALTH_UNKNOWN.
			F("Get cluster health failed for %d times, make sure other chunkservers are all running", cmdExecFailureCount)
	}

	return nil

}

func NewCheckDiskReplacementTask(curveadm *cli.CurveAdm, dc *topology.DeployConfig) (*task.Task, error) {
	host := dc.GetHost()
	hc, err := curveadm.GetHost(host)
	if err != nil {
		return nil, err
	}
	newDiskDevice := curveadm.MemStorage().Get(comm.DISK_REPLACEMENT_NEW_DISK_DEVICE).(string)
	oldDisk, err := curveadm.Storage().GetDiskByMountPoint(host, dc.GetDataDir())
	if err != nil {
		return nil, err
	}
	chunkserverId := oldDisk.ChunkServerID
	// new task
	subname := fmt.Sprintf("host=%s device=%s, chunkserverId=%s",
		host, newDiskDevice, chunkserverId)
	t := task.NewTask("Check Disk Replacement", subname, hc.GetSSHConfig())

	// 1. check cluster health
	t.AddStep(&checkCopysetsHealty{
		host:          host,
		chunkserverId: chunkserverId,
		curveadm:      curveadm,
	})

	// 2. check disk size
	t.AddStep((&checkDiskSize{
		host:          host,
		newDiskDevice: newDiskDevice,
		oldDisk:       oldDisk,
		curveadm:      curveadm,
	}))

	// 3. check disk in use
	t.AddStep(&checkDiskInUse{
		host:          host,
		newDiskDevice: newDiskDevice,
		oldDisk:       oldDisk,
		curveadm:      curveadm,
	})

	// 4. check the same disk
	t.AddStep(&checkDiskTheSame{
		host:          host,
		newDiskDevice: newDiskDevice,
		oldDisk:       oldDisk,
		curveadm:      curveadm,
	})

	// 5. check disk empty
	t.AddStep(&checkDiskEmpty{
		host:          host,
		newDiskDevice: newDiskDevice,
		curveadm:      curveadm,
	})

	// 6. umount old disk
	t.AddStep(&step.UmountFilesystem{
		Directorys:     []string{oldDisk.MountPoint},
		IgnoreUmounted: true,
		IgnoreNotFound: true,
		ExecOptions:    curveadm.ExecOptions(),
	})

	return t, nil
}
