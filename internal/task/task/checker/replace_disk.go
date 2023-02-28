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
 * Created Date: 2023-02-27
 * Author: Lijin Xiong (lijin.xiong@zstack.io)
 */

package checker

import (
	"fmt"
	"strconv"

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

type checkDiskUsed struct {
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

func (s *checkDiskSize) Execute(ctx *context.Context) error {
	// curveadm := s.curveadm
	// steps := []task.Step{}

	var success bool
	// steps = append(steps, &step.ListBlockDevice{ // disk device size
	// 	Device:      []string{s.newDiskDevice},
	// 	Format:      "SIZE -b",
	// 	NoHeadings:  true,
	// 	Success:     &success,
	// 	Out:         &s.newDiskSize,
	// 	ExecOptions: curveadm.ExecOptions(),
	// })

	step := &step.ListBlockDevice{ // disk device size
		Device:      []string{s.newDiskDevice},
		Format:      "SIZE -b",
		NoHeadings:  true,
		Success:     &success,
		Out:         &s.newDiskSize,
		ExecOptions: s.curveadm.ExecOptions(),
	}
	// for _, step := range steps {
	if err := step.Execute(ctx); err != nil {
		return err
	}
	// }

	if !success {
		return errno.ERR_LIST_BLOCK_DEVICES_FAILED.
			F("Get disk[%s:%s] szie failed", s.host, s.newDiskDevice)
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
			F("Disk[%s] size[%d] in host[%s] is smaller than the former disk[%s] size[%d]",
				s.newDiskDevice, newDiskSize, s.host, s.oldDisk.Device, oldDiskSize)
	}
	return nil
}

func (s *checkDiskUsed) Execute(ctx *context.Context) error {
	oldDisk := s.oldDisk
	diskRecords, err := s.curveadm.Storage().GetDisk(
		comm.DISK_QUERY_DEVICE, s.host, s.newDiskDevice)
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

func (s *checkDiskEmpty) Execute(ctx *context.Context) error {
	// steps := []task.Step{}
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
			F("Get disk[%s:%s] filesystem type failed", s.host, s.newDiskDevice)
	}

	if s.fsType != "" {
		return errno.ERR_DISK_NOT_EMPTY.
			F("The disk[%s:%s] has %s filesystem",
				s.host, s.newDiskDevice, s.fsType)
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
			F("Get disk[%s:%s] uuid failed", s.host, s.newDiskDevice)
	}
	oldDiskId, err := disks.GetDiskId(s.oldDisk)
	if err != nil {
		return err
	}
	fmt.Println(oldDiskId, s.newDiskId)
	if s.newDiskId == oldDiskId {
		return errno.ERR_REPLACE_THE_SAME_PHYSICAL_DISK.
			F("The disk[%s:%s] and the former disk has the same UUID[%s]",
				s.host, s.newDiskDevice, s.newDiskId)
	}
	return nil
}

func NewCheckDiskReplacementTask(curveadm *cli.CurveAdm, dc *topology.DeployConfig) (*task.Task, error) {
	var oldDisk storage.Disk
	hc, err := curveadm.GetHost(dc.GetHost())
	if err != nil {
		return nil, err
	}
	newDiskDevice := curveadm.MemStorage().Get(comm.DISK_QUERY_DEVICE).(string)
	chunkserverId := curveadm.MemStorage().Get(comm.DISK_CHUNKSERVER_ID).(string)
	if diskRecords, err := curveadm.Storage().GetDisk(
		comm.DISK_QUERY_SERVICE, chunkserverId); err != nil {
		return nil, err
	} else {
		if len(diskRecords) == 0 {
			return nil, errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
				F("The disk of chunkserver[ID:%s] was not found", chunkserverId)
		}
		oldDisk = diskRecords[0]
	}

	// new task
	subname := fmt.Sprintf("host=%s device=%s, chunkserverId=%s",
		dc.GetHost(), newDiskDevice, chunkserverId)
	t := task.NewTask("Check Disk Replacement", subname, hc.GetSSHConfig())

	// var diskUuid string
	// // 1. get new disk uuid
	// t.AddStep(&step.ListBlockDevice{
	// 	Device:      []string{newDiskDevice},
	// 	Format:      "UUID",
	// 	NoHeadings:  true,
	// 	Out:         &diskUuid,
	// 	ExecOptions: curveadm.ExecOptions(),
	// })

	// 1. check if new disk size smaller than old disk size
	t.AddStep((&checkDiskSize{
		host:          dc.GetHost(),
		newDiskDevice: newDiskDevice,
		oldDisk:       oldDisk,
		curveadm:      curveadm,
	}))

	// 2. check if disk used by other chunkserver
	t.AddStep(&checkDiskUsed{
		host:          dc.GetHost(),
		newDiskDevice: newDiskDevice,
		oldDisk:       oldDisk,
		curveadm:      curveadm,
	})

	// 3. check if new disk and old oldDisk are the same
	t.AddStep(&checkDiskTheSame{
		host:          dc.GetHost(),
		newDiskDevice: newDiskDevice,
		oldDisk:       oldDisk,
		curveadm:      curveadm,
	})

	// 4. check disk not empty
	t.AddStep(&checkDiskEmpty{
		host:          dc.GetHost(),
		newDiskDevice: newDiskDevice,
		curveadm:      curveadm,
	})

	return t, nil
}
