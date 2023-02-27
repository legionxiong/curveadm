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

type checkDiskUsedByOtherChunkserver struct {
	chunkserverId string
	disk          storage.Disk
}

type checkDiskTheSame struct {
	newDiskId   string
	diskDevPath string
	host        string
	disk        storage.Disk
}

func (s *checkDiskUsedByOtherChunkserver) Execute(ctx *context.Context) error {
	disk := s.disk
	if disk.ChunkServerID != s.chunkserverId {
		return errno.ERR_REPLACE_DISK_USED_BY_OTHER_CHUNKSERVER.
			F("The disk[%s:%s] is being used by chunkserver %s",
				disk.Host, disk.Device, disk.ChunkServerID)
	}
	return nil
}

func (s *checkDiskTheSame) Execute(ctx *context.Context) error {
	disk := s.disk
	if diskId, err := disks.GetDiskId(disk); err != nil {
		return err
	} else if diskId == s.newDiskId {
		return errno.ERR_REPLACE_THE_SAME_PHYSICAL_DISK.
			F("The new disk[%s:%s] and the former disk has the same UUID[%s]",
				s.host, s.diskDevPath, s.newDiskId)
	}
	return nil
}

func NewCheckDiskReplacementTask(curveadm *cli.CurveAdm, dc *topology.DeployConfig) (*task.Task, error) {
	var disk storage.Disk
	hc, err := curveadm.GetHost(dc.GetHost())
	if err != nil {
		return nil, err
	}
	diskDevPath := curveadm.MemStorage().Get(comm.DISK_DEVICE).(string)
	chunkserverId := curveadm.MemStorage().Get(comm.DISK_CHUNKSERVER_ID).(string)
	if diskRecords, err := curveadm.Storage().GetDisk(
		"device", dc.GetHost(), diskDevPath); err != nil {
		return nil, err
	} else {
		if len(diskRecords) == 0 {
			return nil, errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
				F("The disk[%s:%s] was not found", disk.Host, disk.Device)
		}
		disk = diskRecords[0]
	}

	// new task
	subname := fmt.Sprintf("host=%s device=%s, chunkserverId=%s",
		dc.GetHost(), diskDevPath, chunkserverId)
	t := task.NewTask("Check Disk Replacement", subname, hc.GetSSHConfig())

	var diskUuid string
	// 1. get new disk uuid
	t.AddStep(&step.ListBlockDevice{
		Device:      []string{diskDevPath},
		Format:      "UUID",
		NoHeadings:  true,
		Out:         &diskUuid,
		ExecOptions: curveadm.ExecOptions(),
	})
	// 1. check if disk used by other chunkserver
	t.AddStep(&checkDiskUsedByOtherChunkserver{
		chunkserverId: chunkserverId,
		disk:          disk,
	})

	// 2. check if new disk and old disk are the same
	t.AddStep(&checkDiskTheSame{
		newDiskId:   diskUuid,
		diskDevPath: diskDevPath,
		host:        dc.GetHost(),
	})

	return t, nil
}
