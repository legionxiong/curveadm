/*
 *  Copyright (c) 2023 NetEase Inc.
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

package bs

import (
	"fmt"

	"github.com/opencurve/curveadm/cli/cli"
	comm "github.com/opencurve/curveadm/internal/common"
	"github.com/opencurve/curveadm/internal/configure/disks"
	"github.com/opencurve/curveadm/internal/configure/topology"
	"github.com/opencurve/curveadm/internal/errno"
	"github.com/opencurve/curveadm/internal/task/context"
	"github.com/opencurve/curveadm/internal/task/step"
	"github.com/opencurve/curveadm/internal/task/task"
)

type replaceDisk struct {
	chunkserverId string
	diskDevPath   string
	host          string
	diskData      string
	curveadm      *cli.CurveAdm
}

func updateDiskUuid(uuid, diskDevPath, host string,
	curveadm *cli.CurveAdm, ctx *context.Context) error {
	var success bool
	var tune2fsRet *string
	step := &step.Tune2Filesystem{
		Device:      diskDevPath,
		Param:       fmt.Sprintf("-U %s", uuid),
		Success:     &success,
		Out:         tune2fsRet,
		ExecOptions: curveadm.ExecOptions(),
	}
	if err := step.Execute(ctx); err != nil {
		return err
	}
	if !success {
		return errno.ERR_TUNE2FS_UPDATE_DISK_UUID_FAILED.
			F("Failed to update the uuid of disk[%s:%s]", host, diskDevPath)
	}
	return nil
}

func (s *replaceDisk) Execute(ctx *context.Context) error {
	if len(s.chunkserverId) == 0 {
		return nil
	}

	diskRecords, err := s.curveadm.Storage().GetDisk(comm.DISK_FILTER_SERVICE, s.chunkserverId)
	if err != nil {
		return err
	}
	if len(diskRecords) == 0 {
		return errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
			F("The chunkserver[ID: %s] currently has not disk device",
				s.chunkserverId)
	}
	disk := diskRecords[0]
	formerDiskId, _, err := disks.GetDiskId(disk)
	if err != nil {
		return err
	}
	if err := updateDiskUuid(formerDiskId, s.host, s.diskDevPath, s.curveadm, ctx); err != nil {
		return err
	}
	if err := disks.UpdateDisks(s.diskData, s.host, s.diskDevPath,
		disk, s.curveadm); err != nil {
		return err
	}
	return nil
}

func NewReplaceDiskTask(curveadm *cli.CurveAdm, dc *topology.DeployConfig) (*task.Task, error) {
	chunkserverId := curveadm.MemStorage().Get(comm.DISK_CHUNKSERVER_ID).(string)
	diskDevPath := curveadm.MemStorage().Get(comm.DISK_FILTER_DEVICE).(string)
	subname := fmt.Sprintf("host=%s device=%s chunkserverId=%s",
		dc.GetHost(), diskDevPath, chunkserverId)
	hc, err := curveadm.GetHost(dc.GetHost())
	if err != nil {
		return nil, err
	}

	// new task
	t := task.NewTask("Replace Chunkserver Disk", subname, hc.GetSSHConfig())

	t.AddStep(&replaceDisk{
		chunkserverId: chunkserverId,
		diskDevPath:   diskDevPath,
		host:          dc.GetHost(),
		diskData:      curveadm.Disks(),
		curveadm:      curveadm,
	})

	return t, nil
}
