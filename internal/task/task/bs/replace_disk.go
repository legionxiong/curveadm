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
	"github.com/opencurve/curveadm/internal/storage"
	"github.com/opencurve/curveadm/internal/task/context"
	"github.com/opencurve/curveadm/internal/task/step"
	"github.com/opencurve/curveadm/internal/task/task"
)

type replaceDisk struct {
	chunkserverId  string
	newDiskDevPath string
	host           string
	disksData      string
	oldDisk        storage.Disk
	curveadm       *cli.CurveAdm
}

func updateDiskUuid(diskUuid, host, newDiskDevPath string,
	curveadm *cli.CurveAdm, ctx *context.Context) error {
	var success bool
	var tune2fsRet *string
	step := &step.Tune2Filesystem{
		Device:      newDiskDevPath,
		Param:       fmt.Sprintf("-U %s", diskUuid),
		Success:     &success,
		Out:         tune2fsRet,
		ExecOptions: curveadm.ExecOptions(),
	}
	if err := step.Execute(ctx); err != nil {
		return err
	}
	if !success {
		return errno.ERR_TUNE2FS_UPDATE_DISK_UUID_FAILED.
			F("Failed to update the UUID of oldDisk[%s:%s]", host, newDiskDevPath)
	}
	return nil
}

func (s *replaceDisk) Execute(ctx *context.Context) error {
	if len(s.chunkserverId) == 0 {
		return nil
	}

	if s.newDiskDevPath != s.oldDisk.Device {
		if err := updateDiskUuid("random", s.host, s.oldDisk.Device, s.curveadm, ctx); err != nil {
			return err
		}
	}
	if err := disks.UpdateDisks(s.disksData, s.host, s.newDiskDevPath,
		s.oldDisk, s.curveadm); err != nil {
		return err
	}
	return nil
}

func NewReplaceDiskTask(curveadm *cli.CurveAdm, dc *topology.DeployConfig) (*task.Task, error) {
	host := dc.GetHost()
	newDiskDevPath := curveadm.MemStorage().Get(comm.DISK_REPLACEMENT_NEW_DISK_DEVICE).(string)
	// if len(newDiskDevPathSlice) == 0 {
	// 	return nil, errno.ERR_REPLACE_DISK_MISSING_NEW_DISK_DEVICE.
	// 		F("New device for oldDisk replacement was not found")
	// }
	// newDiskDevPath := newDiskDevPathSlice[0]
	diskRecord, err := curveadm.Storage().GetDiskByMountPoint(host, dc.GetDataDir())
	if err != nil {
		return nil, err
	}
	chunkserverId := diskRecord.ChunkServerID
	subname := fmt.Sprintf("host=%s device=%s chunkserverId=%s",
		host, newDiskDevPath, chunkserverId)
	hc, err := curveadm.GetHost(host)
	if err != nil {
		return nil, err
	}

	// new task
	t := task.NewTask("Replace Chunkserver Disk", subname, hc.GetSSHConfig())

	t.AddStep(&replaceDisk{
		chunkserverId:  chunkserverId,
		newDiskDevPath: newDiskDevPath,
		host:           host,
		disksData:      curveadm.Disks(),
		oldDisk:        diskRecord,
		curveadm:       curveadm,
	})

	return t, nil
}
