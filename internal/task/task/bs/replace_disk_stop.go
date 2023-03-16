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
	"github.com/opencurve/curveadm/internal/task/task"
)

type restoreDisk struct {
	chunkserverId string
	diskDevPath   string
	host          string
	diskData      string
	curveadm      *cli.CurveAdm
}

func (s *restoreDisk) Execute(ctx *context.Context) error {
	if len(s.chunkserverId) == 0 {
		return nil
	}

	// give a random uuid for the new disk when stop disk replacement
	if err := updateDiskUuid("random", s.host, s.diskDevPath, s.curveadm, ctx); err != nil {
		return err
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

	if err := disks.UpdateDisks(s.diskData, s.host, s.diskDevPath,
		diskRecords[0], s.curveadm); err != nil {
		return err
	}
	return nil
}

func NewStopDiskReplacementTask(curveadm *cli.CurveAdm, dc *topology.DeployConfig) (*task.Task, error) {
	chunkserverId := curveadm.MemStorage().Get(comm.DISK_CHUNKSERVER_ID).(string)
	diskReplacements, err := curveadm.Storage().GetDiskReplacement(
		comm.DISK_REPLACEMENT_FILTER_SERVICE, chunkserverId)
	if err != nil {
		return nil, err
	}
	if len(diskReplacements) == 0 {
		return nil, errno.ERR_REPLACE_DISK_NO_SUCH_REPLACEMENT.
			F("Disk replacement for chunkserver[ID: %s] does not exist", chunkserverId)
	}
	diskDevPath := diskReplacements[0].Device
	subname := fmt.Sprintf("host=%s device=%s chunkserverId=%s",
		dc.GetHost(), diskDevPath, chunkserverId)
	hc, err := curveadm.GetHost(dc.GetHost())
	if err != nil {
		return nil, err
	}
	// new task
	t := task.NewTask("Reset Chunkserver Disk Replacement", subname, hc.GetSSHConfig())

	t.AddStep(&restoreDisk{
		chunkserverId: chunkserverId,
		diskDevPath:   diskDevPath,
		host:          dc.GetHost(),
		diskData:      curveadm.Disks(),
		curveadm:      curveadm,
	})

	return t, nil
}
