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
 * Created Date: 2023-02-27
 * Author: Lijin Xiong (lijin.xiong@zstack.io)
 */

package bs

import (
	"fmt"

	"github.com/opencurve/curveadm/cli/cli"
	comm "github.com/opencurve/curveadm/internal/common"
	"github.com/opencurve/curveadm/internal/configure/disks"
	"github.com/opencurve/curveadm/internal/configure/topology"
	"github.com/opencurve/curveadm/internal/task/context"
	"github.com/opencurve/curveadm/internal/task/step"
	"github.com/opencurve/curveadm/internal/task/task"
)

type replaceDisk struct {
	chunkserverId string
	diskDevPath   string
	host          string
	diskData      string
	diskUuid      *string
	curveadm      *cli.CurveAdm
}

func (s *replaceDisk) Execute(ctx *context.Context) error {
	if len(s.chunkserverId) == 0 {
		return nil
	}
	if err := disks.UpdateDisks(s.diskData, s.host, s.diskDevPath,
		s.chunkserverId, *s.diskUuid, s.curveadm); err != nil {
		return err
	}
	return nil
}

func NewReplaceDiskTask(curveadm *cli.CurveAdm, dc *topology.DeployConfig) (*task.Task, error) {

	chunkserverId := curveadm.MemStorage().Get(comm.DISK_CHUNKSERVER_ID).(string)
	diskDevPath := curveadm.MemStorage().Get(comm.DISK_QUERY_DEVICE).(string)
	subname := fmt.Sprintf("host=%s device=%s chunkserverId=%s",
		dc.GetHost(), diskDevPath, chunkserverId)
	hc, err := curveadm.GetHost(dc.GetHost())
	if err != nil {
		return nil, err
	}
	// new task
	t := task.NewTask("Replace Chunkserver Disk", subname, hc.GetSSHConfig())

	// var oldContainerId string
	// var oldUUID string
	var diskUuid string

	// 1: stop container
	// t.AddStep(&stopContainer{
	// 	containerId: &oldContainerId,
	// 	curveadm:    curveadm,
	// })
	// 2: get disk UUID
	t.AddStep(&step.ListBlockDevice{
		Device:      []string{diskDevPath},
		Format:      "UUID",
		NoHeadings:  true,
		Out:         &diskUuid,
		ExecOptions: curveadm.ExecOptions(),
	})

	// 2. replace disk
	t.AddStep(&replaceDisk{
		chunkserverId: chunkserverId,
		diskDevPath:   diskDevPath,
		host:          dc.GetHost(),
		diskData:      curveadm.Disks(),
		diskUuid:      &diskUuid,
		curveadm:      curveadm,
	})

	return t, nil
}
