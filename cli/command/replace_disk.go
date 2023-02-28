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
 * Created Date: 2023-02-28
 * Author: Lijin Xiong (lijin.xiong@zstack.io)
 */

package command

import (
	"github.com/opencurve/curveadm/cli/cli"
	comm "github.com/opencurve/curveadm/internal/common"
	"github.com/opencurve/curveadm/internal/configure"
	"github.com/opencurve/curveadm/internal/configure/disks"
	"github.com/opencurve/curveadm/internal/configure/topology"
	"github.com/opencurve/curveadm/internal/errno"
	"github.com/opencurve/curveadm/internal/playbook"
	"github.com/opencurve/curveadm/internal/storage"
	"github.com/opencurve/curveadm/internal/tui"
	tuicomm "github.com/opencurve/curveadm/internal/tui/common"
	cliutil "github.com/opencurve/curveadm/internal/utils"
	"github.com/spf13/cobra"
)

const (
	REPLACE_DISK_EXAMPLE = `Examples:
  $ curveadm replace-disk --chunkserver-id chunkserver_id --disk /dev/sdx # replace and format disk for given chunkserver
  $ curveadm replace-disk --chunkserver-id chunkserver_id --status # show the status of disk replacement for chunkserver
  $ curveadm replace-disk --chunkserver-id chunkserver_id --stop   # stop disk replacement of chunkserver
  $ curveadm replace-disk --status # show status of all disk replacements`
)

var (
	REPLACE_DISK_PLAYBOOK_STEPS = []int{
		// playbook.STOP_SERVICE,
		playbook.CHECK_DISK_REPLACEMENT,
		// playbook.FORMAT_CHUNKFILE_POOL,
		// playbook.REPLACE_DISK,
		// playbook.START_CHUNKSERVER,
	}
)

type replaceDiskOptions struct {
	chunkserverId       string
	device              string
	stopDiskReplacement bool
	status              bool
}

func NewReplaceDiskCommand(curveadm *cli.CurveAdm) *cobra.Command {
	var options replaceDiskOptions

	cmd := &cobra.Command{
		Use:     "replace-disk [OPTIONS]",
		Short:   "Replace disk for chunkserver",
		Args:    cliutil.NoArgs,
		Example: REPLACE_DISK_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReplaceDisk(curveadm, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.StringVarP(&options.chunkserverId, "chunkserver-id", "c", "", "Specify chunkserver id")
	flags.StringVarP(&options.device, "device", "d", "", "Specify disk device path")
	flags.BoolVar(&options.stopDiskReplacement, "stop", false, "Stop disk replacing progress")
	flags.BoolVar(&options.status, "status", false, "Stop disk replacing progress")

	return cmd
}

func genReplaceDiskPlaybook(curveadm *cli.CurveAdm, dcs []*topology.DeployConfig,
	options replaceDiskOptions) (*playbook.Playbook, error) {

	steps := REPLACE_DISK_PLAYBOOK_STEPS
	if options.stopDiskReplacement {
		steps = FORMAT_STOP_PLAYBOOK_STEPS
	}

	pb := playbook.NewPlaybook(curveadm)

	if containerId, err := curveadm.Storage().GetContainerId(options.chunkserverId); err != nil {
		return pb, err
	} else if len(containerId) == 0 {
		return pb, errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
			F("The chunkserver[ID: %s] was not found or it has no related container", options.chunkserverId)
	}
	disks, err := curveadm.Storage().GetDisk("service", options.chunkserverId)
	if err != nil {
		return pb, err
	}

	dcs = curveadm.FilterDeployConfig(dcs, topology.FilterOption{
		Id:   options.chunkserverId,
		Role: configure.ROLE_CHUNKSERVER,
		Host: disks[0].Host,
	})
	if len(dcs) == 0 {
		return nil, errno.ERR_NO_SERVICES_MATCHED
	}

	curveadm.MemStorage().Set(comm.DISK_CHUNKSERVER_ID, options.chunkserverId)
	curveadm.MemStorage().Set(comm.DISK_QUERY_DEVICE, options.device)

	for _, step := range steps {
		pb.AddStep(&playbook.PlaybookStep{
			Type:    step,
			Configs: dcs,
		})
	}

	return pb, nil
}

func runReplaceDisk(curveadm *cli.CurveAdm, options replaceDiskOptions) error {
	var err error
	var diskReplacements []storage.DiskReplacement
	curveadmStorage := curveadm.Storage()
	// check disks not empty
	if len(curveadm.DiskRecords()) == 0 {
		return errno.ERR_EMPTY_DISKS.
			F("disks are empty")
	}

	// replace one disk each time
	if diskReplacements, err = curveadmStorage.GetDiskReplacement(comm.DISK_REPLACEMENT_QUERY_STATUS,
		comm.DISK_REPLACEMENT_STATUS_RUNNING); err != nil {
		return err
	}

	if len(diskReplacements) > 0 {
		return errno.ERR_REPLACE_DISK_TOO_MANY.
			F("There is already a running disk replacement task, replace one disk at a time")
	}

	// show disk replacing status
	if options.status {
		if diskReplacements, err = curveadmStorage.GetDiskReplacement(
			comm.DISK_REPLACEMENT_QUERY_ALL); err != nil {
			return err
		}
		if options.chunkserverId != "" {
			if diskReplacements, err = curveadmStorage.GetDiskReplacement(
				comm.DISK_REPLACEMENT_QUERY_SERVICE, options.chunkserverId); err != nil {
				return err
			}
		}
		output := tui.FormatDiskReplacements(diskReplacements)
		curveadm.WriteOut(output)
		return nil
	}

	// check required options
	if options.chunkserverId == "" && (options.stopDiskReplacement || options.device != "") {
		return errno.ERR_CHUNKSERVER_ID_IS_REQUIRED
	}

	if !options.stopDiskReplacement && !options.status {
		if options.device == "" {
			return errno.ERR_DISK_PATH_IS_REQUIRED
		}
	}

	var disk storage.Disk
	if options.chunkserverId != "" {
		if disks, err := curveadmStorage.GetDisk(
			comm.DISK_QUERY_SERVICE,
			options.chunkserverId,
		); err != nil {
			return err
		} else if len(disks) > 0 {
			disk = disks[0]
		}
		diskId, err := disks.GetDiskId(disk)
		if err != nil {
			return err
		}
		// write disk replacement record
		if options.device != "" {
			if err := curveadm.Storage().SetDiskReplacement(
				disk.Host,
				disk.Device,
				diskId,
				disk.ChunkServerID,
			); err != nil {
				return err
			}
		}
	}
	// if err := disks.UpdateDisks(curveadm.Disks(), "curve-1", options.device, options.chunkserverId, curveadm); err != nil {
	// 	return err
	// }
	// return nil
	// 1) parse cluster topology
	dcs, err := curveadm.ParseTopology()
	if err != nil {
		return err
	}

	// 2) generate disk replacement playbook
	pb, err := genReplaceDiskPlaybook(curveadm, dcs, options)
	if err != nil {
		return err
	}

	// 3) confirm by user
	pass := tuicomm.ConfirmYes(tuicomm.PromptReplaceDisk(options.chunkserverId, options.device))
	if !pass {
		curveadm.WriteOut(tuicomm.PromptCancelOpetation("replace chunkserver disk"))
		return errno.ERR_CANCEL_OPERATION
	}

	// 4) run playground
	return pb.Run()
}
