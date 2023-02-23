/*
 *  Copyright (c) 2021 NetEase Inc.
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

package command

import (
	"github.com/opencurve/curveadm/cli/cli"
	comm "github.com/opencurve/curveadm/internal/common"
	"github.com/opencurve/curveadm/internal/configure"
	"github.com/opencurve/curveadm/internal/configure/topology"
	"github.com/opencurve/curveadm/internal/errno"
	"github.com/opencurve/curveadm/internal/playbook"
	tui "github.com/opencurve/curveadm/internal/tui/common"
	cliutil "github.com/opencurve/curveadm/internal/utils"
	"github.com/spf13/cobra"
)

const (
	REPLACE_DISK_EXAMPLE = `Examples:
  $ curveadm replace-disk --chunkserver-id chunkserver_id --disk /dev/sdX # replace and format disk for given chunkserver`
)

var (
	REPLACE_DISK_PLAYBOOK_STEPS = []int{
		// playbook.STOP_SERVICE,
		playbook.REPLACE_DISK,
		// playbook.FORMAT_CHUNKFILE_POOL,
		// playbook.START_CHUNKSERVER,
	}
)

type replaceDiskOptions struct {
	chunkserverId string
	device        string
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

	return cmd
}

func genReplaceDiskPlaybook(curveadm *cli.CurveAdm, dcs []*topology.DeployConfig,
	options replaceDiskOptions) (*playbook.Playbook, error) {

	steps := REPLACE_DISK_PLAYBOOK_STEPS
	pb := playbook.NewPlaybook(curveadm)
	disks, err := curveadm.Storage().GetDisk("service", options.chunkserverId)

	if err != nil {
		return pb, err
	}
	if len(disks) == 0 {
		return pb, errno.ERR_DISK_NOT_FOUND.
			F("disk with chunkserver id[%s] not found", options.chunkserverId)
	}

	disk := disks[0]

	dcs = curveadm.FilterDeployConfig(dcs, topology.FilterOption{
		Id:   options.chunkserverId,
		Role: configure.ROLE_CHUNKSERVER,
		Host: disk.Host,
	})
	if len(dcs) == 0 {
		return nil, errno.ERR_NO_SERVICES_MATCHED
	}
	// curveadm.MemStorage().Set(comm.DISK_ATTACHED_HOST, disk.Host)
	curveadm.MemStorage().Set(comm.DISK_CHUNKSERVER_ID, options.chunkserverId)
	curveadm.MemStorage().Set(comm.DISK_DEVICE_PATH, options.device)

	for _, step := range steps {
		pb.AddStep(&playbook.PlaybookStep{
			Type:    step,
			Configs: dcs,
		})
	}

	return pb, nil
}

func runReplaceDisk(curveadm *cli.CurveAdm, options replaceDiskOptions) error {
	// check disks not empty
	if len(curveadm.DiskRecords()) == 0 {
		return errno.ERR_EMPTY_DISKS.
			F("replace disk relies on that chunkserver and disk relationship stored in database.")
	}

	// check required options
	if options.chunkserverId == "" {
		return errno.ERR_CHUNKSERVER_ID_IS_REQUIRED
	}

	if options.device == "" {
		return errno.ERR_DISK_PATH_IS_REQUIRED
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
	pass := tui.ConfirmYes(tui.PromptReplaceDisk(options.chunkserverId, options.device))
	if !pass {
		curveadm.WriteOut(tui.PromptCancelOpetation("replace chunkserver disk"))
		return errno.ERR_CANCEL_OPERATION
	}

	// 4) run playground
	return pb.Run()
}
