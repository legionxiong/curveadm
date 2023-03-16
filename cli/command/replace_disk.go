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

package command

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/opencurve/curveadm/cli/cli"
	comm "github.com/opencurve/curveadm/internal/common"
	"github.com/opencurve/curveadm/internal/configure"
	"github.com/opencurve/curveadm/internal/configure/disks"
	"github.com/opencurve/curveadm/internal/configure/topology"
	"github.com/opencurve/curveadm/internal/errno"
	"github.com/opencurve/curveadm/internal/playbook"
	"github.com/opencurve/curveadm/internal/storage"
	"github.com/opencurve/curveadm/internal/task/task/bs"
	"github.com/opencurve/curveadm/internal/tui"
	tuicomm "github.com/opencurve/curveadm/internal/tui/common"
	cliutil "github.com/opencurve/curveadm/internal/utils"
	"github.com/spf13/cobra"
)

const (
	REPLACE_DISK_EXAMPLE = `Examples:
  $ curveadm replace-disk --chunkserver-id chunkserverId --disk /dev/sdx # replace and format disk for given chunkserver
  $ curveadm replace-disk --chunkserver-id chunkserverId --stop # stop replacing disk
  $ curveadm replace-disk --chunkserver-id chunkserverId --status # show the status of disk replacement for given chunkserver
  $ curveadm replace-disk --chunkserver-id chunkserverId --start-service  # start given chunkserver
  $ curveadm replace-disk --status # show all disk replacement status`
)

var (
	REPLACE_DISK_PLAYBOOK_STEPS = []int{
		playbook.STOP_SERVICE,
		playbook.CHECK_DISK_REPLACEMENT,
		playbook.FORMAT_CHUNKFILE_POOL,
		// playbook.REPLACE_DISK,
	}

	REPLACE_DISK_STOP_PLAYBOOK_STEPS = []int{
		playbook.STOP_FORMAT,
		playbook.STOP_DISK_REPLACEMENT,
	}

	REPLACE_DISK_STATUS_PLAYBOOK_STEPS = []int{
		playbook.GET_FORMAT_STATUS,
	}
)

type replaceDiskOptions struct {
	chunkserverId         string
	device                string
	stopDiskReplacement   bool
	diskReplacementStatus bool
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
	flags.BoolVar(&options.diskReplacementStatus, "status", false, "Stop disk replacing progress")

	return cmd
}

func getChunkServerDisk(curveadm *cli.CurveAdm, chunkserverId string, dcs []*topology.DeployConfig) (
	storage.Disk, []*topology.DeployConfig, error) {
	var disk storage.Disk
	var dcsFiltered []*topology.DeployConfig
	disks, err := curveadm.Storage().GetDisk(comm.DISK_FILTER_SERVICE, chunkserverId)
	if err != nil {
		return disk, dcsFiltered, err
	}
	if len(disks) == 0 {
		return disk, dcsFiltered, errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
			F("The chunkserver[ID: %s] has not related disk device", chunkserverId)
	}
	dcsFiltered = curveadm.FilterDeployConfig(dcs, topology.FilterOption{
		Id:   chunkserverId,
		Role: configure.ROLE_CHUNKSERVER,
		Host: disks[0].Host,
	})
	if len(dcsFiltered) == 0 {
		return disk, dcsFiltered, errno.ERR_NO_SERVICES_MATCHED.
			F("chunkserver id: %s", chunkserverId)
	}
	return disks[0], dcsFiltered, nil
}

func genReplaceDiskPlaybook(curveadm *cli.CurveAdm, dcs []*topology.DeployConfig,
	options replaceDiskOptions) (*playbook.Playbook, error) {
	var fcs []*configure.FormatConfig

	chunkserverId := options.chunkserverId
	steps := REPLACE_DISK_PLAYBOOK_STEPS
	if options.stopDiskReplacement {
		steps = REPLACE_DISK_STOP_PLAYBOOK_STEPS
	}

	curveadm.MemStorage().Set(comm.DISK_CHUNKSERVER_ID, chunkserverId)
	curveadm.MemStorage().Set(comm.DISK_FILTER_DEVICE, options.device)

	pb := playbook.NewPlaybook(curveadm)
	if options.diskReplacementStatus {
		replacements, err := curveadm.Storage().GetDiskReplacement(
			comm.DISK_REPLACEMENT_FILTER_ALL)
		if err != nil {
			return pb, err
		}
		for _, rps := range replacements {

			disk, _, err := getChunkServerDisk(curveadm, rps.ChunkServerID, dcs)
			if err != nil {
				return pb, err
			}
			diskFormatInfo := fmt.Sprintf("%s:%s:%d", rps.Device,
				disk.MountPoint, disk.FormatPercent)
			fc, err := configure.NewFormatConfig(disk.ContainerImage,
				disk.Host, diskFormatInfo)
			if err != nil {
				return pb, err
			}

			fc.FromDiskRecord = true
			fcs = append(fcs, fc)
			pb.AddStep(&playbook.PlaybookStep{
				Type:    playbook.GET_FORMAT_STATUS,
				Configs: fcs,
			})
		}

	} else {
		disk, dcsFiltered, err := getChunkServerDisk(curveadm, chunkserverId, dcs)
		if err != nil {
			return pb, err
		}

		for _, step := range steps {
			if step == playbook.FORMAT_CHUNKFILE_POOL {
				diskFormatInfo := fmt.Sprintf("%s:%s:%d", options.device,
					disk.MountPoint, disk.FormatPercent)
				fc, err := configure.NewFormatConfig(disk.ContainerImage,
					disk.Host, diskFormatInfo)
				if err != nil {
					return pb, err
				}

				fc.FromDiskRecord = true
				fcs = append(fcs, fc)
				pb.AddStep(&playbook.PlaybookStep{
					Type:    step,
					Configs: fcs,
				})
			} else {
				pb.AddStep(&playbook.PlaybookStep{
					Type:    step,
					Configs: dcsFiltered,
				})
			}
		}
	}

	return pb, nil
}

func updateDiskReplacementProgressStatus(curveadm *cli.CurveAdm,
	options replaceDiskOptions) error {
	var formatPercent string
	var currentFormat, targetFormt int
	curveadmStorage := curveadm.Storage()
	formatStatus := curveadm.MemStorage().Get(comm.KEY_ALL_FORMAT_STATUS)
	if formatStatus == nil {
		return nil
	}
	formatStatusMap := formatStatus.(map[string]bs.FormatStatus)
	for _, status := range formatStatusMap {
		formated := strings.Split(status.Formatted, "/")
		fmt.Println(formated, status.Formatted)
		if len(formated) != 2 {
			continue
		}
		currentFormat, _ = strconv.Atoi(formated[0])
		targetFormt, _ = strconv.Atoi(formated[1])
		percent, _ := strconv.ParseInt(fmt.Sprintf("%.2f",
			float64(currentFormat)/float64(targetFormt)*100), 10, 64)
		if percent >= 100 || status.Status == comm.DISK_REPLACEMENT_STATUS_DONE {
			percent = 100
		}
		diskReplacements, err := curveadmStorage.GetDiskReplacement(
			comm.DISK_FILTER_DEVICE, status.Host, status.Device)
		if err != nil {
			return err
		}
		fmt.Println(diskReplacements)
		formatPercent = fmt.Sprintf("%d", percent)
		if len(diskReplacements) > 0 {
			dr := diskReplacements[0]
			if dr.Status == comm.DISK_REPLACEMENT_STATUS_DONE {
				continue
			}
			if formatPercent == "100" {
				if err := curveadm.Storage().UpdateDiskReplacementStatus(
					comm.DISK_REPLACEMENT_STATUS_DONE, dr.ChunkServerID); err != nil {
					return err
				}
			}
			if err := curveadm.Storage().UpdateDiskReplacementProgress(
				formatPercent, dr.ChunkServerID); err != nil {
				return err
			}
		}
	}

	return nil
}

func runReplaceDisk(curveadm *cli.CurveAdm, options replaceDiskOptions) error {
	var err error
	var diskReplacements []storage.DiskReplacement
	curveadmStorage := curveadm.Storage()
	// check disks not empty
	if len(curveadm.DiskRecords()) == 0 {
		return errno.ERR_EMPTY_DISKS.F("disks are empty")
	}

	// replace one disk at a time
	if diskReplacements, err = curveadmStorage.GetDiskReplacement(
		comm.DISK_REPLACEMENT_FILTER_STATUS,
		comm.DISK_REPLACEMENT_STATUS_RUNNING); err != nil {
		return err
	}
	for _, dr := range diskReplacements {
		if !options.diskReplacementStatus && dr.ChunkServerID != options.chunkserverId {
			return errno.ERR_REPLACE_DISK_TOO_MANY.
				F("There is already a running disk replacement task, replace one disk at a time")
		}
	}

	// check required options
	if options.chunkserverId == "" && (options.stopDiskReplacement || options.device != "") {
		return errno.ERR_CHUNKSERVER_ID_IS_REQUIRED
	}

	if !options.stopDiskReplacement && !options.diskReplacementStatus {
		if options.device == "" {
			return errno.ERR_DISK_PATH_IS_REQUIRED
		}
	}

	var disk storage.Disk
	if options.chunkserverId != "" {
		diskRecords, err := curveadm.Storage().GetDisk(comm.DISK_FILTER_SERVICE, options.chunkserverId)
		if err != nil {
			return err
		}
		if len(diskRecords) == 0 {
			return errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
				F("The chunkserver[ID: %s] currently has not disk device",
					options.chunkserverId)
		}

		disk = diskRecords[0]
		formerDiskId, _, err := disks.GetDiskId(disk)

		if err != nil {
			return err
		}
		// add disk replacement record
		if options.device != "" {
			if err := curveadm.Storage().SetDiskReplacement(
				disk.Host,
				options.device,
				formerDiskId,
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
	if !options.diskReplacementStatus {
		pass := tuicomm.ConfirmYes(tuicomm.PromptReplaceDisk(options.chunkserverId,
			topology.ROLE_CHUNKSERVER, disk.Host, options.device))
		if !pass {
			curveadm.WriteOut(tuicomm.PromptCancelOpetation("replace chunkserver disk"))
			return errno.ERR_CANCEL_OPERATION
		}
	}

	// 4) run playground
	if err := pb.Run(); err != nil {
		return err
	}

	// show disk replacing status
	if options.diskReplacementStatus {
		if err := updateDiskReplacementProgressStatus(curveadm, options); err != nil {
			return err
		}
		if diskReplacements, err = curveadmStorage.GetDiskReplacement(
			comm.DISK_REPLACEMENT_FILTER_ALL); err != nil {
			return err
		}
		if options.chunkserverId != "" {
			if diskReplacements, err = curveadmStorage.GetDiskReplacement(
				comm.DISK_FILTER_SERVICE, options.chunkserverId); err != nil {
				return err
			}
		}
		output := tui.FormatDiskReplacements(diskReplacements)
		curveadm.WriteOut(output)
	}
	return nil
}
