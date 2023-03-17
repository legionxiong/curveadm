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
  $ curveadm replace-disk --chunkserver-id chunkserverId --disk /dev/sdX # replace and format disk for given chunkserver
  $ curveadm replace-disk --chunkserver-id chunkserverId --stop # stop replacing disk
  $ curveadm replace-disk --chunkserver-id chunkserverId --status # show the status of disk replacement for given chunkserver
  $ curveadm replace-disk --chunkserver-id chunkserverId --start-service  # start given chunkserver
  $ curveadm replace-disk --status # show all disk replacement status`
)

var (
	REPLACE_DISK_PLAYBOOK_STEPS = []int{
		playbook.CHECK_DISK_REPLACEMENT,
		playbook.STOP_SERVICE,
		playbook.FORMAT_CHUNKFILE_POOL,
		playbook.REPLACE_DISK,
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
	options replaceDiskOptions, formerDiskId string) (*playbook.Playbook, error) {
	var fcs []*configure.FormatConfig

	chunkserverId := options.chunkserverId
	steps := REPLACE_DISK_PLAYBOOK_STEPS
	if options.stopDiskReplacement {
		steps = REPLACE_DISK_STOP_PLAYBOOK_STEPS
	}

	pb := playbook.NewPlaybook(curveadm)
	// display disk replacing status
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
		}
		pb.AddStep(&playbook.PlaybookStep{
			Type:    playbook.GET_FORMAT_STATUS,
			Configs: fcs,
			ExecOptions: playbook.ExecOptions{
				SilentSubBar:  true,
				SilentMainBar: true,
				SkipError:     false,
			},
		})

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
					Options: map[string]interface{}{
						comm.DISK_REPLACEMENT_FORMER_DISK_UUID: formerDiskId,
					},
					ExecOptions: playbook.ExecOptions{
						SilentSubBar:  true,
						SilentMainBar: true,
						SkipError:     false,
					},
				})
			} else {
				pb.AddStep(&playbook.PlaybookStep{
					Type:    step,
					Configs: dcsFiltered,
					Options: map[string]interface{}{
						comm.DISK_REPLACEMENT_NEW_DISK_DEVICE: options.device,
					},
				})
			}
		}
	}

	return pb, nil
}

func updateDiskReplacementProgressStatus(curveadm *cli.CurveAdm,
	options replaceDiskOptions) error {
	curveadmStorage := curveadm.Storage()
	formatStatus := curveadm.MemStorage().Get(comm.KEY_ALL_FORMAT_STATUS)
	if formatStatus == nil {
		return nil
	}
	formatStatusMap := formatStatus.(map[string]bs.FormatStatus)
	for _, status := range formatStatusMap {
		formated := strings.Split(status.Formatted, "/")
		var percent int64
		if len(formated) == 2 {
			currentFormat, _ := strconv.Atoi(formated[0])
			targetFormt, _ := strconv.Atoi(formated[1])
			percent, _ = strconv.ParseInt(fmt.Sprintf("%.2f",
				float64(currentFormat)/float64(targetFormt)*100), 10, 10)
		} else {
			percent = 0
		}
		if percent >= 100 || status.Status == comm.DISK_REPLACEMENT_STATUS_DONE {
			percent = 100
		}
		diskReplacements, err := curveadmStorage.GetDiskReplacement(
			comm.DISK_FILTER_DEVICE, status.Host, status.Device)
		if err != nil {
			return err
		}

		if len(diskReplacements) > 0 {
			drp := diskReplacements[0]
			if err := curveadm.Storage().UpdateDiskReplacementProgress(
				fmt.Sprintf("%d", percent), drp.ChunkServerID); err != nil {
				return err
			}
			if percent == 100 {
				// status: Done
				if drp.Status == comm.DISK_REPLACEMENT_STATUS_DONE {
					continue
				}
				if err := curveadm.Storage().UpdateDiskReplacementStatus(
					comm.DISK_REPLACEMENT_STATUS_DONE, drp.ChunkServerID); err != nil {
					return err
				}
			} else {
				// status: Running
				if err := curveadm.Storage().UpdateDiskReplacementStatus(
					comm.DISK_REPLACEMENT_STATUS_RUNNING, drp.ChunkServerID); err != nil {
					return err
				}
			}

		}
	}

	return nil
}

func checkRunningDiskReplacement(curveadm *cli.CurveAdm,
	options replaceDiskOptions) (chunkserverReplacedDisk bool, err error) {
	var diskReplacements []storage.DiskReplacement
	if diskReplacements, err = curveadm.Storage().GetDiskReplacement(
		comm.DISK_REPLACEMENT_FILTER_STATUS,
		comm.DISK_REPLACEMENT_STATUS_RUNNING); err != nil {
		return false, err
	}
	if !options.diskReplacementStatus {
		for _, drp := range diskReplacements {
			if drp.ChunkServerID != options.chunkserverId {
				return false, errno.ERR_REPLACE_DISK_TOO_MANY.
					F("There is already a running disk replacement task, replace one disk at a time")
			} else {
				chunkserverReplacedDisk = true
			}
		}
	}
	return chunkserverReplacedDisk, nil
}

func checkRequiredOptions(options replaceDiskOptions) error {
	if options.chunkserverId == "" && (options.stopDiskReplacement || options.device != "") {
		return errno.ERR_CHUNKSERVER_ID_IS_REQUIRED
	}

	if !options.stopDiskReplacement && !options.diskReplacementStatus {
		if options.device == "" {
			return errno.ERR_DISK_PATH_IS_REQUIRED
		}
	}
	return nil
}

func getOldDisk(curveadm *cli.CurveAdm, options replaceDiskOptions,
	chunkserverReplacedDisk bool) (storage.Disk, error) {
	oldDisk := storage.Disk{}
	if options.chunkserverId != "" {
		diskRecords, err := curveadm.Storage().GetDisk(comm.DISK_FILTER_SERVICE, options.chunkserverId)
		if err != nil {
			return oldDisk, err
		}
		if len(diskRecords) == 0 {
			return oldDisk, errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
				F("The chunkserver[ID: %s] currently has not disk device",
					options.chunkserverId)
		}
		oldDisk = diskRecords[0]

		// add disk replacement record
		// if !chunkserverReplacedDisk && options.device != "" {
		// 	if err := curveadm.Storage().SetDiskReplacement(
		// 		oldDisk.Host,
		// 		options.device,
		// 		formerDiskId,
		// 		oldDisk.ChunkServerID,
		// 	); err != nil {
		// 		return oldDisk, "", err
		// 	}
		// }
	}
	return oldDisk, nil
}

func addDiskAndReplacementRecords(curveadm *cli.CurveAdm, oldDisk storage.Disk,
	options replaceDiskOptions, chunkserverReplacedDisk bool, formerDiskId string) error {
	// add disk replacement record
	if options.device != "" {
		if !chunkserverReplacedDisk {
			if err := curveadm.Storage().SetDiskReplacement(
				oldDisk.Host,
				options.device,
				formerDiskId,
				oldDisk.ChunkServerID,
			); err != nil {
				return err
			}
		}
		// add new disk record
		if options.device != oldDisk.Device {
			if err := curveadm.Storage().SetDisk(
				oldDisk.Host,
				options.device,
				oldDisk.MountPoint,
				oldDisk.ContainerImage,
				oldDisk.ChunkServerID,
				oldDisk.FormatPercent,
				oldDisk.ServiceMountDevice); err != nil {
				return err
			}
		}
	}
	return nil
}

func runReplaceDisk(curveadm *cli.CurveAdm, options replaceDiskOptions) error {
	var err error

	curveadmStorage := curveadm.Storage()
	// 1) check disks not empty
	if len(curveadm.DiskRecords()) == 0 {
		return errno.ERR_EMPTY_DISKS.F("disks are empty")
	}

	// 2) check required options
	if err := checkRequiredOptions(options); err != nil {
		return nil
	}

	// 3) check running disks replacemanet
	chunkserverReplacedDisk, err := checkRunningDiskReplacement(curveadm, options)
	if err != nil {
		return err
	}

	// 3) get old disk
	oldDisk, err := getOldDisk(curveadm, options, chunkserverReplacedDisk)
	if err != nil {
		return err
	}
	oldDiskUuid, _, err := disks.GetDiskId(oldDisk)
	if err != nil {
		return err
	}

	// 4) add disk / replacement record
	if err := addDiskAndReplacementRecords(curveadm, oldDisk, options,
		chunkserverReplacedDisk, oldDiskUuid); err != nil {
		return err
	}

	dcs, err := curveadm.ParseTopology()
	if err != nil {
		return err
	}

	// 4) generate disk replacement playbook
	pb, err := genReplaceDiskPlaybook(curveadm, dcs, options, oldDiskUuid)
	if err != nil {
		return err
	}

	// 5) confirm by user
	if !options.diskReplacementStatus {
		pass := tuicomm.ConfirmYes(tuicomm.PromptReplaceDisk(options.chunkserverId,
			topology.ROLE_CHUNKSERVER, oldDisk.Host, options.device))
		if !pass {
			curveadm.WriteOut(tuicomm.PromptCancelOpetation("replace chunkserver disk"))
			return errno.ERR_CANCEL_OPERATION
		}
	}

	// 6) run playground
	if err := pb.Run(); err != nil {
		return err
	}

	// 7) show disk replacing status
	var diskReplacements []storage.DiskReplacement
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
