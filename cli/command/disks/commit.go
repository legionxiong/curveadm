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

package disks

import (
	"github.com/fatih/color"
	"github.com/opencurve/curveadm/cli/cli"
	"github.com/opencurve/curveadm/internal/configure/disks"
	"github.com/opencurve/curveadm/internal/configure/hosts"
	"github.com/opencurve/curveadm/internal/errno"
	tui "github.com/opencurve/curveadm/internal/tui/common"
	"github.com/opencurve/curveadm/internal/utils"
	"github.com/spf13/cobra"
)

const (
	COMMIT_EXAMPLE = `Examples:
  $ curveadm disks commit /path/to/disks.yaml  # Commit disks`
)

type commitOptions struct {
	filename string
	slient   bool
}

func NewCommitCommand(curveadm *cli.CurveAdm) *cobra.Command {
	var options commitOptions

	cmd := &cobra.Command{
		Use:     "commit DISKS [OPTIONS]",
		Short:   "Commit disks",
		Args:    utils.ExactArgs(1),
		Example: COMMIT_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.filename = args[0]
			return runCommit(curveadm, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.BoolVarP(&options.slient, "slient", "s", false, "Slient output for disks commit")

	return cmd
}

func readAndCheckDisks(curveadm *cli.CurveAdm, options commitOptions) (string, error) {
	// 1) read disks from file
	if !utils.PathExist(options.filename) {
		return "", errno.ERR_DISKS_FILE_NOT_FOUND.
			F("%s: no such file", utils.AbsPath(options.filename))
	}
	data, err := utils.ReadFile(options.filename)
	if err != nil {
		return data, errno.ERR_READ_DISKS_FILE_FAILED.E(err)
	}

	// 2) display difference
	oldData := curveadm.Disks()
	if !options.slient {
		diff := utils.Diff(oldData, data)
		curveadm.WriteOutln(diff)
	}

	// 3) check disks data
	_, err = disks.ParseDisks(data)
	return data, err
}

func updateDisk(data string, curveadm *cli.CurveAdm) error {
	var err error
	storage := curveadm.Storage()
	// hosts_data := curveadm.Hosts()
	hcs, err := hosts.ParseHosts(curveadm.Hosts())
	if err != nil {
		return err
	}
	dcs, err := disks.ParseDisks(data)
	if err != nil {
		return err
	}

	var toRecord bool
	diskList := []map[string]string{}
	for _, hc := range hcs {
		for _, dc := range dcs {
			toRecord = true
			host := hc.GetHost()
			device := dc.GetDevice()
			diskOnlyHosts := dc.GetHostsOnly()
			diskExcludeHosts := dc.GetHostsExclude()
			diskMap := map[string]string{}
			if len(diskOnlyHosts) > 0 {
				for _, h := range diskOnlyHosts {
					if host != h {
						toRecord = false
						break
					}
				}
			}
			if len(diskExcludeHosts) > 0 {
				for _, h := range diskExcludeHosts {
					if host == h {
						toRecord = false
						break
					}
				}
			}
			if !toRecord {
				continue
			}
			diskMap[host] = device
			diskList = append(diskList, diskMap)
			err := storage.SetDisk(host, device, dc.GetMountPoint(),
				dc.GetContainerImage(), dc.GetFormatPercent())
			if err != nil {
				return err
			}
		}
	}

	// remove obsolete records in "disk" table
	diskRecords := curveadm.DiskRecords()

	if len(diskRecords) != len(diskList) {
		for _, dr := range diskRecords {
			var hostExists, devExists bool
			for _, dmap := range diskList {
				_, hostExists = dmap[dr.Host]
				_, devExists = dmap[dr.Device]
			}
			// the disk record with nonempty chunkserver id should not be deleted
			if len(dr.ChunkServerID) > 1 {
				continue
			}
			if !hostExists {
				err = storage.DeleteDisk(dr.Host, "")
			} else if !devExists {
				err = storage.DeleteDisk(dr.Host, dr.Device)
			}
			if err != nil {
				return errno.ERR_UPDATE_DISK_FAILED.E(err)
			}
		}
	}

	return err
}

func runCommit(curveadm *cli.CurveAdm, options commitOptions) error {
	// hosts should be committed first before committing disks
	hosts := curveadm.Hosts()
	if len(hosts) == 0 {
		return errno.ERR_EMPTY_HOSTS
	}
	// 1) read and check disks
	data, err := readAndCheckDisks(curveadm, options)
	if err != nil {
		return err
	}

	// 2) confirm by user
	pass := tui.ConfirmYes("Do you want to continue?")
	if !pass {
		curveadm.WriteOut(tui.PromptCancelOpetation("commit disks"))
		return errno.ERR_CANCEL_OPERATION
	}

	// 3) add disks data
	err = curveadm.Storage().SetDisks(data)
	if err != nil {
		return errno.ERR_UPDATE_DISKS_FAILED.E(err)
	}

	// 4) add single disk record
	err = updateDisk(data, curveadm)
	if err != nil {
		return errno.ERR_UPDATE_DISK_FAILED.E(err)
	}

	// 5) print success prompt
	curveadm.WriteOutln(color.GreenString("Disks updated"))
	return nil
}
