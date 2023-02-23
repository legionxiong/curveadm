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

package disks

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/opencurve/curveadm/cli/cli"
	"github.com/opencurve/curveadm/internal/build"
	"github.com/opencurve/curveadm/internal/common"
	"github.com/opencurve/curveadm/internal/configure/hosts"
	"github.com/opencurve/curveadm/internal/errno"
	"github.com/opencurve/curveadm/internal/storage"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

type (
	Disks struct {
		Global map[string]interface{}   `mapstructure:"global"`
		Disk   []map[string]interface{} `mapstructure:"disk"`
	}

	DiskConfig struct {
		sequence int
		config   map[string]interface{}
	}
)

func (dc *DiskConfig) Build() error {
	for key, value := range dc.config {
		if itemset.Get(key) == nil {
			return errno.ERR_UNSUPPORT_DISKS_CONFIGURE_ITEM.
				F("disks[%d].%s = %v", dc.sequence, key, value)
		}
		if _, ok := value.([]interface{}); !ok {
			v, err := itemset.Build(key, value)
			if err != nil {
				return err
			} else {
				dc.config[key] = v
			}
		}
	}

	if len(dc.GetDevice()) == 0 {
		return errno.ERR_DEVICE_FIELD_MISSING.
			F("disks[%d].device = nil", dc.sequence)
	} else if len(dc.GetMountPoint()) == 0 {
		return errno.ERR_MOUNT_POINT_FIELD_MISSING.
			F("disks[%d].mount = nil", dc.sequence)
	} else if dc.GetFormatPercent() > 100 {
		return errno.ERR_DISK_FORMAT_PERCENT_EXCEED_100.
			F("disks[%d].format_percent = %d", dc.sequence, dc.GetFormatPercent())
	}

	return nil
}

func NewDiskConfig(sequence int, config map[string]interface{}) *DiskConfig {
	return &DiskConfig{
		sequence: sequence,
		config:   config,
	}
}

func parseDisksData(data string) (*Disks, error) {
	parser := viper.NewWithOptions(viper.KeyDelimiter("::"))
	parser.SetConfigType("yaml")
	err := parser.ReadConfig(bytes.NewBuffer([]byte(data)))
	if err != nil {
		return nil, errno.ERR_PARSE_DISKS_FAILED.E(err)
	}

	disks := &Disks{}
	if err := parser.Unmarshal(disks); err != nil {
		return nil, errno.ERR_PARSE_DISKS_FAILED.E(err)
	}

	return disks, nil
}

func getDiskId(disk storage.Disk) (string, error) {
	uriSlice := strings.Split(disk.URI, "//")
	if len(uriSlice) == 0 {
		return "", errno.ERR_INVALID_DISK_URI.
			F("The disk[%s:%s] URI[%s] is invalid", disk.Host, disk.Device, disk.URI)
	}

	if uriSlice[0] == common.DISK_URI_PROTO_FS_UUID {
		return uriSlice[1], nil
	}
	return "", nil
}

func UpdateDisks(disksData, host, device, chunkserverId, oldDiskId string, curveadm *cli.CurveAdm) error {
	disks, err := parseDisksData(disksData)
	if err != nil {
		return err
	}
	diskRecords, err := curveadm.Storage().GetDisk("device", host, device)
	if err != nil {
		return err
	}
	// var diskId string
	var diskExist bool

	if len(diskRecords) == 0 {
		if serviceContainerId, err := curveadm.Storage().GetContainerId(chunkserverId); err != nil {
			return err
		} else if len(serviceContainerId) == 0 {
			return errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
				F("The chunkserver[ID: %s] was not found", chunkserverId)
		}
		chunkserverDisk, err := curveadm.Storage().GetDisk("service", chunkserverId)
		if err != nil {
			return err
		}
		if len(chunkserverDisk) == 0 {
			return errno.ERR_DATABASE_EMPTY_QUERY_RESULT.
				F("Chunkserver[ID: %s] related disk device was not found", chunkserverId)
		}
		disk := chunkserverDisk[0]

		fmt.Println(disks.Disk)
		for i, d := range disks.Disk {
			dc := NewDiskConfig(i, d)
			if err := dc.Build(); err != nil {
				return err
			}
			// diskDev := disk.Device
			fmt.Println(device, dc.GetDevice())
			if dc.GetDevice() == device {
				hostsOnly := dc.GetHostsOnly()
				diskExist = true
				if len(hostsOnly) > 0 {
					hostsOnly = append(hostsOnly, host)
					hostsOnlyMap := make(map[string][]interface{})
					hostsOnlyMap[common.DISK_ONLY_HOSTS] = hostsOnly
					intSliceElemValue := reflect.ValueOf(&d).Elem()
					newVale := reflect.ValueOf(hostsOnlyMap)
					intSliceElemValue.Set(newVale)
				}
				break
			}
		}

		if !diskExist {
			diskStruct := map[string]interface{}{
				"device":               device,
				"mount":                disk.MountPoint,
				common.DISK_ONLY_HOSTS: []string{host},
			}
			disks.Disk = append(disks.Disk, diskStruct)
			for i, d := range disks.Disk {
				dc := NewDiskConfig(i, d)
				if err := dc.Build(); err != nil {
					return err
				}
				if dc.GetDevice() == device {
					hostsExclude := dc.GetHostsExclude()
					if len(hostsExclude) > 0 {
						hostsExclude = append(hostsExclude, host)
						hostsOnlyMap := make(map[string][]interface{})
						hostsOnlyMap[common.DISK_ONLY_HOSTS] = hostsExclude
						intSliceElemValue := reflect.ValueOf(&d).Elem()
						newVale := reflect.ValueOf(hostsOnlyMap)
						intSliceElemValue.Set(newVale)
					} else {
						diskStruct := map[string]interface{}{
							"device":                  device,
							"mount":                   disk.MountPoint,
							common.DISK_EXCLUDE_HOSTS: []string{host},
						}
						disks.Disk = append(disks.Disk, diskStruct)
						// add new disk record
						if err := curveadm.Storage().SetDisk(disk.Host, device, disk.MountPoint,
							disk.ContainerImageLocation, disk.FormatPercent); err != nil {
							return err
						}
						// remove old disk record
						if err := curveadm.Storage().DeleteDisk(disk.Host, disk.Device); err != nil {
							return err
						}
					}
					break
				}
			}
		}
	} else {
		disk := diskRecords[0]
		// check if disk used by other chunkserver
		if disk.ChunkServerID != chunkserverId {
			return errno.ERR_REPLACE_DISK_USED_BY_OTHER_CHUNKSERVER.
				F("The disk[%s:%s] is being used by chunkserver %s",
					disk.Host, disk.Device, disk.ChunkServerID)
		}

		// check if the same disk
		if diskId, err := getDiskId(disk); err != nil {
			return err
		} else if diskId == oldDiskId {
			return errno.ERR_REPLACE_THE_SAME_PHYSICAL_DISK.
				F("The new disk[UUID:%s] and the origin disk[UUID:%s] are the same", diskId, oldDiskId)
		}
	}
	fmt.Println(disks.Disk)
	diskm := Disks{disks.Global, disks.Disk}

	data, err := yaml.Marshal(diskm)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	if err := curveadm.Storage().SetDisks(string(data)); err != nil {
		return err
	}

	return nil

}

func ParseDisks(data string) ([]*DiskConfig, error) {
	disks, err := parseDisksData(data)
	if err != nil {
		return nil, err
	}

	dcs := []*DiskConfig{}
	exist := map[string]bool{}
	for i, disk := range disks.Disk {
		disk = hosts.NewIfNil(disk)
		hosts.Merge(disks.Global, disk)
		dc := NewDiskConfig(i, disk)
		err = dc.Build()
		if err != nil {
			return nil, err
		}

		if _, ok := exist[dc.GetDevice()]; ok {
			return nil, errno.ERR_DUPLICATE_DISK.
				F("duplicate disk: %s", dc.GetDevice())
		}
		if _, ok := exist[dc.GetMountPoint()]; ok {
			return nil, errno.ERR_DUPLICATE_DISK_MOUNT_POINT.
				F("duplicate disk mount point: %s", dc.GetMountPoint())
		}
		hostsExclude := dc.GetHostsExclude()
		hostsOnly := dc.GetHostsOnly()
		if len(hostsExclude) > 0 && len(hostsOnly) > 0 {
			return nil, errno.ERR_ONLY_EXCLUDE_HOSTS.
				F("conflict fields hosts_exclude: %s and hosts_only: %s", hostsExclude, hostsOnly)
		}
		dcs = append(dcs, dc)
		exist[dc.GetDevice()] = true
		exist[dc.GetMountPoint()] = true
	}
	build.DEBUG(build.DEBUG_DISKS, disks)
	return dcs, nil
}
