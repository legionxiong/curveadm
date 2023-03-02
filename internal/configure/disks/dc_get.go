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
 * Created Date: 2023-02-24
 * Author: Lijin Xiong (lijin.xiong@zstack.io)
 */

package disks

import (
	comm "github.com/opencurve/curveadm/internal/configure/common"
	"github.com/opencurve/curveadm/internal/utils"
)

func (dc *DiskConfig) get(i *comm.Item) interface{} {
	if v, ok := dc.config[i.Key()]; ok {
		return v
	}

	defaultValue := i.DefaultValue()
	if defaultValue != nil && utils.IsFunc(defaultValue) {
		return defaultValue.(func(*DiskConfig) interface{})(dc)
	}
	return defaultValue
}

func (dc *DiskConfig) getString(i *comm.Item) string {
	v := dc.get(i)
	if v == nil {
		return ""
	}
	return v.(string)
}

func (dc *DiskConfig) getInt(i *comm.Item) int {
	v := dc.get(i)
	if v == nil {
		return 0
	}
	return v.(int)
}

func (dc *DiskConfig) getSlice(i *comm.Item) []string {
	var config []string
	var genericConfig []interface{}
	v := dc.get(i)
	if v == nil {
		return make([]string, 0)
	}
	genericConfig = v.([]interface{})
	for _, c := range genericConfig {
		config = append(config, c.(string))
	}
	return config
}

func (dc *DiskConfig) GetDevice() string         { return dc.getString(CONFIG_DISK_DEVICE) }
func (dc *DiskConfig) GetMountPoint() string     { return dc.getString(DISK_QUERY_DISK_DEVICE) }
func (dc *DiskConfig) GetContainerImage() string { return dc.getString(CONFIG_GLOBAL_CONTAINER_IMAGE) }
func (dc *DiskConfig) GetFormatPercent() int     { return dc.getInt(CONFIG_GLOBAL_FORMAT_PERCENT) }
func (dc *DiskConfig) GetHost() []string         { return dc.getSlice(CONFIG_GLOBAL_HOST) }
func (dc *DiskConfig) GetHostOnly() []string     { return dc.getSlice(CONFIG_DISK_HOST_ONLY) }
func (dc *DiskConfig) GetHostExclude() []string  { return dc.getSlice(CONFIG_DISK_HOST_EXCLUDE) }
