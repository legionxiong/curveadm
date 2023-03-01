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

package tui

import (
	"fmt"

	"github.com/opencurve/curveadm/internal/storage"
	tuicommon "github.com/opencurve/curveadm/internal/tui/common"
)

func FormatDiskReplacements(diskReplacements []storage.DiskReplacement) string {
	lines := [][]interface{}{}
	title := []string{
		"Host",
		"Device Path",
		"Service ID",
		"Progress",
		"Status",
	}
	first, second := tuicommon.FormatTitle(title)
	lines = append(lines, first)
	lines = append(lines, second)

	for _, dr := range diskReplacements {
		lines = append(lines, []interface{}{
			dr.Host,
			dr.Device,
			dr.ChunkServerID,
			fmt.Sprintf("%s%s", dr.Progress, "%%"),
			dr.Status,
		})
	}

	return tuicommon.FixedFormat(lines, 2)
}
