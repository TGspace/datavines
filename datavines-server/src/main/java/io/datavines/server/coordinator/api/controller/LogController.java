/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datavines.server.coordinator.api.controller;

import io.datavines.server.DataVinesConstants;
import io.datavines.server.coordinator.api.annotation.AuthIgnore;
import io.datavines.server.coordinator.api.aop.RefreshToken;
import io.datavines.server.coordinator.server.log.LogService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

@Api(value = "log", tags = "log")
@RestController
@RefreshToken
@RequestMapping(value = DataVinesConstants.BASE_API_PATH + "/task/log")
public class LogController {

    @Resource
    private LogService logService;

    @ApiOperation(value = "queryWholeLog", notes = "query whole task log")
    @GetMapping(value = "/queryWholeLog")
    public Object queryWholeLog(@RequestParam("taskId") Long taskId, HttpServletRequest request, HttpServletResponse response) throws IOException {
        String taskHost = logService.getTaskHost(taskId);
        Boolean isConcurrentHos = judgeConcurrentHost(request, taskHost);
        if (isConcurrentHos) {
            return logService.queryWholeLog(taskId);
        }
        response.sendRedirect(request.getScheme() + "://" + taskHost + "/api/v1/task/log/queryWholeLog?taskId=" + taskId);
        return null;
    }

    @ApiOperation(value = "queryLog", notes = "query task log with offsetLine")
    @GetMapping(value = "/queryLog")
    public Object queryLog(@RequestParam("taskId") Long taskId, @RequestParam("offsetLine") int offsetLine, HttpServletRequest request, HttpServletResponse response) throws IOException {
        String taskHost = logService.getTaskHost(taskId);
        Boolean isConcurrentHos = judgeConcurrentHost(request, taskHost);
        if (isConcurrentHos) {
            return logService.queryLog(taskId, offsetLine);
        }
        response.sendRedirect(request.getScheme() + "://" + taskHost + "/api/v1/task/log/queryLog?taskId=" + taskId);
        return null;
    }

    @ApiOperation(value = "queryLogForLimit", notes = "query task log with limit")
    @GetMapping(value = "/queryLogForLimit")
    public Object queryLogForLimit(@RequestParam("taskId") Long taskId, @RequestParam("offsetLine") int offsetLine, @RequestParam("offsetLine") int limit, HttpServletRequest request, HttpServletResponse response) throws IOException {
        String taskHost = logService.getTaskHost(taskId);
        Boolean isConcurrentHos = judgeConcurrentHost(request, taskHost);
        if (isConcurrentHos) {
            return logService.queryLog(taskId, offsetLine, limit);
        }
        response.sendRedirect(request.getScheme() + "://" + taskHost + "/api/v1/task/log/queryLogForLimit?taskId=" + taskId);
        return null;
    }

    private Boolean judgeConcurrentHost(HttpServletRequest request, String taskHost) throws MalformedURLException {
        URL url = new URL(request.getRequestURL().toString());
        String host = url.getHost();
        int port = url.getPort();
        if(-1 != port){
            host = host.concat(":").concat(String.valueOf(port));
        }
        return taskHost.equals(host);
    }
}
