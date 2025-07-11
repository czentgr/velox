/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/experimental/wave/exec/WaveOperator.h"

#include "velox/common/time/Timer.h"
#include "velox/exec/Task.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/WaveDataSource.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::wave {

class TableScan : public WaveSourceOperator {
 public:
  TableScan(
      CompileState& state,
      int32_t operatorId,
      const core::TableScanNode& tableScanNode,
      DefinesMap defines)
      : WaveSourceOperator(
            state,
            tableScanNode.outputType(),
            tableScanNode.id()),
        tableHandle_(tableScanNode.tableHandle()),
        columnHandles_(tableScanNode.assignments()),
        driverCtx_(state.driver().driverCtx()),
        connectorPool_(driverCtx_->task->addConnectorPoolLocked(
            planNodeId_,
            driverCtx_->pipelineId,
            driverCtx_->driverId,
            "",
            tableHandle_->connectorId())),
        readBatchSize_(driverCtx_->task->queryCtx()
                           ->queryConfig()
                           .preferredOutputBatchRows()) {
    defines_ = std::move(defines);
    connector_ = connector::getConnector(tableHandle_->connectorId());
  }

  std::vector<AdvanceResult> canAdvance(WaveStream& stream) override;

  void schedule(WaveStream& stream, int32_t maxRows = 0) override;

  bool isStreaming() const override {
    return true;
  }

  exec::BlockingReason isBlocked(WaveStream& /*stream*/, ContinueFuture* future)
      override;

  bool isFinished() const override;

  static uint64_t ioWaitNanos() {
    return ioWaitNanos_;
  }

  std::string toString() const override {
    return "TableScan";
  }

 private:
  exec::BlockingReason nextSplit(ContinueFuture* future);

  // Sets 'maxPreloadSplits' and 'splitPreloader' if prefetching
  // splits is appropriate. The preloader will be applied to the
  // 'first 'maxPreloadSplits' of the Tasks's split queue for 'this'
  // when getting splits.
  void checkPreload();

  // Sets 'split->dataSource' to be a Asyncsource that makes a
  // DataSource to read 'split'. This source will be prepared in the
  // background on the executor of the connector. If the DataSource is
  // needed before prepare is done, it will be made when needed.
  void preload(std::shared_ptr<connector::ConnectorSplit> split);

  // Adds 'stats' to operator stats of the containing WaveDriver. Some
  // stats come from DataSource, others from SplitReader. If
  // 'splitReader' is given, the completed rows/bytes from
  // 'splitReader' are added. These do not come in the runtimeStats()
  // map.
  void updateStats(
      std::unordered_map<std::string, RuntimeCounter> stats,
      WaveSplitReader* splitReader = nullptr);

  // Process-wide IO wait time.
  static std::atomic<uint64_t> ioWaitNanos_;

  const connector::ConnectorTableHandlePtr tableHandle_;
  const connector::ColumnHandleMap columnHandles_;
  exec::DriverCtx* const driverCtx_;
  memory::MemoryPool* const connectorPool_;
  ContinueFuture blockingFuture_{ContinueFuture::makeEmpty()};
  exec::BlockingReason blockingReason_;
  bool needNewSplit_ = true;
  std::shared_ptr<connector::Connector> connector_;
  std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  bool noMoreSplits_ = false;

  std::shared_ptr<connector::DataSource> dataSource_;

  std::shared_ptr<WaveDataSource> waveDataSource_;

  int32_t maxPreloadedSplits_{0};

  const int32_t maxSplitPreloadPerDriver_{0};

  // Callback passed to getSplitOrFuture() for triggering async
  // preload. The callback's lifetime is the lifetime of 'this'. This
  // callback can schedule preloads on an executor. These preloads may
  // outlive the Task and therefore need to capture a shared_ptr to
  // it.
  std::function<void(const std::shared_ptr<connector::ConnectorSplit>&)>
      splitPreloader_{nullptr};

  // Count of splits that started background preload.
  int32_t numPreloadedSplits_{0};

  // Count of splits that finished preloading before being read.
  int32_t numReadyPreloadedSplits_{0};

  vector_size_t readBatchSize_;

  // String shown in ExceptionContext inside DataSource and LazyVector loading.
  std::string debugString_;

  // The last value of the IO wait time of 'this' that has been added to the
  // global static 'ioWaitNanos_'.
  uint64_t lastIoWaitNanos_{0};

  // The value returned by canAdvance() of the WaveDataSource after last
  // schedule().
  int32_t nextAvailableRows_{0};

  // True if canAdvance() should do waveDataSource_->canAdvance() instead of
  // returning 'nextAvailableRows_'.
  bool isNewSplit_{false};
};
} // namespace facebook::velox::wave
