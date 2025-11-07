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
#include "velox/exec/OutputBuffer.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

using core::PartitionedOutputNode;

namespace {
std::string printPageSizes(
    const std::vector<std::shared_ptr<SerializedPageBase>>& data) {
  std::stringstream out;
  out << "[data: " << data.size() << ", page sizes: (";
  for (auto& dataPage : data) {
    out << (dataPage ? dataPage->size() : -1L) << ", ";
  }
  out << ")";
  return out.str();
}

std::string printPageSizes(
    const std::vector<std::unique_ptr<folly::IOBuf>>& data) {
  std::stringstream out;
  out << "[data: " << data.size() << ", page sizes: (";
  for (auto& dataPage : data) {
    out << (dataPage ? dataPage->length() : -1L) << ", ";
  }
  out << ")";
  return out.str();
}
} // namespace

void ArbitraryBuffer::noMoreData() {
  // Drop duplicate end markers.
  pages_.withWLock([](auto& pages) {
    if (!pages.empty() && pages.back() == nullptr) {
      return;
    }
    pages.push_back(nullptr);
  });
}

void ArbitraryBuffer::enqueue(std::unique_ptr<SerializedPageBase> page) {
  VELOX_CHECK_NOT_NULL(page, "Unexpected null page");
  VELOX_CHECK(!hasNoMoreData(), "Arbitrary buffer has set no more data marker");
  pages_.withWLock([&](auto& pages) {
    pages.push_back(std::shared_ptr<SerializedPageBase>(page.release()));
    bytesBuffered_ += page->size();
  });
}

int64_t ArbitraryBuffer::bytesBuffered() const {
  return bytesBuffered_.load();
}

std::vector<std::shared_ptr<SerializedPageBase>> ArbitraryBuffer::getPages(
    uint64_t maxBytes) {
  std::vector<std::shared_ptr<SerializedPageBase>> retrievedPages;
  pages_.withRLock([&](auto& pages) {
    // VLOG(4) << "ArbitraryBuffer::getPages() acquired pages_ read lock";
    if (maxBytes == 0 && !pages.empty() && pages.front() == nullptr) {
      // Always give out an end marker when this buffer is finished and fully
      // consumed.  When multiple `DestinationBuffer' polling the same
      // `ArbitraryBuffer', we can simplify the code in
      // `DestinationBuffer::getData' since we will always get a null marker and
      // not going through the callback path, eliminate the chance of getting
      // stuck.
      VELOX_CHECK_EQ(pages.size(), 1);
      retrievedPages.push_back(nullptr);
    }
    // VLOG(4) << "ArbitraryBuffer::getPages() released pages_ read lock";
  });

  if (retrievedPages.size() == 1 && retrievedPages[0] == nullptr) {
    return retrievedPages;
  }

  uint64_t bytesRemoved{0};
  pages_.withWLock([&](auto& pages) {
    // VLOG(4) << "ArbitraryBuffer::getPages() acquired pages_ write lock";
    while (bytesRemoved < maxBytes && !pages.empty()) {
      if (pages.front() == nullptr) {
        // NOTE: keep the end marker in arbitrary buffer to signal all the
        // destination buffers after the buffers have all been consumed.
        VELOX_CHECK_EQ(pages.size(), 1);
        retrievedPages.push_back(nullptr);
        break;
      }
      bytesRemoved += pages.front()->size();
      bytesBuffered_ -= pages.front()->size();
      retrievedPages.push_back(std::move(pages.front()));
      pages.pop_front();
    }
    // VLOG(4) << "ArbitraryBuffer::getPages() released pages_ write lock";
  });

  return retrievedPages;
}

std::string ArbitraryBuffer::toString() const {
  return fmt::format(
      "[ARBITRARY_BUFFER PAGES[{}] NO MORE DATA[{} bytesBuffered_: {}]]",
      pages_.rlock()->size() - !!hasNoMoreData(),
      hasNoMoreData(),
      bytesBuffered_);
}

void DestinationBuffer::recordEnqueue(const SerializedPageBase& data) {
  const auto numRows = data.numRows();
  VELOX_CHECK(numRows.has_value(), "SerializedPage's numRows must be valid");
  bytesBuffered_ += data.size();
  rowsBuffered_ += numRows.value();
  ++pagesBuffered_;
}

void DestinationBuffer::recordAcknowledge(const SerializedPageBase& data) {
  const auto numRows = data.numRows();
  VELOX_CHECK(numRows.has_value(), "SerializedPage's numRows must be valid");
  const int64_t size = data.size();
  bytesBuffered_ -= size;
  VELOX_DCHECK_GE(bytesBuffered_, 0, "bytesBuffered must be non-negative");
  rowsBuffered_ -= numRows.value();
  VELOX_DCHECK_GE(rowsBuffered_, 0, "rowsBuffered must be non-negative");
  --pagesBuffered_;
  VELOX_DCHECK_GE(pagesBuffered_, 0, "pagesBuffered must be non-negative");
  bytesSent_ += size;
  rowsSent_ += numRows.value();
  ++pagesSent_;
}

void DestinationBuffer::recordDelete(const SerializedPageBase& data) {
  recordAcknowledge(data);
}

void DestinationBuffer::getData(
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify,
    DataConsumerActiveCheckCallback activeCheck,
    ArbitraryBuffer* arbitraryBuffer) {
  // VLOG(3) << this << " DestinationBuffer::getData begin maxBytes: " <<
  // maxBytes
  //          << " sequence: " << sequence << " notify: " << &notify
  //          << " activeCheck: " << &activeCheck
  //          << " arbitraryBuffer: " << arbitraryBuffer
  //          << " this: " << this->toString();

  VELOX_CHECK_GE(
      sequence, sequence_, "Get received for an already acknowledged item");
  if (arbitraryBuffer != nullptr) {
    addPages(arbitraryBuffer->getPages(maxBytes));
  }

  std::shared_ptr<PendingRead> oldPendingRead = nullptr;
  std::vector<std::unique_ptr<folly::IOBuf>> retrievedData;
  int64_t remainingBytes = 0;
  data_.withRLock([&](auto& data) {
    // VLOG(4) << this << " DestinationBuffer::getData() acquired data_ read
    // lock";
    if (!data.empty() || maxBytes == 0 || sequence - sequence_ < data.size()) {
      oldPendingRead = std::atomic_exchange(&pendingRead_, oldPendingRead);
      int64_t resultBytes = processReadLocked(
          data, maxBytes, sequence, arbitraryBuffer, retrievedData);
      remainingBytes = bytesBuffered_.load() +
          (arbitraryBuffer ? arbitraryBuffer->bytesBuffered() : 0) -
          resultBytes;
    } else {
      auto pendingRead = std::make_shared<PendingRead>(
          std::move(notify),
          std::move(activeCheck),
          pendingRead_ ? std::min(pendingRead_->sequence, sequence) : sequence,
          maxBytes);
      std::atomic_store(&pendingRead_, pendingRead);
    }
    // VLOG(4) << this << " DestinationBuffer::getData() released data_ read
    // lock";
  });

  if (notify && (!retrievedData.empty() || maxBytes == 0)) {
    // VLOG(3) << this
    //          << " DestinationBuffer::getData. Got immediate nonempty result :
    //          "
    //          << retrievedData.size() << ", remainingBytes: ("
    //          << fmt::format("{}", fmt::join(remainingBytes, ", "))
    //          << "), this: " << this->toString() << " notifying callback.";
    notify(std::move(retrievedData), sequence, remainingBytes);
  }
  oldPendingRead = nullptr;
  // VLOG(3) << this
  //          << " DestinationBuffer::getData end. this: " << this->toString();
}

void DestinationBuffer::enqueue(
    const std::vector<std::shared_ptr<SerializedPageBase>>& newPages,
    const ArbitraryBuffer* arbitraryBuffer) {
  // VLOG(3) << this << " DestinationBuffer::enqueue begin. newPages: "
  //          << printPageSizes(newPages) << ", this: " << this->toString();

  // Drop duplicate end markers. Early return with reader lock.
  std::shared_ptr<PendingRead> oldPendingRead = nullptr;
  oldPendingRead = std::atomic_exchange(&pendingRead_, oldPendingRead);

  addPages(newPages);

  // we just added a page, so process the pending read
  if (oldPendingRead) {
    processRead(
        oldPendingRead->maxBytes,
        oldPendingRead->sequence,
        oldPendingRead->callback,
        oldPendingRead->aliveCheck,
        arbitraryBuffer);
  }

  // VLOG(3) << this << " DestinationBuffer::enqueue end. newPages: "
  //          << std::to_string(newPages.size()) << ", this: " <<
  //          this->toString();
}

void DestinationBuffer::loadDataIfNecessary(ArbitraryBuffer* arbitraryBuffer) {
  // VLOG(3) << this
  //          << " DestinationBuffer::loadDataIfNecessary begin.
  //          arbitraryBuffer: "
  //          << arbitraryBuffer << ", this: " << this->toString();

  VELOX_CHECK(!arbitraryBuffer->empty() || arbitraryBuffer->hasNoMoreData());

  auto oldPendingRead = std::atomic_exchange(
      &pendingRead_, std::shared_ptr<PendingRead>(nullptr));

  if (oldPendingRead == nullptr ||
      (oldPendingRead->aliveCheck != nullptr &&
       !oldPendingRead->aliveCheck())) {
    return;
  }

  addPages(arbitraryBuffer->getPages(oldPendingRead->maxBytes));

  processRead(
      oldPendingRead->maxBytes,
      oldPendingRead->sequence,
      oldPendingRead->callback,
      oldPendingRead->aliveCheck,
      arbitraryBuffer);

  // VLOG(3) << this
  //          << " DestinationBuffer::loadDataIfNecessary end. arbitraryBuffer:
  //          "
  //          << arbitraryBuffer << ", this: " << this->toString();
}

void DestinationBuffer::addPages(
    const std::vector<std::shared_ptr<SerializedPageBase>>& newPages) {
  // VLOG(3) << this << " DestinationBuffer::addPages begin. #newPages: "
  //          << printPageSizes(newPages) << ", this: " << this->toString();

  data_.withWLock([&](auto& data) {
    // VLOG(4) << this
    //          << " DestinationBuffer::addPages() acquired data_ write lock";
    for (auto& newPage : newPages) {
      // Drop duplicate end markers.
      if (newPage == nullptr && !data.empty() && data.back() == nullptr) {
        //        VLOG(3)
        //          << this
        //          << " DestinationBuffer::addPages end. Drop duplicate end
        //          markers.";
        // VLOG(4) << this
        //    //          << " DestinationBuffer::addPages() released data_
        //    write lock 1";
        return;
      }
      if (newPage != nullptr) {
        recordEnqueue(*newPage);
        if (!data.empty() && data.back() == nullptr) {
          // VLOG(3) << this << " DestinationBuffer::addPages out of order";
        }
      } else {
        // VLOG(3) << this << " DestinationBuffer::addPages adding null page";
      }
      data.push_back(std::move(newPage));
    }
    // VLOG(4) << this
    //          << " DestinationBuffer::addPages() released data_ write lock 2";
  });

  // VLOG(3) << this
  //          << " DestinationBuffer::addPages end. #newPages: " <<
  //          newPages.size()
  //          << ", this: " << this->toString();
}

std::vector<std::shared_ptr<SerializedPageBase>> DestinationBuffer::acknowledge(
    int64_t sequence,
    bool fromGetData) {
  // VLOG(3) << this << " DestinationBuffer::acknowledge begin."
  //          << " sequence: " << sequence << " fromGetData: " << fromGetData
  //          << " this: " << this->toString();

  // Early return with read lock
  bool isValidRequest = true;
  data_.withRLock([&](auto& data) {
    if (finished_ || !validateSequenceNumberLocked(sequence, fromGetData)) {
      isValidRequest = false;
    }
  });
  if (!isValidRequest) {
    return {};
  }

  std::vector<std::shared_ptr<SerializedPageBase>> freed;
  data_.withWLock([&](auto& data) {
    // VLOG(4) << this
    //          << " DestinationBuffer::acknowledge() acquired data_ write
    //          lock";
    if (finished_ || !validateSequenceNumberLocked(sequence, fromGetData)) {
      return;
    }

    const int64_t numDeleted = sequence - sequence_.load();

    VELOX_CHECK_LE(
        numDeleted, data.size(), "Ack received for a not yet produced item");

    for (auto i = 0; i < numDeleted; ++i) {
      if (data[i] == nullptr) {
        VELOX_CHECK_EQ(i, data.size() - 1, "null marker found in the middle");
        break;
      }
      recordAcknowledge(*data[i]);
      freed.push_back(std::move(data[i]));
    }
    data.erase(data.begin(), data.begin() + numDeleted);
    sequence_ += numDeleted;

    // VLOG(4) << this
    //          << " DestinationBuffer::acknowledge() released data_ write
    //          lock";
  });

  // VLOG(3) << this << " DestinationBuffer::acknowledge end."
  //          << " freed.size(): " << freed.size() << " this: " <<
  //          this->toString();

  return freed;
}

std::vector<std::shared_ptr<SerializedPageBase>>
DestinationBuffer::deleteResults() {
  // VLOG(3) << this << " DestinationBuffer::deleteResults begin."
  //          << " this: " << this->toString();

  std::vector<std::shared_ptr<SerializedPageBase>> freed;
  std::shared_ptr<PendingRead> oldPendingRead = nullptr;

  data_.withWLock([&](auto& data) {
    // VLOG(4) << this
    //          << " DestinationBuffer::deleteResults acquired data_ write
    //          Lock";

    oldPendingRead = std::atomic_exchange(
        &pendingRead_, std::shared_ptr<PendingRead>(nullptr));

    for (auto i = 0; i < data.size(); ++i) {
      if (data[i] == nullptr) {
        VELOX_CHECK_EQ(i, data.size() - 1, "null marker found in the middle");
        break;
      }
      recordDelete(*data[i]);
      freed.push_back(std::move(data[i]));
    }

    data.clear();
    finished_ = true;

    // VLOG(4) << this
    //          << " DestinationBuffer::deleteResults released data_ write
    //          Lock";
  });

  if (oldPendingRead) {
    oldPendingRead->setEmptyResults();
  }

  // VLOG(3) << this << " DestinationBuffer::deleteResults end."
  //          << " freed.size(): " << freed.size() << " this: " <<
  //          this->toString();
  return freed;
}

DestinationBuffer::Stats DestinationBuffer::stats() const {
  return DestinationBuffer::Stats(
      finished_,
      bytesBuffered_,
      rowsBuffered_,
      pagesBuffered_,
      bytesSent_,
      rowsSent_,
      pagesSent_);
}

std::string DestinationBuffer::toString() {
  std::stringstream out;
  data_.withRLock([&](auto& data) {
    // VLOG(4) << this << " DestinationBuffer::toString acquired data_ read
    // Lock";

    out << this << "[available: " << data.size(); //<< ", page sizes: (";
    //    for (auto& dataPage : data) {
    //      out << (dataPage ? dataPage->size() : -1L) << ", ";
    //    }
    out << ", sequence: " << sequence_ << ", pendingRead_: "
        << (pendingRead_ ? pendingRead_->toString() : "null")
        << " finished_: " << finished_ << "]";

    // VLOG(4) << this << " DestinationBuffer::toString released data_ read
    // Lock";
  });
  return out.str();
}

std::shared_ptr<PendingRead> DestinationBuffer::pendingRead() {
  return std::atomic_load(&pendingRead_);
}

void DestinationBuffer::processRead(
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify,
    DataConsumerActiveCheckCallback activeCheck,
    const ArbitraryBuffer* arbitraryBuffer) {
  // VLOG(3) << this << " DestinationBuffer::processRead begin."
  //          << " maxBytes: " << maxBytes << ", sequence: " << sequence
  //          << " notify: " << &notify << " activeCheck: " << &activeCheck
  //          << " this: " << this->toString();

  std::vector<std::unique_ptr<folly::IOBuf>> retrievedData;
  int64_t remainingBytes = 0;
  data_.withRLock([&](auto& data) {
    // VLOG(4) << this
    //          << " DestinationBuffer::processRead acquired data_ read Lock";
    int64_t resultBytes = processReadLocked(
        data, maxBytes, sequence, arbitraryBuffer, retrievedData);
    remainingBytes = bytesBuffered_.load() +
        (arbitraryBuffer ? arbitraryBuffer->bytesBuffered() : 0) - resultBytes;
    // VLOG(4) << this
    //          << " DestinationBuffer::processRead release data_ read Lock";
  });

  // VLOG(3) << this << " DestinationBuffer::processRead end."
  //          << " maxBytes: " << maxBytes << ", sequence: " << sequence
  //          << " notify: " << &notify << " activeCheck: " << &activeCheck
  //          << " this: " << this->toString();
  notify(std::move(retrievedData), sequence, remainingBytes);
}

int64_t DestinationBuffer::processReadLocked(
    const std::vector<std::shared_ptr<SerializedPageBase>>& data,
    uint64_t maxBytes,
    int64_t sequence,
    const ArbitraryBuffer* arbitraryBuffer,
    std::vector<std::unique_ptr<folly::IOBuf>>& retrievedData) {
  auto offset = sequence - sequence_;
  uint64_t resultBytes = 0;
  if (maxBytes > 0) {
    for (; offset < data.size(); ++offset) {
      // nullptr is used as end marker
      if (data[offset] == nullptr) {
        VELOX_CHECK_EQ(
            offset, data.size() - 1, "null marker found in the middle");
        retrievedData.push_back(nullptr);
        break;
      }
      retrievedData.push_back(data[offset]->getIOBuf());
      resultBytes += data[offset]->size();
      if (resultBytes >= maxBytes) {
        ++offset;
        break;
      }
    }
  }

  bool atEnd = false;
  for (; offset < data.size(); ++offset) {
    if (data[offset] == nullptr) {
      VELOX_CHECK_EQ(
          offset, data.size() - 1, "null marker found in the middle");
      atEnd = true;
      break;
    }
  }

  if (retrievedData.empty() && bytesBuffered_ == 0 &&
      (!arbitraryBuffer || arbitraryBuffer->empty()) && atEnd) {
    retrievedData.push_back(nullptr);
  }

  return resultBytes;
}

bool DestinationBuffer::validateSequenceNumberLocked(
    const int64_t sequence,
    bool fromGetData) {
  bool isValid = true;
  const int64_t numDeleted = sequence - sequence_;
  if (numDeleted == 0 && fromGetData) {
    // If called from getData, it is expected that there will be
    // nothing to delete because a previous acknowledgement has been
    // received before the getData. This is not guaranteed though
    // because the messages may arrive out of order. Note that getData
    // implicitly acknowledges all messages with a lower sequence
    // number than the one in getData.
    isValid = false;
    // VLOG(3) << this << " DestinationBuffer::acknowledge nothing to delete";
  } else if (numDeleted <= 0) {
    // Acknowledges come out of order, e.g. ack of 10 and 9 have
    // swapped places in flight.
    VLOG(1) << this << " Out of order ack: " << sequence << " over "
            << sequence_;
    isValid = false;
  }
  return isValid;
}

namespace {
// Frees 'freed' and realizes 'promises'. Used after
// updateAfterAcknowledge. This runs outside of the mutex, so
// that we do the expensive free outside and only then continue the
// producers which will allocate more memory.
void releaseAfterAcknowledge(
    std::vector<std::shared_ptr<SerializedPageBase>>& freed,
    std::vector<ContinuePromise>& promises) {
  freed.clear();
  for (auto& promise : promises) {
    promise.setValue();
  }
}

} // namespace

OutputBuffer::OutputBuffer(
    std::shared_ptr<Task> task,
    PartitionedOutputNode::Kind kind,
    int numDestinations,
    uint32_t numDrivers)
    : task_(std::move(task)),
      kind_(kind),
      maxSize_(task_->queryCtx()->queryConfig().maxOutputBufferSize()),
      continueSize_((maxSize_ * kContinuePct) / 100),
      arbitraryBuffer_(
          isArbitrary() ? std::make_unique<ArbitraryBuffer>() : nullptr),
      numDrivers_(numDrivers) {
  buffers_.withWLock([&](auto& buffers) {
    buffers.reserve(numDestinations);
    for (int i = 0; i < numDestinations; i++) {
      buffers.push_back(std::make_unique<DestinationBuffer>());
    }
  });

  finishedBufferStats_.resize(numDestinations);
  if (isPartitioned()) {
    noMoreBuffers_ = true;
  }
}

void OutputBuffer::updateOutputBuffers(int numBuffers, bool noMoreBuffers) {
  VLOG(1) << this
          << " OutputBuffer::updateOutputBuffers begin for: " << task_->taskId()
          << " numBuffers: " << numBuffers
          << ", noMoreBuffers: " << noMoreBuffers
          << " this: " << this->toString();

  if (isPartitioned()) {
    VELOX_CHECK_EQ(buffersSize(), numBuffers);
    VELOX_CHECK(noMoreBuffers);
    noMoreBuffers_ = true;
    return;
  }

  if (numBuffers > buffers_.rlock()->size()) {
    addOutputBuffersLocked(numBuffers);
  }

  if (!noMoreBuffers) {
    return;
  }

  noMoreBuffers_ = true;
  bool isFinished = this->isFinished();

  std::vector<ContinuePromise> promises;
  updateAfterAcknowledge(dataToBroadcast_, promises);
  releaseAfterAcknowledge(dataToBroadcast_, promises);
  if (isFinished) {
    task_->setAllOutputConsumed();
  }

  VLOG(1) << this << " OutputBuffer::updateOutputBuffers end. "
          << task_->taskId() << " this: " << this->toString();
}

void OutputBuffer::updateNumDrivers(uint32_t newNumDrivers) {
  VLOG(1) << this << " OutputBuffer::updateNumDrivers begin for "
          << task_->taskId() << " newNumDrivers: " << newNumDrivers
          << " this: " << this->toString();

  bool isNoMoreDrivers{false};
  buffers_.withWLock([&](auto& buffers) {
    // VLOG(4) << this
    //          << " OutputBuffer::updateNumDrivers acquired buffers_ write
    //          Lock";
    numDrivers_ = newNumDrivers;
    // If we finished all drivers, ensure we register that we are 'done'.
    if (numDrivers_ == numFinished_) {
      isNoMoreDrivers = true;
    }
    // VLOG(4) << this
    //          << " OutputBuffer::updateNumDrivers released buffers_ write
    //          Lock";
  });

  if (isNoMoreDrivers) {
    noMoreDrivers();
  }

  VLOG(1) << this
          << " OutputBuffer::updateNumDrivers end for: " << task_->taskId()
          << " newNumDrivers: " << newNumDrivers
          << " this: " << this->toString();
}

void OutputBuffer::addOutputBuffersLocked(int numBuffers) {
  VLOG(1) << this << " OutputBuffer::addOutputBuffersLocked begin for: "
          << task_->taskId() << " numBuffers: " << numBuffers
          << " this: " << this->toString();

  VELOX_CHECK(!noMoreBuffers_);
  VELOX_CHECK(!isPartitioned());

  buffers_.withWLock([&](auto& buffers) {
    //    VLOG(4)
    //        << "OutputBuffer::addOutputBuffersLocked acquired buffers_ write
    //        Lock";
    if (numBuffers <= buffers.size()) {
      //      VLOG(3)
      //          << "OutputBuffer::addOutputBuffersLocked numBuffers <=
      //          buffers.size(). released buffers_ write Lock";
      return;
    }

    buffers.reserve(numBuffers);
    for (int32_t i = buffers.size(); i < numBuffers; ++i) {
      auto buffer = std::make_unique<DestinationBuffer>();
      if (isBroadcast()) {
        //        for (const auto& data : dataToBroadcast_) {
        buffer->enqueue(dataToBroadcast_, arbitraryBuffer_.get());
        //        }
        if (atEnd_) {
          buffer->enqueue({nullptr}, arbitraryBuffer_.get());
        }
      }
      buffers.emplace_back(std::move(buffer));
    }
    finishedBufferStats_.resize(numBuffers);
    //    VLOG(4)
    //        << "OutputBuffer::addOutputBuffersLocked released buffers_ write
    //        Lock";
  });

  VLOG(1) << this << " OutputBuffer::addOutputBuffersLocked end for: "
          << task_->taskId() << " numBuffers: " << numBuffers
          << " this: " << this->toString();
}

void OutputBuffer::updateStatsWithEnqueuedPage(
    int64_t pageBytes,
    int64_t pageRows) {
  updateTotalBufferedBytesMs();

  bufferedBytes_ += pageBytes;
  ++bufferedPages_;

  ++numOutputPages_;
  numOutputRows_ += pageRows;
  numOutputBytes_ += pageBytes;
}

void OutputBuffer::updateStatsWithFreedPages(int numPages, int64_t pageBytes) {
  updateTotalBufferedBytesMs();

  bufferedBytes_ -= pageBytes;
  VELOX_CHECK_GE(bufferedBytes_, 0);
  bufferedPages_ -= numPages;
  VELOX_CHECK_GE(bufferedPages_, 0);
}

void OutputBuffer::updateTotalBufferedBytesMs() {
  const auto nowMs = getCurrentTimeMs();
  buffers_.withWLock([&](auto& buffers) {
    //    VLOG(4)
    //        << "OutputBuffer::updateTotalBufferedBytesMs acquired buffers_
    //        write Lock";
    if (bufferedBytes_ > 0) {
      const auto deltaMs = nowMs - bufferStartMs_;
      totalBufferedBytesMs_ += bufferedBytes_ * deltaMs;
    }
    bufferStartMs_ = nowMs;
    //    VLOG(4)
    //        << "OutputBuffer::updateTotalBufferedBytesMs released buffers_
    //        write Lock";
  });
}

bool OutputBuffer::enqueue(
    int destination,
    std::unique_ptr<SerializedPageBase> data,
    ContinueFuture* future) {
  VLOG(1) << this << " OutputBuffer::enqueue begin for: " << task_->taskId()
          << "/results/" << destination
          << ", data: " << (data ? data->size() : -1L)
          << " this: " << this->toString();

  // The task might have been deleted already.
  if (!task_->isRunning()) {
    VLOG(1) << "Task " << task_->taskId() << " is in " << task_->state()
            << "state, cannot enqueue data to OutputBuffer.";
    return false;
  }

  VELOX_CHECK_NOT_NULL(data);
  VELOX_CHECK_LT(destination, buffersSize());

  updateStatsWithEnqueuedPage(data->size(), data->numRows().value());

  switch (kind_) {
    case PartitionedOutputNode::Kind::kBroadcast:
      VELOX_CHECK_EQ(destination, 0, "Bad destination {}", destination);
      enqueueBroadcastOutput(std::move(data));
      break;
    case PartitionedOutputNode::Kind::kArbitrary:
      VELOX_CHECK_EQ(destination, 0, "Bad destination {}", destination);
      enqueueArbitraryOutput(std::move(data));
      break;
    case PartitionedOutputNode::Kind::kPartitioned:
      enqueuePartitionedOutput(destination, std::move(data));
      break;
  }

  bool blocked = false;
  if (bufferedBytes_ >= maxSize_ && future) {
    promises_.withWLock([&future, this](auto& promises) {
      common::testutil::TestValue::adjust(
          "facebook::velox::exec::OutputBuffer::enqueue", this);
      // VLOG(4) << "OutputBuffer::enqueue acquired promises_ write Lock";
      promises.emplace_back("OutputBuffer::enqueue");
      *future = promises.back().getSemiFuture();
      // VLOG(4) << "OutputBuffer::enqueue released promises_ write Lock";
    });
    blocked = true;
  }

  VLOG(1) << this << " OutputBuffer::enqueue end for: " << task_->taskId()
          << "/results/" << destination << " this: " << this->toString();

  return blocked;
}

void OutputBuffer::enqueueBroadcastOutput(
    std::unique_ptr<SerializedPageBase> data) {
  VELOX_DCHECK(isBroadcast());
  VELOX_CHECK_NULL(arbitraryBuffer_);

  std::shared_ptr<SerializedPageBase> sharedData(data.release());
  buffers_.withRLock([&](auto& buffers) {
    //    VLOG(4)
    //        << "OutputBuffer::enqueueBroadcastOutput acquired buffers_ read
    //        Lock";
    for (auto& buffer : buffers) {
      if (buffer != nullptr) {
        buffer->enqueue({sharedData}, arbitraryBuffer_.get());
      }
    }
    //    VLOG(4)
    //        << "OutputBuffer::enqueueBroadcastOutput released buffers_ read
    //        Lock";
  });
  // NOTE: we don't need to add new buffer to 'dataToBroadcast_' if there is
  // no more output buffers.
  dataToBroadcast_.emplace_back(sharedData);
}

void OutputBuffer::enqueueArbitraryOutput(
    std::unique_ptr<SerializedPageBase> data) {
  VELOX_DCHECK(isArbitrary());
  VELOX_DCHECK_NOT_NULL(arbitraryBuffer_);
  VELOX_CHECK(!arbitraryBuffer_->hasNoMoreData());

  arbitraryBuffer_->enqueue(std::move(data));

  buffers_.withRLock([&](auto& buffers) {
    VLOG(4)
        << "OutputBuffer::enqueueArbitraryOutput acquired buffers_ read Lock";
    VELOX_CHECK_LT(nextArbitraryLoadBufferIndex_, buffers.size());
    int32_t bufferId = nextArbitraryLoadBufferIndex_;
    for (int32_t i = 0; i < buffers.size();
         ++i, bufferId = (bufferId + 1) % buffers.size()) {
      if (arbitraryBuffer_->empty()) {
        nextArbitraryLoadBufferIndex_ = bufferId;
        break;
      }

      auto* buffer = buffers[bufferId].get();
      if (buffer == nullptr) {
        continue;
      }
      buffer->loadDataIfNecessary(arbitraryBuffer_.get());
    }
    VLOG(4)
        << "OutputBuffer::enqueueArbitraryOutput released buffers_ read Lock";
  });
}

void OutputBuffer::enqueuePartitionedOutput(
    int destination,
    std::unique_ptr<SerializedPageBase> data) {
  VELOX_DCHECK(isPartitioned());
  VELOX_DCHECK(noMoreBuffers_);
  VELOX_CHECK_NULL(arbitraryBuffer_);

  DestinationBuffer* buffer = (*buffers_.rlock())[destination].get();
  if (buffer != nullptr) {
    buffer->enqueue({std::move(data)}, arbitraryBuffer_.get());
  } else {
    // Some downstream tasks may finish early and delete the corresponding
    // buffers. Further data for these buffers is dropped.
    updateStatsWithFreedPages(1, data->size());
  }
}

void OutputBuffer::noMoreData() {
  VLOG(1) << this << " OutputBuffer::noMoreData begin for: " << task_->taskId()
          << "this: " << this->toString();
  // Increment number of finished drivers.
  checkIfDone(true);
}

void OutputBuffer::noMoreDrivers() {
  VLOG(1) << this
          << " OutputBuffer::noMoreDrivers begin for: " << task_->taskId()
          << "this: " << this->toString();

  // Do not increment number of finished drivers.
  checkIfDone(false);
}

void OutputBuffer::checkIfDone(bool oneDriverFinished) {
  VLOG(1) << this << " OutputBuffer::checkIfDone for " << task_->taskId()
          << " oneDriverFinished: " << oneDriverFinished
          << " this: " << this->toString();

  buffers_.withWLock([&](auto& buffers) {
    //    VLOG(4)
    //        << this
    //        << " OutputBuffer::checkIfDone() acquired buffers_ write lock 1.
    //        numFinished_: "
    //        << numFinished_ << " numDrivers_: " << numDrivers_;
    if (oneDriverFinished) {
      ++numFinished_;
    }
    VELOX_CHECK_LE(
        numFinished_,
        numDrivers_,
        "Each driver should call noMoreData exactly once");
    atEnd_ = numFinished_ == numDrivers_;
  });

  // VLOG(3) << this << " OutputBuffer::checkIfDone atEnd_= " << atEnd_;
  // VLOG(4) << this
  //          << " OutputBuffer::checkIfDone() released buffers_ write lock
  //          1";
  if (!atEnd_) {
    // VLOG(3) << this << " OutputBuffer::checkIfDone end. atEnd_=false, return
    // ";
    return;
  }

  if (isArbitrary()) {
    arbitraryBuffer_->noMoreData();
    buffers_.withRLock([&](auto& buffers) {
      // VLOG(4) << this
      //    //          << " OutputBuffer::checkIfDone() acquired buffers_ read
      //    lock 2";
      for (auto& buffer : buffers) {
        if (buffer != nullptr) {
          buffer->loadDataIfNecessary(arbitraryBuffer_.get());
        }
      }
      // VLOG(4) << this
      //    //          << " OutputBuffer::checkIfDone() released buffers_ read
      //    lock 2";
    });
  } else {
    buffers_.withRLock([&](auto& buffers) {
      // VLOG(4) << this
      //    //          << " OutputBuffer::checkIfDone() acquired buffers_ read
      //    lock 3";
      for (auto& buffer : buffers) {
        if (buffer != nullptr) {
          buffer->enqueue({nullptr}, arbitraryBuffer_.get());
        }
      }
      // VLOG(4) << this
      //    //          << " OutputBuffer::checkIfDone() released buffers_ read
      //    lock 3";
    });

    //  VLOG(1) << this << " OutputBuffer::checkIfDone end. oneDriverFinished: "
    //          << oneDriverFinished << " this: " << this->toString();

    common::testutil::TestValue::adjust(
        "facebook::velox::exec::OutputBuffer::checkIfDone", this);
  }

  //  VLOG(1) << this << " OutputBuffer::checkIfDone end. oneDriverFinished: "
  //          << oneDriverFinished << " this: " << this->toString();
}

bool OutputBuffer::isFinished() {
  VLOG(1) << this << " OutputBuffer::isFinished begin for: " << task_->taskId()
          << " this: " << this->toString();

  // NOTE: for broadcast output buffer, we can only mark it as finished after
  // receiving the no more (destination) buffers signal.
  if (isBroadcast() && !noMoreBuffers_) {
    return false;
  }

  bool isFinished = true;
  buffers_.withRLock([&](auto& buffers) {
    // VLOG(4) << this
    //          << " OutputBuffer::isFinished() acquired buffers_ read lock";
    for (auto& buffer : buffers) {
      if (buffer != nullptr) {
        isFinished = false;
        break;
      }
    }
    // VLOG(4) << this
    //          << " OutputBuffer::isFinished() released buffers_ read lock";
  });

  //  VLOG(1) << this << " OutputBuffer::isFinished end. isFinished: " <<
  //  isFinished
  //          << ", this: " << this->toString();

  return isFinished;
}

void OutputBuffer::acknowledge(int destination, int64_t sequence) {
  VLOG(1) << this << " OutputBuffer::acknowledge begin for " << task_->taskId()
          << "/results/" << destination << "/" << sequence
          << " this: " << this->toString();

  if (atEnd_) {
    return;
  }

  std::vector<std::shared_ptr<SerializedPageBase>> freed;
  std::vector<ContinuePromise> promises;

  DestinationBuffer* buffer = (*buffers_.rlock())[destination].get();
  if (!buffer) {
    VLOG(1) << "Ack received after final ack for destination "
            << task_->taskId() << "/results/" << destination << "/" << sequence;
    return;
  }

  freed = buffer->acknowledge(sequence, false);
  updateAfterAcknowledge(freed, promises);
  releaseAfterAcknowledge(freed, promises);

  VLOG(1) << this << " OutputBuffer::acknowledge end for: " << task_->taskId()
          << "/results/" << destination << "/" << sequence
          << " this: " << this->toString();
}

void OutputBuffer::updateAfterAcknowledge(
    const std::vector<std::shared_ptr<SerializedPageBase>>& freed,
    std::vector<ContinuePromise>& updatedPromises) {
  uint64_t freedBytes{0};
  int freedPages{0};
  for (const auto& free : freed) {
    if (free.use_count() == 1) {
      ++freedPages;
      freedBytes += free->size();
    }
  }
  if (freedPages == 0) {
    VELOX_CHECK_EQ(freedBytes, 0);
    return;
  }
  VELOX_CHECK_GT(freedBytes, 0);

  updateStatsWithFreedPages(freedPages, freedBytes);

  if (bufferedBytes_ < continueSize_) {
    promises_.withWLock([&](auto& promises) {
      //      VLOG(4)
      //          << this
      //          << " OutputBuffer::updateAfterAcknowledge() acquired promises_
      //          write lock";
      if (bufferedBytes_ < continueSize_) {
        updatedPromises = std::move(promises);
      }
      //      VLOG(4)
      //          << this
      //          << " OutputBuffer::updateAfterAcknowledge() released promises_
      //          write lock";
    });
  }
}

bool OutputBuffer::deleteResults(int destination) {
  VLOG(1) << this << " OutputBuffer::deleteResults for " << task_->taskId()
          << "/" << destination << " this: " << this->toString();

  std::vector<std::shared_ptr<SerializedPageBase>> freed;
  std::vector<ContinuePromise> promises;

  DestinationBuffer* buffer = (*buffers_.rlock())[destination].get();
  if (!buffer) {
    VLOG(1) << "Extra delete received for destination " << destination;
    return false;
  }

  freed = buffer->deleteResults();

  buffers_.withWLock([&](auto& buffers) {
    // VLOG(4) << this
    //          << " OutputBuffer::deleteResults() acquired buffers_ write
    //          lock";
    VELOX_CHECK_LT(destination, finishedBufferStats_.size());
    finishedBufferStats_[destination] = buffer->stats();
    buffers[destination] = nullptr;
    // VLOG(4) << this
    //          << " OutputBuffer::deleteResults() released buffers_ write
    //          lock";
  });

  ++numFinalAcknowledges_;
  bool isFinished = this->isFinished();
  updateAfterAcknowledge(freed, promises);

  if (!promises.empty()) {
    VLOG(1) << "Delete of results unblocks producers. Can happen in early end "
            << "due to error or limit";
  }
  releaseAfterAcknowledge(freed, promises);
  if (isFinished) {
    task_->setAllOutputConsumed();
  }

  VLOG(1) << this << " OutputBuffer::deleteResults end for " << task_->taskId()
          << "/" << destination << " isFinished: " << isFinished
          << " this: " << this->toString();

  return isFinished;
}

void OutputBuffer::getData(
    int destination,
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify,
    DataConsumerActiveCheckCallback activeCheck) {
  VLOG(1) << this << " OutputBuffer::getData begin for " << task_->taskId()
          << "/results/" << destination << "/" << sequence
          << ", maxBytes: " << maxBytes << ", notify: " << &notify
          << ", activeCheck: " << &activeCheck << " this: " << this->toString();

  std::vector<std::shared_ptr<SerializedPageBase>> freed;
  std::vector<ContinuePromise> promises;

  if (!isPartitioned() && destination >= buffersSize()) {
    addOutputBuffersLocked(destination + 1);
  }

  auto* buffer = (*buffers_.rlock())[destination].get();
  if (buffer) {
    freed = buffer->acknowledge(sequence, true);
    updateAfterAcknowledge(freed, promises);
    buffer->getData(
        maxBytes, sequence, notify, activeCheck, arbitraryBuffer_.get());
  } else {
    std::vector<std::unique_ptr<folly::IOBuf>> emptyData;
    emptyData.emplace_back(nullptr);
    notify(std::move(emptyData), sequence, 0);
    VLOG(1) << "getData received after deleteResults for " << task_->taskId()
            << "/results/" << destination << "/" << sequence;
  }

  releaseAfterAcknowledge(freed, promises);
  VLOG(1) << this << " OutputBuffer::getData end. " << task_->taskId()
          << "/results/" << destination << " this: " << this->toString();
}

void OutputBuffer::terminate() {
  VLOG(1) << this
          << " OutputBuffer::terminate begin. this: " << this->toString();

  VELOX_CHECK(!task_->isRunning());

  std::vector<ContinuePromise> outstandingPromises;
  promises_.withWLock([&](auto& promises) {
    // VLOG(4) << this
    //          << " OutputBuffer::terminate() acquired promises_ write lock";
    outstandingPromises.swap(promises);
    // VLOG(4) << this
    //          << " OutputBuffer::terminate() released promises_ write lock";
  });

  for (auto& promise : outstandingPromises) {
    promise.setValue();
  }
}

std::string OutputBuffer::toString() {
  std::stringstream out;
  buffers_.withRLock([&](auto& buffers) {
    // VLOG(4) << this << " OutputBuffer::toString() acquired buffers_ read
    // lock";
    out << this << std::endl << "  [OutputBuffer[" << kind_ << "] bufferedBytes_="
        << bufferedBytes_
        //        << "b, num producers blocked=" << promises_.rlock()->size()
        << ",maxSize_=" << maxSize_ << ",continueSize_=" << continueSize_ <<",numberOfBuffers=" << buffers.size()
        << ",completed=" << numFinished_ << "/" << numDrivers_
        << ",atEnd_=" << atEnd_ << std::endl << " destinations: " << std::endl;
    for (auto i = 0; i < buffers.size(); ++i) {
      auto buffer = buffers[i].get();
      out << "    " << i << ": " << (buffer ? buffer->toString() : "none") << std::endl;
    }
    if (isArbitrary()) {
      out << "    arbitraryBuffer: " << arbitraryBuffer_->toString();
    }
    // VLOG(4) << this << " OutputBuffer::toString() released buffers_ read
    // lock";
  });

  out << "]" << std::endl;
  return out.str();
}

double OutputBuffer::getUtilization() const {
  return bufferedBytes_ / static_cast<double>(maxSize_);
}

bool OutputBuffer::isOverutilized() const {
  return (bufferedBytes_ > (0.5 * maxSize_)) || atEnd_;
}

int64_t OutputBuffer::getAverageBufferTimeMs() {
  return buffers_.withRLock([&](auto& buffers) {
    VLOG(4)
        << this
        << " OutputBuffer::getAverageBufferTimeMs() acquired buffers_ read lock";
    if (numOutputBytes_ > 0) {
      //      VLOG(4)
      //          << this
      //          << " OutputBuffer::getAverageBufferTimeMs() released buffers_
      //          read lock";
      return (int64_t)(totalBufferedBytesMs_ / numOutputBytes_);
    }
    VLOG(4)
        << this
        << " OutputBuffer::getAverageBufferTimeMs() released buffers_ read lock";
    return static_cast<int64_t>(0);
  });
}

namespace {
// Find out how many buffers hold 80% of the data. Useful to identify skew.
int32_t countTopBuffers(
    const std::vector<DestinationBuffer::Stats>& bufferStats,
    int64_t totalBytes) {
  std::vector<int64_t> bufferSizes;
  bufferSizes.reserve(bufferStats.size());
  for (auto i = 0; i < bufferStats.size(); ++i) {
    const auto& stats = bufferStats[i];
    bufferSizes.push_back(stats.bytesBuffered + stats.bytesSent);
  }

  // Sort descending.
  std::sort(bufferSizes.begin(), bufferSizes.end(), std::greater<int64_t>());

  const auto limit = totalBytes * 0.8;
  int32_t numBuffers = 0;
  int32_t runningTotal = 0;
  for (auto size : bufferSizes) {
    runningTotal += size;
    numBuffers++;

    if (runningTotal >= limit) {
      break;
    }
  }

  return numBuffers;
}

} // namespace

OutputBuffer::Stats OutputBuffer::stats() {
  //  std::lock_guard<std::mutex> l(mutex_);
  std::vector<DestinationBuffer::Stats> bufferStats;

  buffers_.withRLock([&](auto& buffers) {
    // VLOG(4) << this << " OutputBuffer::stats() acquired buffers_ read lock";
    VELOX_CHECK_EQ(buffers.size(), finishedBufferStats_.size());
    bufferStats.resize(buffers.size());
    for (auto i = 0; i < buffers.size(); ++i) {
      auto buffer = buffers[i].get();
      if (buffer != nullptr) {
        bufferStats[i] = buffer->stats();
      } else {
        bufferStats[i] = finishedBufferStats_[i];
      }
    }
    // VLOG(4) << this << " OutputBuffer::stats() released buffers_ read lock";
  });

  updateTotalBufferedBytesMs();

  return OutputBuffer::Stats(
      kind_,
      noMoreBuffers_,
      atEnd_,
      isFinished(),
      bufferedBytes_.load(),
      bufferedPages_.load(),
      numOutputBytes_.load(),
      numOutputRows_.load(),
      numOutputPages_.load(),
      getAverageBufferTimeMs(),
      countTopBuffers(bufferStats, numOutputBytes_.load()),
      bufferStats);
}

} // namespace facebook::velox::exec
