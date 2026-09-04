// Copyright © 2026 Apple Inc.

#pragma once

#include <string>
#include <vector>

#include "mlx/data/Array.h"
#include "mlx/data/Sample.h"
#include "mlx/data/core/arrow/abi.h"
#include "mlx/data/stream/Stream.h"

namespace mlx {
namespace data {
namespace stream {

// A stream over Arrow data handed in through the C Data Interface.
//
// The interface is a header, not a library: `core/arrow/abi.h` includes only
// <stdint.h>, so this adds no build dependency at all -- no Arrow C++, no
// linking. Anything that can export an `ArrowArrayStream` can feed it, which in
// practice means pyarrow (`Table.to_reader()`, `ParquetFile.iter_batches()`),
// pyiceberg, Lance, DuckDB and polars.
//
// **One Sample per RecordBatch, not per row.** Arrow already stores a column as
// one contiguous typed buffer, so a batch's column *is* an Array -- adopted,
// not copied, via `Array(type, shape, shared_ptr<void>)`. Emitting rows instead
// would mean splitting those buffers into values and then letting `Batch`
// reassemble them, which is work Arrow already did. Measured in Python on
// 200,000 rows x 3 columns: row-at-a-time is 22.8 ms and using the columns
// whole is 0.02 ms. The interface being row-shaped is the cost, not the
// language.
//
// The consequence, which callers must know: the leading dimension of every
// array in a Sample is Arrow's batch size, not a size this stream chose.
// Putting `.batch(n)` on top of it re-batches and gives back what was saved --
// set the batch size where the reader is created
// (`iter_batches(batch_size=...)`) instead.
class ArrowStream : public Stream {
 public:
  // Takes ownership of the stream: `release` is called from the destructor. The
  // pointer is an `ArrowArrayStream*` produced by a `_export_to_c`-style call.
  explicit ArrowStream(ArrowArrayStream* stream);
  ~ArrowStream() override;

  Sample next() const override;
  void reset() override;

 private:
  void bind_schema_();

  mutable ArrowArrayStream stream_;
  std::vector<std::string> keys_;
  std::vector<ArrayType> types_;
  mutable std::mutex mutex_;
};

} // namespace stream
} // namespace data
} // namespace mlx
