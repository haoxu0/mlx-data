// Copyright © 2026 Apple Inc.

#include "mlx/data/stream/ArrowStream.h"

#include <cstring>
#include <stdexcept>

namespace mlx {
namespace data {
namespace stream {

namespace {

// Arrow's format strings, from the C Data Interface spec. Only the fixed-width
// types that `ArrayType` can hold are accepted: there is no bool, no 16-bit, no
// unsigned above a byte, and no string type, so anything else is refused by
// name rather than approximated.
ArrayType array_type_of(const char* format) {
  if (format == nullptr) {
    throw std::runtime_error(
        "ArrowStream: a child schema has no format string");
  }
  const std::string f(format);
  if (f == "c") {
    return ArrayType::Int8;
  } else if (f == "C" || f == "b") {
    // "b" is Arrow's boolean, and it is a *bitmap*, not one byte per value --
    // adopting its buffer would silently reinterpret 8 values as 1. Refused
    // below instead.
    if (f == "b") {
      throw std::runtime_error(
          "ArrowStream: boolean columns are a bitmap, one bit per value, and cannot be "
          "adopted as a byte array; cast to int8 before exporting");
    }
    return ArrayType::UInt8;
  } else if (f == "i") {
    return ArrayType::Int32;
  } else if (f == "l") {
    return ArrayType::Int64;
  } else if (f == "f") {
    return ArrayType::Float;
  } else if (f == "g") {
    return ArrayType::Double;
  }
  throw std::runtime_error(
      "ArrowStream: no ArrayType holds Arrow format '" + f +
      "'. Fixed-width int8/uint8/int32/int64/float/double are supported; strings, "
      "booleans, 16-bit and nested types are not.");
}

} // namespace

ArrowStream::ArrowStream(ArrowArrayStream* stream) {
  if (stream == nullptr || stream->release == nullptr) {
    throw std::runtime_error(
        "ArrowStream: got a released or null ArrowArrayStream");
  }
  // Move: the producer hands over ownership, and a moved-from stream is marked
  // released so nobody calls it twice. This is the convention the C Data
  // Interface specifies.
  std::memcpy(&stream_, stream, sizeof(ArrowArrayStream));
  stream->release = nullptr;
  bind_schema_();
}

ArrowStream::~ArrowStream() {
  if (stream_.release != nullptr) {
    stream_.release(&stream_);
  }
}

void ArrowStream::bind_schema_() {
  ArrowSchema schema;
  if (stream_.get_schema(&stream_, &schema) != 0) {
    const char* msg =
        stream_.get_last_error ? stream_.get_last_error(&stream_) : nullptr;
    throw std::runtime_error(
        std::string("ArrowStream: get_schema failed: ") +
        (msg ? msg : "unknown"));
  }
  // A struct at the top level is how a batch is described; its children are the
  // columns.
  for (int64_t c = 0; c < schema.n_children; ++c) {
    const ArrowSchema* child = schema.children[c];
    keys_.push_back(child->name ? child->name : ("col" + std::to_string(c)));
    types_.push_back(array_type_of(child->format));
  }
  if (schema.release != nullptr) {
    schema.release(&schema);
  }
}

Sample ArrowStream::next() const {
  ArrowArray batch;
  {
    std::unique_lock lock(mutex_);
    if (stream_.release == nullptr) {
      return Sample();
    }
    if (stream_.get_next(&stream_, &batch) != 0) {
      const char* msg =
          stream_.get_last_error ? stream_.get_last_error(&stream_) : nullptr;
      throw std::runtime_error(
          std::string("ArrowStream: get_next failed: ") +
          (msg ? msg : "unknown"));
    }
  }
  // A released array is how the interface says "end of stream".
  if (batch.release == nullptr) {
    return Sample();
  }

  // One owner for the whole batch, so its release callback fires exactly once
  // when the last column Array is dropped -- not per column, and not while a
  // column is still referenced.
  auto keep =
      std::shared_ptr<ArrowArray>(new ArrowArray(batch), [](ArrowArray* a) {
        if (a->release != nullptr) {
          a->release(a);
        }
        delete a;
      });

  Sample sample;
  for (int64_t c = 0; c < keep->n_children; ++c) {
    const ArrowArray* child = keep->children[c];
    if (child->null_count != 0) {
      throw std::runtime_error(
          "ArrowStream: column '" + keys_.at(c) + "' has " +
          std::to_string(child->null_count) +
          " null(s), and a Sample has nowhere to record which values are absent. Fill or "
          "drop them before exporting.");
    }
    if (child->n_buffers < 2 || child->buffers[1] == nullptr) {
      throw std::runtime_error(
          "ArrowStream: column '" + keys_.at(c) +
          "' has no values buffer, so it is not a fixed-width column");
    }
    // The offset is in *values*, not bytes, and a sliced batch has a non-zero
    // one. Ignoring it would silently return the wrong rows.
    auto* base = static_cast<char*>(const_cast<void*>(child->buffers[1]));
    Array probe(types_.at(c), std::vector<int64_t>{0});
    void* values = base + child->offset * probe.itemsize();
    // Aliasing constructor: points at this column's buffer, owns the whole
    // batch.
    sample[keys_.at(c)] = std::make_shared<Array>(
        types_.at(c),
        std::vector<int64_t>{child->length},
        std::shared_ptr<void>(keep, values));
  }
  return sample;
}

void ArrowStream::reset() {
  // The C Data Interface has no rewind: a stream is consumed once.
  // `Stream::reset` is what `repeat` and a second epoch call, so refusing is
  // the honest answer -- a caller who needs more than one pass exports a new
  // stream, which is what `stream_python_iterable`'s factory argument exists
  // for on the Python side.
  throw std::runtime_error(
      "ArrowStream: an ArrowArrayStream is consumed once and cannot be reset. Wrap a "
      "factory that exports a fresh stream per epoch.");
}

} // namespace stream
} // namespace data
} // namespace mlx
