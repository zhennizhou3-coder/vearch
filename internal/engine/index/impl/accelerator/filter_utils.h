/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstring>
#include <string>
#include <vector>

#include "common/gamma_common_data.h"
#include "common/common_query_data.h"
#include "util/log.h"
#include "util/utils.h"

namespace vearch {
namespace accelerator {

// Multi-value delimiter used by Vearch for STRING / STRINGARRAY fields.
// Kept in sync with the historical `kDelim` constants previously defined in
// every accelerator (GPU / NPU) index base class.
inline constexpr const char *kFilterDelim = "\001";

int ParseFilters(
    SearchCondition *condition,
    std::vector<enum DataType> &range_filter_types,
    std::vector<std::vector<std::string>> &all_term_items);

/**
 * Check whether the value of `range.field` on `docid` falls inside the
 * [lower_value, upper_value] interval described by `range` (inclusivity is
 * controlled by `range.include_lower` / `range.include_upper`).
 */
template <class T>
inline bool IsInRange(Table *table, RangeFilter &range, long docid) {
  T value = 0;
  std::string field_value;
  int field_id = table->GetAttrIdx(range.field);
  table->GetFieldRawValue(docid, field_id, field_value);
  memcpy(&value, field_value.c_str(), sizeof(value));

  T lower_value, upper_value;
  memcpy(&lower_value, range.lower_value.c_str(), range.lower_value.size());
  memcpy(&upper_value, range.upper_value.c_str(), range.upper_value.size());

  if (range.include_lower != 0 && range.include_upper != 0) {
    if (value >= lower_value && value <= upper_value) return true;
  } else if (range.include_lower != 0 && range.include_upper == 0) {
    if (value >= lower_value && value < upper_value) return true;
  } else if (range.include_lower == 0 && range.include_upper != 0) {
    if (value > lower_value && value <= upper_value) return true;
  } else {
    if (value > lower_value && value < upper_value) return true;
  }
  return false;
}

bool FilteredByRangeFilter(
    SearchCondition *condition,
    std::vector<enum DataType> &range_filter_types, long docid);

bool FilteredByTermFilter(
    SearchCondition *condition,
    std::vector<std::vector<std::string>> &all_term_items, long docid);

}  // namespace accelerator
}  // namespace vearch
