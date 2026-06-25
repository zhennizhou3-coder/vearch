/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "index/impl/accelerator/filter_utils.h"

namespace vearch {
namespace accelerator {

int ParseFilters(
    SearchCondition *condition,
    std::vector<enum DataType> &range_filter_types,
    std::vector<std::vector<std::string>> &all_term_items) {
  for (size_t i = 0; i < condition->range_filters.size(); ++i) {
    auto range = condition->range_filters[i];

    enum DataType type;
    if (condition->table->GetFieldType(range.field, type)) {
      LOG(ERROR) << "Can't get " << range.field << " data type";
      return -1;
    }

    if (type == DataType::STRING || type == DataType::STRINGARRAY) {
      LOG(ERROR) << range.field << " can't be range filter";
      return -1;
    }
    range_filter_types[i] = type;
  }

  for (size_t i = 0; i < condition->term_filters.size(); ++i) {
    auto term = condition->term_filters[i];

    enum DataType type;
    if (condition->table->GetFieldType(term.field, type)) {
      LOG(ERROR) << "Can't get " << term.field << " data type";
      return -1;
    }

    if (type != DataType::STRING && type != DataType::STRINGARRAY) {
      LOG(ERROR) << term.field << " can't be term filter";
      return -1;
    }

    std::vector<std::string> term_items = utils::split(term.value, kFilterDelim);
    all_term_items[i] = term_items;
  }
  return 0;
}

bool FilteredByRangeFilter(
    SearchCondition *condition,
    std::vector<enum DataType> &range_filter_types, long docid) {
  for (size_t i = 0; i < condition->range_filters.size(); ++i) {
    auto range = condition->range_filters[i];

    if (range_filter_types[i] == DataType::INT) {
      if (!IsInRange<int>(condition->table, range, docid)) return true;
    } else if (range_filter_types[i] == DataType::LONG) {
      if (!IsInRange<long>(condition->table, range, docid)) return true;
    } else if (range_filter_types[i] == DataType::FLOAT) {
      if (!IsInRange<float>(condition->table, range, docid)) return true;
    } else {
      if (!IsInRange<double>(condition->table, range, docid)) return true;
    }
  }
  return false;
}

bool FilteredByTermFilter(
    SearchCondition *condition,
    std::vector<std::vector<std::string>> &all_term_items, long docid) {
  for (size_t i = 0; i < condition->term_filters.size(); ++i) {
    auto term = condition->term_filters[i];

    std::string field_value;
    int field_id = condition->table->GetAttrIdx(term.field);
    condition->table->GetFieldRawValue(docid, field_id, field_value);
    std::vector<std::string> field_items;
    if (field_value.size() >= 0)
      field_items = utils::split(field_value, kFilterDelim);

    bool all_in_field_items;
    if (term.is_union == static_cast<int>(FilterOperator::Or))
      all_in_field_items = false;
    else
      all_in_field_items = true;

    for (auto term_item : all_term_items[i]) {
      bool in_field_items = false;
      for (size_t j = 0; j < field_items.size(); j++) {
        if (term_item == field_items[j]) {
          in_field_items = true;
          break;
        }
      }
      if (term.is_union == static_cast<int>(FilterOperator::Or))
        all_in_field_items |= in_field_items;
      else
        all_in_field_items &= in_field_items;
    }
    if (!all_in_field_items) return true;
  }
  return false;
}

}  // namespace accelerator
}  // namespace vearch
