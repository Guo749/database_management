//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      has_seen_result_(false) {
  assert(plan->GetAggregates().size() == plan->GetAggregateTypes().size());
}

std::unordered_map<uint32_t, Value> AggregationExecutor::GetAggregateKeys() {
  std::unordered_map<uint32_t, Value> result = {};
  std::unordered_set<uint32_t> column_index;
  const std::vector<const AbstractExpression *> aggregate_columns = plan_->GetAggregates();

  for (const AbstractExpression *abstract_expression : aggregate_columns) {
    const ColumnValueExpression *column_value_expression =
        dynamic_cast<const ColumnValueExpression *>(abstract_expression);

    uint32_t col_index = column_value_expression->GetColIdx();
    if (column_index.count(col_index) == 0) {
      result.insert({col_index, Value{TypeId::INTEGER, (int32_t)col_index}});
    }
  }

  return result;
}

void AggregationExecutor::GenerateResultTuple(std::unordered_map<uint32_t, Value> &aggregate_keys) {
  std::vector<Value> output_values;
  const Schema *output_schema = plan_->OutputSchema();

  for (unsigned long i = 0; i < plan_->GetAggregateTypes().size(); i++) {
    const ColumnValueExpression *column_expression =
        dynamic_cast<const ColumnValueExpression *>(plan_->GetAggregates()[i]);
    const AggregationType aggregate_type = plan_->GetAggregateTypes()[i];

    Value column_value = aggregate_keys[column_expression->GetColIdx()];
    AggregateKey aggregate_key;
    aggregate_key.group_bys_ = {column_value};

    for (SimpleAggregationHashTable::Iterator it = aht_.Begin(); it != aht_.End(); ++it) {
      const AggregateKey &hahs_table_key = it.Key();
      const AggregateValue &hash_table_val = it.Val();

      if (hahs_table_key == aggregate_key) {
        if (aggregate_type == AggregationType::CountAggregate) {
          output_values.emplace_back(hash_table_val.aggregates_[0]);
        } else if (aggregate_type == AggregationType::SumAggregate) {
          output_values.emplace_back(hash_table_val.aggregates_[1]);
        } else if (aggregate_type == AggregationType::MinAggregate) {
          output_values.emplace_back(hash_table_val.aggregates_[2]);
        } else if (aggregate_type == AggregationType::MaxAggregate) {
          output_values.emplace_back(hash_table_val.aggregates_[3]);
        } else {
          throw Exception{ExceptionType::INVALID, "Wrong aggregate type."};
        }
      }
    }
  }

  result_tuple_ = Tuple{output_values, output_schema};
}

void AggregationExecutor::Init() {
  child_->Init();
  std::unordered_map<uint32_t, Value> aggregate_keys = GetAggregateKeys();

  Tuple tuple;
  RID rid;

  // Steps:
  //  1. iterate all tuples,
  //  2. get all keys that need to aggregate
  //  3. for each aggregation, insert the value into hash table
  //  4. format the output and return result;
  while (child_->Next(&tuple, &rid)) {
    for (const std::pair<uint64_t, Value> aggregate_key : aggregate_keys) {
      uint32_t column_index = aggregate_key.first;
      Value to_insert_value = tuple.GetValue(child_->GetOutputSchema(), column_index);

      AggregateKey hash_table_key;
      hash_table_key.group_bys_ = {aggregate_keys[column_index]};
      AggregateValue aggregate_value;
      aggregate_value.aggregates_ = {to_insert_value, to_insert_value, to_insert_value, to_insert_value};

      aht_.InsertCombine(hash_table_key, aggregate_value);
    }
  }

  GenerateResultTuple(aggregate_keys);
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (has_seen_result_) {
    return false;
  }

  has_seen_result_ = true;
  *tuple = result_tuple_;

  return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
