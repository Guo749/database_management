//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // Get all datas from the target execution plan
  table_oid_t table_to_scan = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_to_scan);
  if (table_info == nullptr) {
    LOG_WARN("Sequential scan nul table id %d is a no-op", table_to_scan);
    return;
  }
  const Schema &schema = table_info->schema_;
  TableIterator table_begin_iterator = table_info->table_->Begin(exec_ctx_->GetTransaction());
  TableIterator table_end_iterator = table_info->table_->End();

  std::vector<uint32_t> key_attributes = {};
  const Schema *key_schema = plan_->OutputSchema();
  for (const Column &column : key_schema->GetColumns()) {
    key_attributes.push_back(schema.GetColIdx(column.GetName()));
  }

  // Create a deque and fill all the data by predicate if any
  const AbstractExpression *abstract_expression = plan_->GetPredicate();
  for (TableIterator ti = table_begin_iterator; ti != table_end_iterator; ti++) {
    Tuple tuple = *ti;

    if (abstract_expression == nullptr || abstract_expression->Evaluate(&tuple, &schema).GetAs<bool>()) {
      // either no predicate or predicate satisfies. we add the result.
      tuples_.emplace_back(std::make_pair(tuple.KeyFromTuple(schema, *key_schema, key_attributes), tuple.GetRid()));
    }
  }

  // return result;
  return;
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (tuples_.empty()) {
    return false;
  }

  std::pair<Tuple, RID> &res = tuples_.front();
  *tuple = res.first;
  *rid = res.second;
  tuples_.pop_front();
  return true;
}

}  // namespace bustub
