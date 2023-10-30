//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_oid_t table_oid = plan_->TableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_oid);

  Tuple source_tuple;
  TableIterator begin_iterator = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  TableIterator end_iterator = table_info_->table_->End();

  for (TableIterator ti = begin_iterator; ti != end_iterator; ti++) {
    const Tuple source_tuple = *ti;
    Tuple dest_tuple = GenerateUpdatedTuple(source_tuple);
    table_info_->table_->UpdateTuple(dest_tuple, source_tuple.GetRid(), exec_ctx_->GetTransaction());
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { return false; }

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
