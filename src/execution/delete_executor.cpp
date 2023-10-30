//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_oid_t table_oid = plan_->TableOid();
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
  // Pulls data from SELECT
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  // Deletes the records.
  while (child_executor_->Next(&tuple, &rid)) {
    bool res = table_info->table_->MarkDelete(rid, exec_ctx_->GetTransaction());
    if (!res) {
      LOG_ERROR("cannot delete record");
    }
    std::cout << tuple.ToString(&(table_info->schema_)) << std::endl;
    std::cout << rid.ToString() << std::endl;
    // Also updates index
    std::vector<IndexInfo *> index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (IndexInfo *index_info : index_infos) {
      std::cout << "tuple " << tuple.ToString(&(table_info->schema_)) << "\n";
      index_info->index_->DeleteEntry(tuple, rid, exec_ctx_->GetTransaction());
      LOG_INFO("hello 456");
    }
    LOG_INFO("hello412");
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { return false; }

}  // namespace bustub
