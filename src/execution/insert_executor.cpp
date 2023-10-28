//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/exception.h"
#include "execution/executors/insert_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void InsertExecutor::Init() {
  // Get table which needs to be inserted
  table_oid_t table_oid = plan_->TableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(table_oid);
  const Schema &schema = table_info->schema_;
  // check if it is raw inserted ?
  if (plan_->IsRawInsert()) {
    std::vector<std::vector<Value>> raw_values = plan_->RawValues();

    for (std::vector<Value> &raw_value : raw_values) {
      RID rid;
      bool res = table_info->table_->InsertTuple(Tuple{raw_value, &schema}, &rid, exec_ctx_->GetTransaction());
      if (!res) {
        throw Exception{ExceptionType::OUT_OF_MEMORY, "Too large the tuple, cannot insert"};
      }
    }

    return;
  }

    // no raw insert
    const AbstractPlanNode* child_plan = plan_->GetChildPlan();
    if (child_plan->GetType() == PlanType::SeqScan){
        const SeqScanPlanNode* child_seq_scan = static_cast<const SeqScanPlanNode*>(child_plan);
        SeqScanExecutor seq_scan_executor(exec_ctx_, child_seq_scan);

        seq_scan_executor.Init();
        Tuple tuple;
        RID rid;
        while(seq_scan_executor.Next(&tuple, &rid)){
            bool res = table_info->table_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction());
            if (!res) {
                throw Exception{ExceptionType::OUT_OF_MEMORY, "Too large the tuple, cannot insert"};                
            }
        }
    }
    
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { return false; }

}  // namespace bustub
