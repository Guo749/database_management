//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    std::string string_tuple = tuple.ToString(child_executor_->GetOutputSchema());

    if (has_seen_.count(string_tuple) == 0) {
      has_seen_.insert(string_tuple);
      result_.push_back(tuple);
    }
  }
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (result_.empty()) {
    return false;
  }

  *tuple = result_.front();
  result_.pop_front();
  return true;
}

}  // namespace bustub
