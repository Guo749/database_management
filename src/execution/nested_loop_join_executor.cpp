//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <deque>

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {
namespace {
std::vector<Tuple> GetTuples(std::unique_ptr<AbstractExecutor> &executor) {
  std::vector<Tuple> result;

  Tuple tuple;
  RID rid;
  while (executor->Next(&tuple, &rid)) {
    result.push_back(tuple);
  }

  return result;
}
}  // namespace

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  const AbstractPlanNode *left_node = plan_->GetLeftPlan();
  const AbstractPlanNode *right_node = plan_->GetRightPlan();

  left_executor_->Init();
  right_executor_->Init();

  std::vector<Tuple> left_tuples = GetTuples(left_executor_);
  std::vector<Tuple> right_tuples = GetTuples(right_executor_);

  for (const Tuple &left_tuple : left_tuples) {
    for (const Tuple &right_tuple : right_tuples) {
      bool res = plan_->Predicate()
                     ->EvaluateJoin(&left_tuple, left_node->OutputSchema(), &right_tuple, right_node->OutputSchema())
                     .GetAs<bool>();
      if (res) {
        // TODO(change)
        result_.push_back(left_tuple);
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (result_.empty()) {
    return false;
  }

  *tuple = result_.front();
  *rid = tuple->GetRid();
  result_.pop_front();
  return true;
}

}  // namespace bustub
