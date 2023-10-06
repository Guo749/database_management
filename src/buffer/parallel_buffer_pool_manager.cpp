//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "buffer/buffer_pool_manager_instance.h"
#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances), pool_size_(pool_size), locks_(num_instances), candidate_instance_index_(0) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances; i++) {
    buffer_pool_manager_instances_.emplace_back(
        std::make_unique<BufferPoolManagerInstance>(pool_size, num_instances, i, disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pool_manager_instances_[page_id % num_instances_].get();
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);

  int instance_index = page_id % num_instances_;
  std::lock_guard<std::mutex> lock(locks_[instance_index]);
  return buffer_pool_manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);

  int instance_index = page_id % num_instances_;
  std::lock_guard<std::mutex> lock(locks_[instance_index]);
  return buffer_pool_manager->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);

  int instance_index = page_id % num_instances_;
  std::lock_guard<std::mutex> lock(locks_[instance_index]);
  return buffer_pool_manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  // At one time, only one call to NewPg is allowed, to guard the candidate_instance_index_.
  std::lock_guard<std::mutex> new_page_lock_guard(new_page_lock_);
  for (size_t i = 0; i < num_instances_; i++) {
    size_t candidate_index = (candidate_instance_index_ + i) % num_instances_;
    std::lock_guard<std::mutex> instance_lock_guard(locks_[candidate_index]);

    std::unique_ptr<BufferPoolManager> &manager_instance = buffer_pool_manager_instances_[candidate_index];
    Page *page_res = manager_instance->NewPage(page_id);

    // Allocation fails, call next instance.
    if (page_res == nullptr) {
      LOG_INFO("Allocation from  %zuth instance fails, find next one.", candidate_index);
      continue;
    } else {
      // Update the index, not just get pages from the current instance.
      // Next round will begin from a brand new instance.
      candidate_index = (candidate_index + 1) % num_instances_;
      return page_res;
    }
  }

  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);

  int instance_index = page_id % num_instances_;
  std::lock_guard<std::mutex> lock(locks_[instance_index]);
  return buffer_pool_manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    std::lock_guard<std::mutex> lock_guard(locks_[i]);
    buffer_pool_manager_instances_[i]->FlushAllPages();
  }
}

}  // namespace bustub
