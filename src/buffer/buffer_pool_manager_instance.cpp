//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"
#include "common/config.h"
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {


BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  frame_id_t frame_id = FindPageByPageId(page_id);

  if (frame_id == -1) {
    LOG_WARN("Cannot find page_id=%d in buffer pool while we want to flush it.", page_id);
    return false;
  }

  Page& page = pages_[frame_id];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page_id, page.GetData());
    LOG_INFO("Page %d is dirty, writing it back to disk.", page.GetPageId());
  } else {
    LOG_INFO("Page %d is not dirty in FlushPgImp, this is a no-op.", page.GetPageId());
  }

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    Page& page = pages_[i];
    if (page.GetPageId() == INVALID) {
      continue;
    }

    if (page.IsDirty()){
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool all_pages_pinned = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      all_pages_pinned = false;
      break;
    }
  }

  if (all_pages_pinned) {
    return nullptr;
  }

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id = GetVictimPageFromListOrReplacer();
  if (frame_id == -1) {
    LOG_INFO("Cannot get frame id from either replacer or free_list.");
    return nullptr;
  }
  
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_id_t new_page = AllocatePage();
  Page& page = pages_[frame_id];
  // Before reset the memory, make sure the victim's data is written to db.
  FlushPgImp(page.GetPageId());

  page.ResetMemory();
  page.page_id_ = new_page;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = new_page;
  return pages_ + frame_id;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  frame_id_t frame_id = FindPageByPageId(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if (frame_id != -1) {
    return pages_ + frame_id;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t new_frame = GetVictimPageFromListOrReplacer();
  if (new_frame == -1) {
    LOG_WARN("Cannot fetch the page %d, since the buffer pool is full", page_id);
    return nullptr;
  }

  // 2.     If R is dirty, write it back to the disk.
  Page& page = pages_[new_frame];
  if (page.IsDirty()) {
    FlushPgImp(page.GetPageId());
  }
  // 3.     Delete R from the page table and insert P.
  page.ResetMemory();
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page.data_);

  return pages_ + new_frame;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  frame_id_t frame_id = FindPageByPageId(page_id);

  // 1.   If P does not exist, return true.
  if (frame_id == -1) {
    LOG_INFO("When delete the page, we cannot find it in page tables for page %d.", page_id);
    return true;
  }

  Page& page = pages_[frame_id];
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page.GetPinCount() != 0) {
    LOG_INFO("When delete the page,  pin count is %d, cannot delete it.",
      page.GetPinCount());
    return false;
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page.IsDirty()) {
    FlushPgImp(page.page_id_);
  }

  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  frame_id_t frame_id = FindPageByPageId(page_id);
  if (frame_id == -1) {
    LOG_WARN("Cannot unpin the page %d, since it is not in the buffer pool.", page_id);
    return false;
  }

  Page& page = pages_[frame_id];
  if (page.pin_count_ <= 0) {
    LOG_INFO("Unpin a page %d with pin count = 0.", page_id);
    return false;
  }

  page.pin_count_ -= 1;
  page.is_dirty_ = is_dirty;

  if (page.pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  return true; 
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

frame_id_t BufferPoolManagerInstance::GetVictimPageFromListOrReplacer() {
  frame_id_t res = -1;
  int size = free_list_.size();

  // Try to get free page from free_list
  if (size > 0) {
    res = free_list_.back();
    free_list_.pop_back();
    return res;
  }

  // If we cannot get page from free_list, try with lru_replacer
  bool get_page_from_replacer = replacer_->Victim(&res);
  return get_page_from_replacer ? res : -1;
}

frame_id_t BufferPoolManagerInstance::FindPageByPageId(page_id_t page_id) {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      return i;
    }
  }

  return -1;
}

}  // namespace bustub
