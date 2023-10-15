//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <deque>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : name_(name),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      hash_fn_(std::move(hash_fn)),
      cur_pages_count(0) {
  Page *directory_page = buffer_pool_manager_->NewPage(&directory_page_id_, nullptr);
  if (directory_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannott allocate directory page since NewPage fails.");
  }
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash_res = Hash(key);
  uint32_t global_mask = dir_page->GetGlobalDepthMask();
  uint32_t bucket_index = hash_res & global_mask;
  return bucket_index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  directory_page->PrintDirectory();
  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_index);

  bool res = bucket_page->GetValue(key, comparator_, result);

  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();

  // Initial call
  if (directory_page->GetGlobalDepth() == 0) {
    bool insert_result = SplitInsert(transaction, key, value);
    table_latch_.WUnlock();
    return insert_result;
  }

  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);

  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_index);
  if (bucket_page->IsFull()) {
    bool insert_result = SplitInsert(transaction, key, value);
    table_latch_.WUnlock();
    return insert_result;
  }

  bool insert_result = bucket_page->Insert(key, value, comparator_);
  table_latch_.WUnlock();
  return insert_result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // Create new page and update directory metadata
  // Does not collect old elements.
  page_id_t old_page_id;
  CreatePageAndUpdateDirectory(key, value, &old_page_id);

  // Collect all elements and add it back to either old / new pages.
  std::vector<std::pair<KeyType, ValueType>> pairs_to_add = {};
  if (old_page_id != -1) {
    HASH_TABLE_BUCKET_TYPE *bucket_page =
        reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(old_page_id)->GetData());

    pairs_to_add = bucket_page->GetAllElements();
  }

  pairs_to_add.push_back({key, value});

  // Insert them all.
  for (const auto &the_pair : pairs_to_add) {
    std::cout << the_pair.first << " hello wenchao " << the_pair.second << "\n";
    uint32_t bucket_index = KeyToDirectoryIndex(the_pair.first, FetchDirectoryPage());
    HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_index);
    // All bucket pages should have enough place to add.
    bool res = bucket_page->Insert(key, value, comparator_);
    if (!res) {
      LOG_WARN("Cannot insert the element after split");
      return false;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::CreatePageAndUpdateDirectory(const KeyType &key, const ValueType &value,
                                                   page_id_t *old_full_page_id) {
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  directory_page->IncrGlobalDepth();
  uint32_t new_global_depth = directory_page->GetGlobalDepth();

  if (new_global_depth == 1) {
    *old_full_page_id = -1;

    for (int i = 0; i < 2; i++) {
      page_id_t new_bucket_page_id;
      Page *new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
      if (new_bucket_page == nullptr) {
        LOG_ERROR("When split insert, cannot allocate new page");
        return;
      }

      lookup_page_lsb_value_.insert({new_bucket_page_id, i});
      directory_page->SetLocalDepth(i, 1);
      directory_page->SetBucketPageId(cur_pages_count, new_bucket_page_id);
      cur_pages_count++;
    }

  } else {  // create one new page is enough
    page_id_t new_bucket_page_id;
    Page *new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
    if (new_bucket_page == nullptr) {
      LOG_ERROR("When split insert, cannot allocate new page");
      return;
    }

    // for old page, lsb stay the same
    // for new page, lsb add 1 at the beginning.
    // update old pages
    uint32_t old_bucket_index = KeyToDirectoryIndex(key, directory_page);
    page_id_t old_page_id = directory_page->GetBucketPageId(old_bucket_index);
    *old_full_page_id = old_page_id;

    uint8_t original_local_depth = directory_page->GetLocalDepth(old_bucket_index);
    directory_page->SetLocalDepth(old_bucket_index, original_local_depth + 1);

    // update new pages
    directory_page->SetBucketPageId(cur_pages_count, new_bucket_page_id);
    directory_page->SetLocalDepth(cur_pages_count, directory_page->GetLocalDepth(old_bucket_index));
    lookup_page_lsb_value_[new_bucket_page_id] = (1 << original_local_depth) | lookup_page_lsb_value_[old_page_id];

    cur_pages_count++;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);

  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_index);
  bool res = bucket_page->Remove(key, value, comparator_);
  if (!res) {
    LOG_WARN("Remove element that does not exist");
  }

  Merge(transaction, key, value);
  table_latch_.WUnlock();
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // TODO(IMPLEMENT THIS)
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
