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
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {
namespace {
uint32_t MaskByLocalDepth(uint32_t candidate, uint32_t local_depth) { return ((1 << local_depth) - 1) & candidate; }
}  // namespace

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
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
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
  // table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(directory_page->GetBucketPageId(bucket_index));

  bool res = bucket_page->GetValue(key, comparator_, result);
  PrintDirectory("In GetValue");

  // table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  LOG_INFO("Begin writing.... \n");
  std::cout << "Inserting " << key << " -> " << value << std::endl;
  // Initial call
  if (directory_page->GetGlobalDepth() == 0) {
    bool insert_result = SplitInsert(transaction, key, value);
    // table_latch_.WUnlock();
    return insert_result;
  }

  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);

  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(directory_page->GetBucketPageId(bucket_index));

  if (bucket_page->KeyAndValueExistInArray(key, value, comparator_)){
    LOG_WARN("Key and value has already exsit");
    return false;
  }

  if (bucket_page->IsFull()) {
    std::cout << "Current bucket index " << bucket_index << " is full, need split " << std::endl;
    bool insert_result = SplitInsert(transaction, key, value);
    // table_latch_.WUnlock();
    return insert_result;
  }



  bool insert_result = bucket_page->Insert(key, value, comparator_);
  // table_latch_.WUnlock();
  LOG_INFO("End Writing...\n");
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
    bucket_page->RemoveAllElements();
  }

  PrintDirectory("in SplitInsert");
  pairs_to_add.push_back({key, value});

  // Insert them all.
  for (const auto &the_pair : pairs_to_add) {
    uint32_t bucket_index = KeyToDirectoryIndex(the_pair.first, FetchDirectoryPage());
    HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(FetchDirectoryPage()->GetBucketPageId(bucket_index));
    // All bucket pages should have enough place to add.
    bool res = bucket_page->Insert(the_pair.first, the_pair.second, comparator_);
    if (!res) {
      LOG_ERROR("Cannot insert the element after split");
      std::cout << "key is " << the_pair.first << " " << the_pair.second << std::endl;
      return false;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::CreatePageAndUpdateDirectory(const KeyType &key, const ValueType &value,
                                                   page_id_t *old_full_page_id) {
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  if (GetGlobalDepth() == 0) {
    directory_page->IncrGlobalDepth();
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

    return;
  }

  PrintDirectory("In create page");
  // create one new page is enough
  page_id_t new_bucket_page_id;
  Page *new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  if (new_bucket_page == nullptr) {
    LOG_ERROR("When split insert, cannot allocate new page");
    return;
  } else {
    LOG_INFO("Create a new page");
  }

  // for old page, lsb stay the same
  // for new page, lsb add 1 at the beginning.
  // update old pages
  uint32_t old_bucket_index = KeyToDirectoryIndex(key, directory_page);
  page_id_t old_page_id = directory_page->GetBucketPageId(old_bucket_index);
  *old_full_page_id = old_page_id;

  directory_page->IncrGlobalDepth();

  uint32_t original_local_depth = directory_page->GetLocalDepth(old_bucket_index);
  for (int i = 0; i < DIRECTORY_ARRAY_SIZE; i++) {
    page_id_t page_id = directory_page->GetBucketPageId(i);
    if (page_id == 0){
      LOG_INFO("Page id %d == 0", page_id);
      break;
    }

    if (page_id == old_page_id){
      directory_page->SetLocalDepth(i, original_local_depth + 1);
    }
  }

  // update new pages
  directory_page->SetBucketPageId(cur_pages_count, new_bucket_page_id);
  directory_page->SetLocalDepth(cur_pages_count, original_local_depth + 1);
  lookup_page_lsb_value_[new_bucket_page_id] = (1 << (original_local_depth)) | lookup_page_lsb_value_[old_page_id];

  cur_pages_count++;

  // Update all bucket pointers
  std::unordered_map<page_id_t, uint32_t> page_to_local_depth = GetPageToLocalDepth();
  for (const auto &[page_id, local_depth] : page_to_local_depth) {
    std::cout << "page_to_local_depth table is page id" << page_id << " local depth" << local_depth << std::endl;
  }
  for (const auto &[page_id, lsb_value] : lookup_page_lsb_value_) {
    std::cout << "Lookup table page_id " << page_id << "- lsb_value" << lsb_value << std::endl;
  }

  for (uint32_t i = 0; i < directory_page->Size(); i++) {
    int match = 0;

    for (const auto &[page_id, lsb_value] : lookup_page_lsb_value_) {
      uint32_t lsb_equal = MaskByLocalDepth(i, page_to_local_depth[page_id]) ^ lsb_value;
      if (lsb_equal == 0) {
        directory_page->SetBucketPageId(i, page_id);

        assert(page_to_local_depth.count(page_id) != 0);
        directory_page->SetLocalDepth(i, page_to_local_depth[page_id]);
        match++;
      }
    }

    if (match != 1) {
      LOG_INFO("Macth bucket %d not equal to 1, instead %d", i, match);
    }
  }

  PrintDirectory("end of create page");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::unordered_map<page_id_t, uint32_t> HASH_TABLE_TYPE::GetPageToLocalDepth() {
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  std::unordered_map<page_id_t, uint32_t> res;
  for (uint32_t i = 0; i < directory_page->Size(); i++) {
    page_id_t page_id = directory_page->GetBucketPageId(i);
    if (page_id == 0) {
      LOG_INFO("starting from Page id %d, page id is zero", i);
      break;
    }

    uint32_t local_depth = directory_page->GetLocalDepth(i);
    if (local_depth == 0) {
      LOG_INFO("starting from %d, local depth is 0", i);
      break;
    }

    res.insert({page_id, local_depth});
  }

  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);

  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(directory_page->GetBucketPageId(bucket_index));
  bool res = bucket_page->Remove(key, value, comparator_);
  if (!res) {
    LOG_WARN("Remove element that does not exist");
  }

  Merge(transaction, key, value);
  // table_latch_.WUnlock();
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // TODO(IMPLEMENT THIS)
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintDirectory(std::string msg) {
  return;
  // table_latch_.RLock();
  std::cout << msg << std::endl;
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", GetGlobalDepth());
  LOG_DEBUG("| bucket_idx | page_id | local_depth | num_elements |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << GetGlobalDepth()); idx++) {
    page_id_t page_id = dir_page->GetBucketPageId(idx);
    uint32_t local_depth = dir_page->GetLocalDepth(idx);
    uint32_t num_read = FetchBucketPage(page_id)->NumReadable();

    LOG_DEBUG("|      %u     |     %u     |     %u     |     %u     |", idx, page_id, local_depth, num_read);
  }
  LOG_DEBUG("================ END DIRECTORY ================\n");

  for (const auto& [page_id, lsb] : lookup_page_lsb_value_) {
    std::cout << "Page id" << page_id << " lsb is " << lsb << "\n";

  }

  LOG_DEBUG("================ For Page Begin ================\n");

  std::unordered_map<uint32_t, uint32_t> map;
  for (int i = 0; i < DIRECTORY_ARRAY_SIZE; i++) {
    uint32_t page_id = dir_page->GetBucketPageId(i);
    if (dir_page->GetBucketPageId(i) == 0) {
      break;
    }

    if (map.count(page_id) == 0) {
      map[page_id] = 1;
      HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);
      std::cout << "\n checking page " << page_id << " size is " << bucket_page->NumReadable()  << std::endl;
      std::vector<std::pair<KeyType, ValueType>> res = bucket_page->GetAllElements();
      int count = 0;
      for (const auto &[key, val] : res) {
        std::cout << " key " << key << " val " << val;
        count++;

        if (count == 10) {
          count = 0;
          std::cout << "\n";
        }
      }
    }
  }

  LOG_DEBUG("\n================ For Page End ================\n");

  // table_latch_.RUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  // table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  // table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  // table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  // table_latch_.RUnlock();
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
