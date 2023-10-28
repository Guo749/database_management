//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {
// For one char, it is 1 byte, which is 8 bits
constexpr uint32_t kCharBit = 8;

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::KeyExistInArray(KeyType key_type, KeyComparator cmp) {
  for (unsigned long i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(KeyAt(i), key_type) == 0) {
        return true;
      }
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::KeyAndValueExistInArray(KeyType key_type, ValueType value_type, KeyComparator cmp) {
  for (unsigned long i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if ((cmp(KeyAt(i), key_type) == 0) && ValueAt(i) == value_type) {
        return true;
      }
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool found_res = false;
  for (unsigned long i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(KeyAt(i), key) == 0) {
        result->push_back(ValueAt(i));
        found_res = true;
      }
    }
  }
  return found_res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsFull() || KeyAndValueExistInArray(key, value, cmp)) {
    LOG_WARN("Cannot insert element since it is full or already exist");
    return false;
  }

  // Find one empty slot
  for (unsigned long i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      LOG_INFO("Inserting element into %zu", i);
      array_[i].first = key;
      array_[i].second = value;

      SetOccupied(i);
      SetReadable(i);
      return true;
    }
  }

  LOG_WARN("Should not reach here, meaning cannot find one empty slot while not detected at first place.");
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  // Not found.
  // std::cout << "IsEmpty() " << IsEmpty();
  // std::cout << KeyExistInArray(key, cmp) << std::endl;
  if (IsEmpty() || !KeyExistInArray(key, cmp)) {
    return false;
  }

  for (unsigned long i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsOccupied(i) && IsReadable(i)) {
      if (cmp(key, KeyAt(i)) == 0) {
        // if i = 3,
        // 1 << (i % kCharBit) = 1000
        // ~ = 0111
        char mask = ~(1 << (i % kCharBit));
        readable_[i / kCharBit] &= mask;
        occupied_[i / kCharBit] &= mask;
        return true;
      }
    }
  }

  LOG_WARN("Should not reach here, all elements are either not occupied nor readable.");
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].first;
  }

  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].second;
  }

  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (IsOccupied(bucket_idx)) {
    readable_[bucket_idx / kCharBit] &= (1 << (bucket_idx % kCharBit));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return ((occupied_[bucket_idx / kCharBit]) >> (bucket_idx % kCharBit)) & 0x01;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / kCharBit] |= 1 << (bucket_idx % kCharBit);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return ((readable_[bucket_idx / kCharBit]) >> (bucket_idx % kCharBit)) & 0x01;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / kCharBit] |= 1 << (bucket_idx % kCharBit);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  for (unsigned long i = 0; i < OCCUPIED_ARRAY_SIZE; i++) {
    if ((occupied_[i] & 0xFF) != 0xFF) {
      return false;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t res = 0;
  for (unsigned long i = 0; i < OCCUPIED_ARRAY_SIZE; i++) {
    char cur_char = readable_[i];
    for (int i = 0; i < 8; i++) {
      if ((cur_char & (1 << i)) != 0) {
        res++;
      }
    }
  }

  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (unsigned long i = 0; i < OCCUPIED_ARRAY_SIZE; i++) {
    if (occupied_[i] != 0X00) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::vector<std::pair<KeyType, ValueType>> HASH_TABLE_BUCKET_TYPE::GetAllElements() {
  std::vector<std::pair<KeyType, ValueType>> res = {};
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      res.push_back(array_[i]);
    }
  }

  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAllElements() {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    occupied_[i] = 0;
    readable_[i] = 0;
  }

  if (!IsEmpty()) {
    LOG_ERROR("Remove all elements while IsEmpty() flag does not indicate this.");
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u\n", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
