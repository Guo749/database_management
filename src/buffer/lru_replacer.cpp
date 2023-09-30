//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {
namespace {
// void SwapValue(std::vector<frame_id_t>& vec, int i, int j){
//     int vec_size = vec.size();
//     if (i < 0 || j < 0 || i >= vec_size || j >= vec_size){
//         LOG_ERROR("Cannot swap the value in vector, position i=%d, j=%d", i, j);
//         return;
//     }
//     frame_id_t temp = vec[i];
//     vec[i] = vec[j];
//     vec[j] = temp;
// }
}  // namespace

LRUReplacer::LRUReplacer(size_t num_pages) : 
    lru_cache_(num_pages, -1), 
    current_element_(0){}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
 if (current_element_ == 0) {
    frame_id = nullptr;
    LOG_INFO("No element can be evicted,");
    return false;
 }

 *frame_id = lru_cache_[0];
 for (int i = 0; i < current_element_ - 1; i++){
    lru_cache_[i] = lru_cache_[i + 1];
 }

 current_element_ -= 1;
 return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    for (int i = 0; i < current_element_; i++) {
        if (lru_cache_[i] == frame_id) {
            lru_cache_.erase(lru_cache_.begin() + i);
            current_element_ -= 1;
            break;
        }
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    for (int i = 0; i < current_element_; i++) {
        // If we pined it before, we do nothing.
        if (lru_cache_[i] == frame_id) {
            return;
        }
    }

    // Now the page is no where
    // Check if we have the capacity to add
    int cache_size = lru_cache_.size();
    if (current_element_ == cache_size)  {
        LOG_WARN("Capacity is full when unpin %d", frame_id);
        return;
    }

    lru_cache_[current_element_] = frame_id;
    current_element_++;
}

size_t LRUReplacer::Size() { 
    return current_element_;
}

}  // namespace bustub
