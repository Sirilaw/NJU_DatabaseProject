/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/17.
//

#include "lru_k_replacer.h"
#include "common/config.h"
#include "../common/error.h"
#include <algorithm>
#define ull unsigned long long 

namespace wsdb {

LRUKReplacer::LRUKReplacer(size_t k) : max_size_(BUFFER_POOL_SIZE), k_(k) {}

auto LRUKReplacer::Victim(frame_id_t *frame_id) -> bool
{
    std::lock_guard<std::mutex> lock(latch_);  // grant the latch
    if (cur_size_ == 0) return false;
    for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
        frame_id_t frameId = it->first;
        LRUKNode  &knode   = node_store_[frameId];
        if (knode.IsEvictable()) {
            *frame_id = frameId;
            lru_list_.erase(it);
            node_store_.erase(frameId);
            cur_size_--;
            break;
        }
    }
    return true;
}

void LRUKReplacer::Pin(frame_id_t frame_id)
{
    // WSDB_STUDENT_TODO(l1, f1);
    // 综合一种情况，但实际是两种细分情况（是否为inf distance）
    std::lock_guard<std::mutex> lock(latch_);
    cur_ts_++; // Pin了一次，则访问序列++
    if (node_store_.find(frame_id) != node_store_.end()) {
        LRUKNode& knode = node_store_[frame_id];
        knode.AddHistory(cur_ts_);
        ull back_k = knode.GetBackwardKDistance(cur_ts_);
        if (knode.IsEvictable() == true) {
            knode.SetEvictable(false);
            cur_size_--;
        }
        // node_store_[frame_id] = knode;
        for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
            if (it->first == frame_id) {
                lru_list_.erase(it);
                break;
            }
        }
        frame_distance fd(frame_id, back_k);
        auto upper = std::upper_bound(lru_list_.begin(), lru_list_.end(), fd, CmpDistance);
        lru_list_.insert(upper, fd);
    }
    else { // node_store里面已经没存对应的frame_id
        LRUKNode knode(frame_id, k_);
        knode.AddHistory(cur_ts_);
        ull back_k = knode.GetBackwardKDistance(cur_ts_);
        node_store_[frame_id] = knode;
        if (lru_list_.size() >= max_size_) {
            frame_id_t v;
            if (Victim(&v)) {
                frame_distance fd(frame_id, back_k);                
                auto upper = std::upper_bound(lru_list_.begin(), lru_list_.end(), fd, CmpDistance);
                lru_list_.insert(upper, fd);
            }
        }
        else {
            frame_distance fd(frame_id, back_k);
            auto upper = std::upper_bound(lru_list_.begin(), lru_list_.end(), fd, CmpDistance);
            lru_list_.insert(upper, fd);
        }
    }
}

void LRUKReplacer::Unpin(frame_id_t frame_id) { 
    // WSDB_STUDENT_TODO(l1, f1); 
    std::lock_guard<std::mutex> lock(latch_);
    if (node_store_.find(frame_id) == node_store_.end()) return;
    LRUKNode& knode = node_store_[frame_id];
    if (knode.IsEvictable() == false) {
        knode.SetEvictable(true);
        cur_size_++;
    }
}

auto LRUKReplacer::Size() -> size_t { 
    return cur_size_;    
}

auto LRUKReplacer::CmpDistance(const LRUKReplacer::frame_distance& fd1, const LRUKReplacer::frame_distance& fd2) -> bool {
    return fd1.second < fd2.second;
}

}  // namespace wsdb
