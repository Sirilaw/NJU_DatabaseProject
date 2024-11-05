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

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
using namespace std;

namespace wsdb {

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {} //构造函数 不用管

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex> lock(latch_); // grant the latch
    if (lru_list_.empty())  // if lru_list has no frame
        return false;
    auto it = lru_list_.begin();
    while (it != lru_list_.end()) {
        if (it->second) break;
        it++;
    }
    if (it == lru_list_.end()) return false; // 如果没有找到可以被抛弃的frame，返回false
    *frame_id = it->first; 
    lru_hash_.erase(it->first);
    lru_list_.erase(it);
    cur_size_--;
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    /* Pin的逻辑：
    1.在hash中找frame_id项，找不到直接返回
    2.Pin视为访问，则需要把frame_id对应的list中的项先清除（如已经有），然后插入list末尾
    3.在lru_hash中填入插入位置对应的iterator
    4.处理cur_size（如果cur_size当前为0，则说明是从空开始pin的，则不变; 如果cur_size > 0，则说明是unpin过后才开始pin的
    又因为hash有对应项的时候才会实际“pin”，所以在list中必定有对应项，则cur_size--即可）
    */
    std::lock_guard<std::mutex> lock(latch_); // grant the latch
    auto hash_it = lru_hash_.find(frame_id);

    if (hash_it == lru_hash_.end()) return; // 如果frame_id没在hash_map，则直接返回

    for (auto i = lru_list_.begin(); i != lru_list_.end(); i++) { // 删除旧位置
        if (i->first == frame_id) {
            lru_list_.remove(*i); // 感觉不太优雅，但没想出来别的写法
            break;
        }
    }

    lru_list_.push_back({frame_id, false}); // 添加新位置
    auto frame_it = std::prev(lru_list_.end());
    lru_hash_[frame_id] = frame_it;
    if (cur_size_ > 0) cur_size_--;
}

void LRUReplacer::Unpin(frame_id_t frame_id) { 
    /* Unpin的逻辑：
    1. 看lru_list中有无frame_id对应的项
        (1)有 若second=false,则改为true,cur_size++, return;若second=true, return
        (2)无 则转到2
    2. 先插入到lru_list的尾部；然后建立对应的hash项
    3. cur_size++
    */
    std::lock_guard<std::mutex> lock(latch_); // grant the latch
    for (auto i = lru_list_.begin(); i != lru_list_.end(); i++) {
        if (i->first == frame_id) {
            if (i->second == false) {
                i->second = true;
                cur_size_++;
            }
            return;
        }
    }
    lru_list_.push_back({frame_id, true}); // 添加新位置
    auto frame_it = std::prev(lru_list_.end());
    lru_hash_[frame_id] = frame_it;
    cur_size_++;
}

auto LRUReplacer::Size() -> size_t { 
    // WSDB_STUDENT_TODO(l1, t1); 
    std::lock_guard<std::mutex> lock(latch_); // grant the latch
    return cur_size_;
}

}  // namespace wsdb
