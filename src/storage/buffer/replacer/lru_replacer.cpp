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

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}  // 构造函数 不用管

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool
{
    std::lock_guard<std::mutex> lock(latch_);  // grant the latch
    // if lru_list has no frame
    // 一定要注意这里能不能写lru_list.empty()！
    // 因为后面会发现cur_size并不是和lru_list的增删元素同步变化的……
    // byd t1, t2因为这个卡了n天……
    if (cur_size_ == 0)
        return false;
    lru_list_.reverse();
    for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
        if (it->second) {
            *frame_id = it->first;
            lru_list_.erase(it);
            lru_hash_.erase(it->first);
            cur_size_--;
            break;
        }
    }
    lru_list_.reverse();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id)
{
    /* Pin的逻辑：
    1.在hash中找frame_id项，找不到直接返回
    2.Pin视为访问，则需要把frame_id对应的list中的项先清除（如已经有），然后插入list末尾
    3.在lru_hash中填入插入位置对应的iterator
    4.处理cur_size（如果cur_size当前为0，则说明是从空开始pin的，则不变; 如果cur_size > 0，则说明是unpin过后才开始pin的
    又因为hash有对应项的时候才会实际“pin”，所以在list中必定有对应项，则cur_size--即可）
    */
    std::lock_guard<std::mutex> lock(latch_);  // grant the latch
    if (lru_hash_.find(frame_id) != lru_hash_.end()) {
        auto it = lru_hash_[frame_id];
        if (it->second == true) {
            it->second = false;
            cur_size_--;
        }
        lru_list_.splice(lru_list_.begin(), lru_list_, it);
    } else {
        if (lru_list_.size() < max_size_) {
            lru_list_.emplace_front(frame_id, false);
            lru_hash_[frame_id] = lru_list_.begin();
        } else {
            frame_id_t v;
            if (Victim(&v)) {
                lru_list_.emplace_front(frame_id, false);
                lru_hash_[frame_id] = lru_list_.begin();
            }
        }
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id)
{
    /* Unpin的逻辑：
    1. 看lru_list中有无frame_id对应的项
        (1)有 若second=false,则改为true,cur_size++, return; 若second=true, return
        (2)无 则转到2
    2. 先插入到lru_list的尾部；然后建立对应的hash项
    3. cur_size++
    */
    std::lock_guard<std::mutex> lock(latch_);  // grant the latch
    // for (auto i = lru_list_.begin(); i != lru_list_.end(); i++) {
    //     if (i->first == frame_id) {
    //         if (i->second == false) {
    //             i->second = true;
    //             cur_size_++;
    //         }
    //         return;
    //     }
    // }
    // lru_list_.push_back({frame_id, true}); // 添加新位置
    // auto frame_it = std::prev(lru_list_.end());
    // lru_hash_[frame_id] = frame_it;
    // cur_size_++;

    if (lru_hash_.find(frame_id) == lru_hash_.end())
        return;
    auto it = lru_hash_[frame_id];
    if (it->second == false) {
        it->second = true;
        cur_size_++;
    }
}

auto LRUReplacer::Size() -> size_t
{
    // WSDB_STUDENT_TODO(l1, t1);
    // size_t res = 0;
    // for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
    //     if (it->second) {
    //         res += 1;
    //     }
    // }
    // return res;
    return cur_size_;
}

}  // namespace wsdb
