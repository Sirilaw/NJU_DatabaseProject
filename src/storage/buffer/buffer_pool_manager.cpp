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
#include "buffer_pool_manager.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"
#include <mutex>

namespace wsdb {

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, wsdb::LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
    if (REPLACER == "LRUReplacer") {
        replacer_ = std::make_unique<LRUReplacer>();
    } else if (REPLACER == "LRUKReplacer") {
        replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
    } else {
        WSDB_FETAL("Unknown replacer: " + REPLACER);
    }
    for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
        free_list_.push_back(i);
    }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page *
{
    std::lock_guard<std::mutex> lock(latch_);
    auto                        it = page_frame_lookup_.find({fid, pid});
    if (it != page_frame_lookup_.end()) {
        frame_id_t frame_id = it->second;
        frames_[frame_id].Pin();
        replacer_->Pin(frame_id);
        return frames_[frame_id].GetPage();
    }
    frame_id_t frame_id = INVALID_FRAME_ID;
    frame_id            = GetAvailableFrame();
    UpdateFrame(frame_id, fid, pid);
    return frames_[frame_id].GetPage();
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool
{
    std::lock_guard<std::mutex> lock(latch_);

    fid_pid_t page_key{fid, pid};
    auto      it = page_frame_lookup_.find(page_key);
    if (it == page_frame_lookup_.end())
        return false;
    frame_id_t frame_id = it->second;
    if (frames_[frame_id].GetPinCount() == 0) {
        return false;
    }
    frames_[frame_id].Unpin();
    if (frames_[frame_id].GetPinCount() == 0) {
        replacer_->Unpin(frame_id);
    }
    if (is_dirty) {
        frames_[frame_id].SetDirty(true);
    }
    return true;
}

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool
{
    // WSDB_STUDENT_TODO(l1, t2);
    std::lock_guard<std::mutex> lock(latch_);
    fid_pid_t                   page_key{fid, pid};
    auto                        it = page_frame_lookup_.find(page_key);
    if (it == page_frame_lookup_.end()) {
        return true;
    }
    frame_id_t frame_id = it->second;
    if (frames_[frame_id].GetPinCount() > 0) {
        return false;
    }
    frames_[frame_id].Reset();
    replacer_->Unpin(frame_id);
    disk_manager_->WritePage(fid, pid, frames_[frame_id].GetPage()->GetData());
    page_frame_lookup_.erase({fid, pid});
    free_list_.push_back(frame_id);
    return true;
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool
{
    bool                                      flag                   = true;
    std::unordered_map<fid_pid_t, frame_id_t> page_frame_lookup_temp = page_frame_lookup_;
    for (auto it = page_frame_lookup_temp.begin(); it != page_frame_lookup_temp.end(); it++) {
        fid_pid_t fp = it->first;
        if (fp.fid == fid) {
            bool res = DeletePage(fp.fid, fp.pid);
            if (!res)
                flag = false;
        }
    }
    return flag;
}

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool
{
    // WSDB_STUDENT_TODO(l1, t2);
    std::lock_guard<std::mutex> lock(latch_);

    fid_pid_t page_key{fid, pid};
    auto      it = page_frame_lookup_.find(page_key);
    if (it == page_frame_lookup_.end()) {
        return false;
    }
    frame_id_t frame_id = it->second;
    disk_manager_->WritePage(fid, pid, frames_[frame_id].GetPage()->GetData());
    frames_[frame_id].SetDirty(false);
    return true;
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool
{
    bool flag = true;
    for (auto it = page_frame_lookup_.begin(); it != page_frame_lookup_.end(); it++) {
        fid_pid_t fp = it->first;
        if (fp.fid == fid) {
            bool res = FlushPage(fp.fid, fp.pid);
            if (!res) {
                flag = false;
            }
        }
    }
    return flag;
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t
{
    if (!free_list_.empty()) {
        frame_id_t frame_id = free_list_.front();
        free_list_.pop_front();
        return frame_id;
    }
    frame_id_t victim_frame;
    if (replacer_->Victim(&victim_frame)) {
        // page_frame_lookup_.erase({frames_[victim_frame].GetPage()->GetTableId(),
        // frames_[victim_frame].GetPage()->GetPageId()});
        for (auto it = page_frame_lookup_.begin(); it != page_frame_lookup_.end(); it++) {
            if (it->second == victim_frame) {
                page_frame_lookup_.erase(it);
                break;
            }
        }
        return victim_frame;
    }
    WSDB_THROW(WSDB_NO_FREE_FRAME, "");
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid)
{
    // WSDB_STUDENT_TODO(l1, t2);
    Frame &frame = frames_[frame_id];
    if (frame.IsDirty()) {
        disk_manager_->WritePage(
            frame.GetPage()->GetTableId(), frame.GetPage()->GetPageId(), frame.GetPage()->GetData());
        frame.SetDirty(false);
        frame.GetPage()->Clear();
    }
    // Update the frame with the new page
    // page_frame_lookup_.erase({frames_[frame_id].GetPage()->GetTableId(), frames_[frame_id].GetPage()->GetPageId()});
    // frames_[frame_id].Reset();
    disk_manager_->ReadPage(fid, pid, frames_[frame_id].GetPage()->GetData());
    frames_[frame_id].GetPage()->SetTablePageId(fid, pid);
    // Pin the frame in the buffer and the replacer
    frames_[frame_id].Pin();
    replacer_->Pin(frame_id);
    page_frame_lookup_[{fid, pid}] = frame_id;
}

auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame *
{
    const auto it = page_frame_lookup_.find({fid, pid});
    return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

}  // namespace wsdb
