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
// Created by ziqi on 2024/8/10.
//

#include "executor_limit.h"

namespace wsdb {
LimitExecutor::LimitExecutor(AbstractExecutorUptr child, int limit)
    : AbstractExecutor(Basic), child_(std::move(child)), limit_(limit), count_(0)
{}

void LimitExecutor::Init() { 
    // WSDB_STUDENT_TODO(l2, t1); 
    child_->Init();
    record_ = child_->GetRecord();
    count_ += 1;
}

void LimitExecutor::Next() { 
    // WSDB_STUDENT_TODO(l2, t1); 
    if (!IsEnd()) {
        child_->Next();
        record_ = child_->GetRecord();
        count_ += 1;
    }
}

[[nodiscard]] auto LimitExecutor::IsEnd() const -> bool { 
    // WSDB_STUDENT_TODO(l2, t1); 
    if (count_ > limit_) return true;
    if (child_->IsEnd()) return true;
    return false;
}

[[nodiscard]] auto LimitExecutor::GetOutSchema() const -> const RecordSchema * { return child_->GetOutSchema(); }
}  // namespace wsdb
