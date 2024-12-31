
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
// Created by ziqi on 2024/7/18.
//

#include "executor_update.h"

namespace wsdb {

UpdateExecutor::UpdateExecutor(AbstractExecutorUptr child, TableHandle *tbl, std::list<IndexHandle *> indexes,
    std::vector<std::pair<RTField, ValueSptr>> updates)
    : AbstractExecutor(DML),
      child_(std::move(child)),
      tbl_(tbl),
      indexes_(std::move(indexes)),
      updates_(std::move(updates)),
      is_end_(false)
{
  std::vector<RTField> fields(1);
  fields[0]   = RTField{.field_ = {.field_name_ = "updated", .field_size_ = sizeof(int), .field_type_ = TYPE_INT}};
  out_schema_ = std::make_unique<RecordSchema>(fields);
}

void UpdateExecutor::Init() { WSDB_FETAL("UpdateExecutor does not support Init"); }

void UpdateExecutor::Next()
{
  // number of updated records
  int count = 0;

  // WSDB_STUDENT_TODO(l2, t1);
  for (child_->Init(); !child_->IsEnd(); child_->Next()) {
    auto old_record = child_->GetRecord();
    const RecordSchema* schema = old_record->GetSchema();
    std::vector<ValueSptr> new_values;
    for (size_t i = 0; i < schema->GetFieldCount(); i++) {
        auto field = schema->GetFieldAt(i);
        bool updated = false;

        for (const auto& update: updates_) {
            if (update.first.field_.field_name_ == field.field_.field_name_) {
                new_values.push_back(update.second);
                updated = true;
                break;
            }
        }

        if (!updated) {
            new_values.push_back(old_record->GetValueAt(i));
        }
    }

    auto new_record = std::make_unique<Record>(schema, new_values, old_record->GetRID());

    tbl_->UpdateRecord(old_record->GetRID(), *new_record);

    for (auto *index:indexes_) {
        index->UpdateRecord(*old_record, *new_record);
    }

    count++;
  }

  std::vector<ValueSptr> values{ValueFactory::CreateIntValue(count)};
  record_ = std::make_unique<Record>(out_schema_.get(), values, INVALID_RID);
  is_end_ = true;
}

auto UpdateExecutor::IsEnd() const -> bool { return is_end_; }

}  // namespace wsdb