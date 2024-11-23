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
// Created by ziqi on 2024/7/19.
//

#include "table_handle.h"
namespace wsdb {

TableHandle::TableHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t table_id,
    TableHeader &hdr, RecordSchemaUptr &schema, StorageModel storage_model)
    : tab_hdr_(hdr),
      table_id_(table_id),
      disk_manager_(disk_manager),
      buffer_pool_manager_(buffer_pool_manager),
      schema_(std::move(schema)),
      storage_model_(storage_model)
{
    // set table id for table handle;
    schema_->SetTableId(table_id_);
    if (storage_model_ == PAX_MODEL) {
        field_offset_.resize(schema_->GetFieldCount());
        // calculate offsets of fields
        WSDB_STUDENT_TODO(l1, f2);
    }
}

auto TableHandle::GetRecord(const RID &rid) -> RecordUptr
{
    auto nullmap = std::make_unique<char[]>(tab_hdr_.nullmap_size_);
    auto data    = std::make_unique<char[]>(tab_hdr_.rec_size_);
    // WSDB_STUDENT_TODO(l1, t3);
    auto  page_handle = FetchPageHandle(rid.PageID());
    char *bitmap      = page_handle->GetBitmap();
    bool  slot_bit    = BitMap::GetBit(bitmap, rid.SlotID());
    if (!slot_bit) {
        buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
        WSDB_THROW(WSDB_RECORD_MISS, "");
    } else {
        page_handle->ReadSlot(rid.SlotID(), nullmap.get(), data.get());
        buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
        auto returnRecord = std::make_unique<Record>(schema_.get(), nullmap.get(), data.get(), rid);
        return returnRecord;
    }
}

auto TableHandle::GetChunk(page_id_t pid, const RecordSchema *chunk_schema) -> ChunkUptr { WSDB_STUDENT_TODO(l1, f2); }

auto TableHandle::InsertRecord(const Record &record) -> RID
{
    // WSDB_STUDENT_TODO(l1, t3);
    // create a new page handle
    auto newPageHandle = CreatePageHandle();
    // get an empty slot in the page
    char *bitmap     = newPageHandle->GetBitmap();
    auto  empty_slot = BitMap::FindFirst(bitmap, tab_hdr_.rec_per_page_, 0, false);
    newPageHandle->WriteSlot(empty_slot, record.GetNullMap(), record.GetData(), false);
    // update bitmap and number of records
    BitMap::SetBit(bitmap, empty_slot, true);
    size_t curRecordNum = newPageHandle->GetPage()->GetRecordNum();
    newPageHandle->GetPage()->SetRecordNum(++curRecordNum);
    tab_hdr_.rec_num_++;
    // if the page is full after inserting
    if (curRecordNum == tab_hdr_.rec_per_page_) {
        tab_hdr_.first_free_page_ = newPageHandle->GetPage()->GetNextFreePageId();
        newPageHandle->GetPage()->SetNextFreePageId(INVALID_PAGE_ID);
    }
    // is_dirty should be set true because insert record to a page makes the page dirty
    page_id_t page_id = newPageHandle->GetPage()->GetPageId();
    buffer_pool_manager_->UnpinPage(table_id_, page_id, true);
    return RID(page_id, empty_slot);
}

void TableHandle::InsertRecord(const RID &rid, const Record &record)
{
    if (rid.PageID() == INVALID_PAGE_ID) {
        WSDB_THROW(WSDB_PAGE_MISS, fmt::format("Page: {}", rid.PageID()));
    }
    // WSDB_STUDENT_TODO(l1, t3);
    auto  page_handle = FetchPageHandle(rid.PageID());
    char *bitmap      = page_handle->GetBitmap();
    if (BitMap::GetBit(bitmap, rid.SlotID())) {
        WSDB_THROW(WSDB_RECORD_EXISTS, "");
    }
    page_handle->WriteSlot(rid.SlotID(), record.GetNullMap(), record.GetData(), false);
    BitMap::SetBit(bitmap, rid.SlotID(), true);
    size_t curRecordNum = page_handle->GetPage()->GetRecordNum();
    page_handle->GetPage()->SetRecordNum(++curRecordNum);
    tab_hdr_.rec_num_++;
    Page *page = page_handle->GetPage();
    if (curRecordNum == tab_hdr_.rec_per_page_) {
        if (tab_hdr_.first_free_page_ == page->GetPageId()) {
            // 如果这个page就是本来的first_free_page，那么该page需要从free page的list中删掉
            tab_hdr_.first_free_page_ = page->GetNextFreePageId();
            page->SetNextFreePageId(INVALID_PAGE_ID);
        } else {
            // 否则，需要先找到这个page前面的那个free_page X, X的next free page设置为这个page的next free page
            auto prev = FetchPageHandle(tab_hdr_.first_free_page_);
            while (prev->GetPage()->GetNextFreePageId() != page->GetPageId()) {
                prev = FetchPageHandle(prev->GetPage()->GetNextFreePageId());
            }
            prev->GetPage()->SetNextFreePageId(page->GetNextFreePageId());
            page->SetNextFreePageId(INVALID_PAGE_ID);
        }
    }
    page_id_t page_id = page->GetPageId();
    buffer_pool_manager_->UnpinPage(table_id_, page_id, true);
}

void TableHandle::DeleteRecord(const RID &rid)
{
    // WSDB_STUDENT_TODO(l1, t3);
    slot_id_t slot_id     = rid.SlotID();
    auto      page_handle = FetchPageHandle(rid.PageID());
    char     *bitmap      = page_handle->GetBitmap();
    if (BitMap::GetBit(bitmap, slot_id) == false) {
        buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
        WSDB_THROW(WSDB_RECORD_MISS, "");
    }
    BitMap::SetBit(bitmap, slot_id, false);
    size_t curRecordNum = page_handle->GetPage()->GetRecordNum();
    page_handle->GetPage()->SetRecordNum(--curRecordNum);
    tab_hdr_.rec_num_--;
    Page *page = page_handle->GetPage();
    if (curRecordNum < tab_hdr_.rec_per_page_) {
        page->SetNextFreePageId(tab_hdr_.first_free_page_);
        tab_hdr_.first_free_page_ = page->GetPageId();
    }
    buffer_pool_manager_->UnpinPage(table_id_, page->GetPageId(), true);
}

void TableHandle::UpdateRecord(const RID &rid, const Record &record)
{
    // WSDB_STUDENT_TODO(l1, t3);
    slot_id_t slot_id     = rid.SlotID();
    auto      page_handle = FetchPageHandle(rid.PageID());
    char     *bitmap      = page_handle->GetBitmap();
    if (BitMap::GetBit(bitmap, slot_id) == false) {
        buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
        WSDB_THROW(WSDB_RECORD_MISS, "");
    }
    page_handle->WriteSlot(slot_id, record.GetNullMap(), record.GetData(), true);
    buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), true);
}

auto TableHandle::FetchPageHandle(page_id_t page_id) -> PageHandleUptr
{
    auto page = buffer_pool_manager_->FetchPage(table_id_, page_id);
    return WrapPageHandle(page);
}

auto TableHandle::CreatePageHandle() -> PageHandleUptr
{
    if (tab_hdr_.first_free_page_ == INVALID_PAGE_ID) {
        return CreateNewPageHandle();
    }
    auto page = buffer_pool_manager_->FetchPage(table_id_, tab_hdr_.first_free_page_);
    return WrapPageHandle(page);
}

auto TableHandle::CreateNewPageHandle() -> PageHandleUptr
{
    auto page_id = static_cast<page_id_t>(tab_hdr_.page_num_);
    tab_hdr_.page_num_++;
    auto page   = buffer_pool_manager_->FetchPage(table_id_, page_id);
    auto pg_hdl = WrapPageHandle(page);
    page->SetNextFreePageId(tab_hdr_.first_free_page_);
    tab_hdr_.first_free_page_ = page_id;
    return pg_hdl;
}

auto TableHandle::WrapPageHandle(Page *page) -> PageHandleUptr
{
    switch (storage_model_) {
        case StorageModel::NARY_MODEL: return std::make_unique<NAryPageHandle>(&tab_hdr_, page);
        case StorageModel::PAX_MODEL:
            return std::make_unique<PAXPageHandle>(&tab_hdr_, page, schema_.get(), field_offset_);
        default: WSDB_FETAL("Unknown storage model");
    }
}

auto TableHandle::GetTableId() const -> table_id_t { return table_id_; }

auto TableHandle::GetTableHeader() const -> const TableHeader & { return tab_hdr_; }

auto TableHandle::GetSchema() const -> const RecordSchema & { return *schema_; }

auto TableHandle::GetTableName() const -> std::string
{
    auto file_name = disk_manager_->GetFileName(table_id_);
    return OBJNAME_FROM_FILENAME(file_name);
}

auto TableHandle::GetStorageModel() const -> StorageModel { return storage_model_; }

auto TableHandle::GetFirstRID() -> RID
{
    auto page_id = FILE_HEADER_PAGE_ID + 1;
    while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
        auto pg_hdl = FetchPageHandle(page_id);
        auto id     = BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, true);
        if (id != tab_hdr_.rec_per_page_) {
            buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
            return {page_id, static_cast<slot_id_t>(id)};
        }
        buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
        page_id++;
    }
    return INVALID_RID;
}

auto TableHandle::GetNextRID(const RID &rid) -> RID
{
    auto page_id = rid.PageID();
    auto slot_id = rid.SlotID();
    while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
        auto pg_hdl = FetchPageHandle(page_id);
        slot_id =
            static_cast<slot_id_t>(BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, slot_id + 1, true));
        if (slot_id == static_cast<slot_id_t>(tab_hdr_.rec_per_page_)) {
            buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
            page_id++;
            slot_id = -1;
        } else {
            buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
            return {page_id, static_cast<slot_id_t>(slot_id)};
        }
    }
    return INVALID_RID;
}

auto TableHandle::HasField(const std::string &field_name) const -> bool
{
    return schema_->HasField(table_id_, field_name);
}

}  // namespace wsdb
