//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  latch_.lock();

  // 1.     Search the page table for the requested page (P).
  std::unordered_map<page_id_t, frame_id_t>::const_iterator page = page_table_.find (page_id);
  Page *page_;
  frame_id_t frame_id;

  if (page != page_table_.end()){
    frame_id = page->second;

    // 1.1    If P exists, pin it and return it immediately.
    replacer_->Pin(frame_id);
    page_ = &pages_[frame_id];
    page_->pin_count_++;

    latch_.unlock();
    return page_;
  } else {
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    if(!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
      page_ = &pages_[frame_id];
    } else {
      if(!replacer_->Victim(&frame_id)) {
        latch_.unlock();
        return nullptr;
      }

      page_ = &pages_[frame_id];

      // 2.     If R is dirty, write it back to the disk.
      if(page_->IsDirty()) {
        disk_manager_->WritePage(page_->GetPageId(), page_->GetData());
      }

      // 3.     Delete R from the page table and insert P.
      page_table_.erase(page_->GetPageId());
    }

    std::pair<page_id_t, frame_id_t> elem (page_id,frame_id);
    page_table_.insert(elem);

    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    disk_manager_->ReadPage(page_id, page_->data_);
    page_->page_id_ = page_id;
    page_->pin_count_ = 1;
    page_->is_dirty_ = false;

    latch_.unlock();
    return page_;
  } 
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();

  // 1.     Search the page table for the requested page (P).
  std::unordered_map<page_id_t, frame_id_t>::const_iterator page = page_table_.find (page_id);

  if(page == page_table_.end()) {
    // 1.1    If P does not exist, return false.
    latch_.unlock();
    return false;
  } else {
    frame_id_t frame_id = page->second;
    Page *page_ = &pages_[frame_id];

    // 1.2    If P does exist but is not currently pinned, return false.
    if(page_->GetPinCount() <= 0) {
      latch_.unlock();
      return false;
    }

    // 2.     Update P's metadata and unpin from LRU is pin count is zero.
    if(!page_->IsDirty()) {
      page_->is_dirty_ = is_dirty;
    }
    page_->pin_count_--;

    if(page_->GetPinCount() == 0) {
      replacer_->Unpin(frame_id);
    }

    latch_.unlock();
    return true;
  }
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  latch_.lock();
  std::unordered_map<page_id_t, frame_id_t>::const_iterator page = page_table_.find (page_id);

  if(page == page_table_.end()) {
    // 1.1    If P does not exist, return false.
    latch_.unlock();
    return false;
  } else {
    frame_id_t frame_id = page->second;
    Page *page_ = &pages_[frame_id];

    // 2.     If R is dirty, write it back to the disk.
    if(page_->IsDirty()) {
      disk_manager_->WritePage(page_->GetPageId(), page_->GetData());
    }

    // 3.     Update P's metadata and return true
    page_->is_dirty_ = false;

    latch_.unlock();
    return true;
  }
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  latch_.lock();

  // 0.   Make sure you call DiskManager::AllocatePage!
  frame_id_t frame_id;
  Page *page_;

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if(free_list_.empty() && replacer_->Size() <= 0) {
    latch_.unlock();
    return nullptr;
  }

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page_ = &pages_[frame_id];
  } else {
    if(!replacer_->Victim(&frame_id)) {
      latch_.unlock();
      return nullptr;
    }

    page_ = &pages_[frame_id];

    if(page_->IsDirty()) {
      disk_manager_->WritePage(page_->GetPageId(), page_->GetData());
    }

    page_table_.erase(page_->GetPageId());
  } 

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_id_t page_id_ = disk_manager_->AllocatePage();

  page_->page_id_ = page_id_;
  page_->pin_count_ = 1;
  page_->is_dirty_ = false;
  page_->ResetMemory();

  std::pair<page_id_t, frame_id_t> elem (page_id_,frame_id);
  page_table_.insert(elem);

  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = page_id_;
  latch_.unlock();
  return page_;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  latch_.lock();
  std::unordered_map<page_id_t, frame_id_t>::const_iterator page = page_table_.find (page_id);

  if(page == page_table_.end()) {
    // 1.1   If P does not exist, return true.
    latch_.unlock();
    return true;
  } else {
    frame_id_t frame_id = page->second;
    Page *page_ = &pages_[frame_id];

    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    if(page_->GetPinCount() != 0) {
      latch_.unlock();
      return false;
    }

    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    page_table_.erase(page_id);

    page_->page_id_ = INVALID_PAGE_ID;
    page_->pin_count_ = 0;
    page_->is_dirty_ = false;
    page_->ResetMemory();

    replacer_->Pin(frame_id);
    free_list_.emplace_back(static_cast<int>(frame_id));
    disk_manager_->DeallocatePage(page_id);

    latch_.unlock();
    return true;
  }
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (std::pair<page_id_t, frame_id_t> element : page_table_)
  {
    page_id_t page_id = element.first;
    this->FlushPageImpl(page_id);
  }
}

}  // namespace bustub
