//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  BPlusTreePage::SetPageId(page_id);
  BPlusTreePage::SetParentPageId(parent_id);
  BPlusTreePage::SetSize(0);
  BPlusTreePage::SetMaxSize(max_size);
  BPlusTreePage::SetPageType(IndexPageType::LEAF_PAGE);
  next_page_id_ = INVALID_PAGE_ID;
  prev_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper methods to set/get prev page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrevPageId() const { return prev_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrevPageId(page_id_t prev_page_id) { prev_page_id_ = prev_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    KeyType k = array[i].first;

    int compare = comparator(k, key);

    if (compare >= 0) {
      return i;
    }
  }

  return BPlusTreePage::GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const { return array[index].first; }

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) { return array[index]; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int size = BPlusTreePage::GetSize();

  if (size == 0) {
    array[0].first = key;
    array[0].second = value;
  } else {
    int index = KeyIndex(key, comparator);

    for (int i = size; i > index; i--) {
      array[i] = array[i - 1];
    }

    array[index].first = key;
    array[index].second = value;
  }

  BPlusTreePage::IncreaseSize(1);

  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int curr_size = BPlusTreePage::GetSize();
  int split_index = (curr_size + 1) / 2;
  int copy_size = curr_size - split_index;
  BPlusTreePage::IncreaseSize(-copy_size);

  recipient->CopyNFrom(array + split_index, copy_size);
  recipient->SetNextPageId(next_page_id_);
  recipient->SetPrevPageId(BPlusTreePage::GetPageId());

  if (next_page_id_ != INVALID_PAGE_ID) {
    Page *next = buffer_pool_manager->FetchPage(next_page_id_);
    BPlusTreeLeafPage *next_page = reinterpret_cast<BPlusTreeLeafPage *>(next->GetData());
    next_page->SetPrevPageId(recipient->BPlusTreePage::GetPageId());
    buffer_pool_manager->UnpinPage(next_page_id_, true);
  }

  SetNextPageId(recipient->BPlusTreePage::GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int curr_size = BPlusTreePage::GetSize();
  int j = 0;

  for (int i = curr_size; i < curr_size + size; i++) {
    array[i].first = items[j].first;
    array[i].second = items[j].second;
    j++;
  }

  BPlusTreePage::IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    KeyType k = array[i].first;

    int compare = comparator(k, key);

    if (compare == 0) {
      *value = array[i].second;
      return true;
    }
  }

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int size = BPlusTreePage::GetSize();
  ValueType value;
  if (!Lookup(key, &value, comparator)) {
    return size;
  }

  int index = KeyIndex(key, comparator);
  BPlusTreePage::IncreaseSize(-1);
  size--;

  for (int i = index; i < size; i++) {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }

  return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = BPlusTreePage::GetSize();
  recipient->CopyNFrom(array, size);
  BPlusTreePage::IncreaseSize(-size);

  recipient->SetNextPageId(next_page_id_);

  if (next_page_id_ != INVALID_PAGE_ID) {
    Page *next = buffer_pool_manager->FetchPage(next_page_id_);
    BPlusTreeLeafPage *next_page = reinterpret_cast<BPlusTreeLeafPage *>(next->GetData());
    next_page->SetPrevPageId(prev_page_id_);
    buffer_pool_manager->UnpinPage(next_page_id_, true);
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyLastFrom(array[0]);
  BPlusTreePage::IncreaseSize(-1);

  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int size = BPlusTreePage::GetSize();

  array[size].first = item.first;
  array[size].second = item.second;

  BPlusTreePage::IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array[BPlusTreePage::GetSize() - 1]);
  BPlusTreePage::IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for (int i = BPlusTreePage::GetSize(); i > 0; i--) {
    array[i].first = array[i - 1].first;
    array[i].second = array[i - 1].second;
  }

  array[0].first = item.first;
  array[0].second = item.second;

  BPlusTreePage::IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
