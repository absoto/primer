//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cmath>
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  BPlusTreePage::SetPageId(page_id);
  BPlusTreePage::SetParentPageId(parent_id);
  BPlusTreePage::SetSize(0);
  BPlusTreePage::SetMaxSize(max_size);
  BPlusTreePage::SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const { return array[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    ValueType val = array[i].second;
    if (val == value) {
      return i;
    }
  }

  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator,
                                                 bool from_insert) const {
  int size = BPlusTreePage::GetSize();
  if (size == 0) {
    return INVALID_PAGE_ID;
  }

  for (int i = 1; i < size; i++) {
    KeyType k = array[i].first;

    int compare = comparator(k, key);

    if (compare == 0) {
      return array[i].second;
    }

    if (compare == 1) {
      if (from_insert) {
        if (comparator(array[0].first, key) > 0) {
          return INVALID_PAGE_ID;
        }
      }

      return array[i - 1].second;
    }
  }

  return array[size - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value,
                                                     BufferPoolManager *buffer_pool_manager) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;

  page_id_t page_id = BPlusTreePage::GetPageId();

  Page *page_ = buffer_pool_manager->FetchPage(old_value);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page_->GetData());
  tree_page->SetParentPageId(page_id);
  buffer_pool_manager->UnpinPage(tree_page->GetPageId(), true);

  page_ = buffer_pool_manager->FetchPage(new_value);
  tree_page = reinterpret_cast<BPlusTreePage *>(page_->GetData());
  tree_page->SetParentPageId(page_id);
  buffer_pool_manager->UnpinPage(tree_page->GetPageId(), true);

  BPlusTreePage::SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value,
                                                    BufferPoolManager *buffer_pool_manager) {
  int size = BPlusTreePage::GetSize();

  Page *page_ = buffer_pool_manager->FetchPage(new_value);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page_->GetData());
  tree_page->SetParentPageId(BPlusTreePage::GetPageId());
  buffer_pool_manager->UnpinPage(new_value, true);

  if (old_value == INVALID_PAGE_ID) {
    KeyType key = array[0].first;
    ValueType value = array[0].second;
    array[0].first = new_key;
    array[0].second = new_value;
    return InsertNodeAfter(new_value, key, value, buffer_pool_manager);
  }

  int index = ValueIndex(old_value);

  for (int i = BPlusTreePage::GetSize(); i > index + 1; i--) {
    array[i] = array[i - 1];
  }

  array[index + 1].first = new_key;
  array[index + 1].second = new_value;

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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, int index,
                                                BufferPoolManager *buffer_pool_manager) {
  int curr_size = BPlusTreePage::GetSize();
  int split_index = ceil((curr_size + 1) / 2.0);

  if (index < BPlusTreePage::GetMinSize()) {
    split_index -= 1;
  }

  int copy_size = curr_size - split_index;

  recipient->CopyNFrom(array + split_index, copy_size, buffer_pool_manager);
  BPlusTreePage::IncreaseSize(-copy_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  page_id_t parent_page_id = BPlusTreePage::GetPageId();
  int curr_size = BPlusTreePage::GetSize();
  int j = 0;

  for (int i = curr_size; i < curr_size + size; i++) {
    page_id_t page_id = items[j].second;
    Page *page_ = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page_->GetData());
    tree_page->SetParentPageId(parent_page_id);
    buffer_pool_manager->UnpinPage(page_id, true);

    array[i].first = items[j].first;
    array[i].second = page_id;
    j++;
  }

  BPlusTreePage::IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  BPlusTreePage::IncreaseSize(-1);
  int new_size = BPlusTreePage::GetSize();

  for (int i = index; i < new_size; i++) {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  BPlusTreePage::IncreaseSize(-1);
  return array[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int size = BPlusTreePage::GetSize();
  // this is redundant in curr implementation
  array[0].first = middle_key;
  recipient->CopyNFrom(array, size, buffer_pool_manager);
  BPlusTreePage::IncreaseSize(-size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  array[0].first = middle_key;
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  BPlusTreePage::IncreaseSize(-1);

  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  page_id_t page_id = pair.second;
  Page *page_ = buffer_pool_manager->FetchPage(page_id);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page_->GetData());
  tree_page->SetParentPageId(BPlusTreePage::GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);

  int size = BPlusTreePage::GetSize();
  array[size].first = pair.first;
  array[size].second = pair.second;

  BPlusTreePage::IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array[BPlusTreePage::GetSize() - 1], middle_key, buffer_pool_manager);
  BPlusTreePage::IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, const KeyType &middle_key,
                                                   BufferPoolManager *buffer_pool_manager) {
  array[0].first = middle_key;

  page_id_t page_id = pair.second;
  Page *page_ = buffer_pool_manager->FetchPage(page_id);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page_->GetData());
  tree_page->SetParentPageId(BPlusTreePage::GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);

  for (int i = BPlusTreePage::GetSize(); i > 0; i--) {
    array[i].first = array[i - 1].first;
    array[i].second = array[i - 1].second;
  }

  array[0].first = pair.first;
  array[0].second = pair.second;

  BPlusTreePage::IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
